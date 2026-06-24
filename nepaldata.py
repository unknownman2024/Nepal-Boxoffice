from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_BASE_DIR = "Nepal Boxoffice"
DEFAULT_OUT_DIR = "Nepal Data"
DEFAULT_CONCURRENCY = 100


# -------------------------
# utilities
# -------------------------

def canon(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def write_if_changed(path: Path, obj: Any) -> bool:
    """Write JSON only if the final serialized text differs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = canon(obj) + "\n"
    if path.exists():
        old = path.read_text(encoding="utf-8")
        if sha256_text(old) == sha256_text(text):
            return False
    path.write_text(text, encoding="utf-8")
    return True


def to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def round2(v: float) -> float:
    return round(v + 1e-12, 2)


def occupancy(sold: int, reserved: int, seats: int) -> float:
    if seats <= 0:
        return 0.0
    return round2(((sold + reserved) / seats) * 100.0)


def iso(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def compact_day(d: str) -> str:
    return d.replace("-", "")[:8]


def slugify(name: str) -> str:
    s = name.strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[\u2010-\u2015/\\|]+", "-", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "movie"


def normalize_dt(raw: Any, fallback_date: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return fallback_date
    # Keep only date portion if timestamp is provided
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return fallback_date


def daterange(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


# -------------------------
# local file scan + async read
# -------------------------

def discover_source_files(base_dir: Path, start_date: Optional[date], end_date: Optional[date]) -> List[Path]:
    """
    Scan local files under:
      base_dir/YYYY/YYYY-MM-DD_Detailed.json

    If start/end are provided, filter by the date in filename.
    """
    files = sorted(base_dir.glob("*/*_Detailed.json"))
    if not files:
        return []

    if start_date is None and end_date is None:
        return files

    out: List[Path] = []
    for p in files:
        # filename: 2025-12-08_Detailed.json
        name = p.name
        if not name.endswith("_Detailed.json"):
            continue
        ds = name[:10]
        try:
            d = datetime.strptime(ds, "%Y-%m-%d").date()
        except Exception:
            continue
        if start_date is not None and d < start_date:
            continue
        if end_date is not None and d > end_date:
            continue
        out.append(p)
    return out


def load_json_sync(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[read error] {path} -> {e}")
        return None


async def load_many(paths: List[Path], concurrency: int) -> List[Optional[Dict[str, Any]]]:
    sem = asyncio.Semaphore(concurrency)

    async def worker(path: Path) -> Optional[Dict[str, Any]]:
        async with sem:
            return await asyncio.to_thread(load_json_sync, path)

    return await asyncio.gather(*(worker(p) for p in paths))


# -------------------------
# aggregation models
# -------------------------

@dataclass
class MovieState:
    movie_id: str
    movie_name: str
    first_date: str
    last_date: str
    total_gross: int = 0
    total_sold: int = 0
    total_reserved: int = 0
    total_shows: int = 0
    total_seats: int = 0
    venues: set = field(default_factory=set)
    raw_show_count: int = 0
    daily: Dict[str, Dict[str, Any]] = field(default_factory=dict)


def normalize_show(raw: Dict[str, Any], fallback_date: str) -> Dict[str, Any]:
    movie_id = str(raw.get("movie_id", "")).strip()
    movie_name = str(raw.get("movie_name", "")).strip()
    show_id = str(raw.get("show_id", "")).strip()
    venue = str(raw.get("venue", "")).strip()
    theatre = str(raw.get("theatre", "")).strip()
    dt = normalize_dt(raw.get("date"), fallback_date)
    seats = to_int(raw.get("seats"))
    sold = to_int(raw.get("sold"))
    reserved = to_int(raw.get("reserved"))
    available = to_int(raw.get("available"))
    gross = to_int(raw.get("gross"))

    return {
        "movie_id": movie_id,
        "movie_name": movie_name,
        "show_id": show_id,
        "venue": venue,
        "theatre": theatre,
        "date": dt,
        "seats": seats,
        "sold": sold,
        "reserved": reserved,
        "available": available,
        "gross": gross,
        "occupancy_percent": occupancy(sold, reserved, seats),
    }


# compact day row order:
# [date, gross, occupancy_percent, reserved, seats, shows, sold, venues]

def aggregate(payloads: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    seen: set = set()  # (date, movie_id, show_id)
    movies: Dict[str, MovieState] = {}
    all_source_dates: List[str] = []

    for payload in payloads:
        if not payload:
            continue

        file_date = str(payload.get("date") or "").strip()
        if file_date:
            all_source_dates.append(file_date)

        shows = payload.get("shows") or []
        if not isinstance(shows, list):
            continue

        for raw in shows:
            if not isinstance(raw, dict):
                continue

            s = normalize_show(raw, file_date)
            mid = s["movie_id"]
            mname = s["movie_name"]
            sid = s["show_id"]
            d = s["date"]

            if not mid or not mname or not sid or not d:
                continue

            key = (d, mid, sid)
            if key in seen:
                continue
            seen.add(key)

            if mid not in movies:
                movies[mid] = MovieState(
                    movie_id=mid,
                    movie_name=mname,
                    first_date=d,
                    last_date=d,
                )

            ms = movies[mid]
            ms.movie_name = mname or ms.movie_name
            ms.raw_show_count += 1
            ms.first_date = min(ms.first_date, d)
            ms.last_date = max(ms.last_date, d)
            if s["venue"]:
                ms.venues.add(s["venue"])

            day = ms.daily.get(d)
            if day is None:
                day = {
                    "gross": 0,
                    "sold": 0,
                    "reserved": 0,
                    "seats": 0,
                    "shows": 0,
                    "venues": set(),
                }
                ms.daily[d] = day

            day["gross"] += s["gross"]
            day["sold"] += s["sold"]
            day["reserved"] += s["reserved"]
            day["seats"] += s["seats"]
            day["shows"] += 1
            if s["venue"]:
                day["venues"].add(s["venue"])

            ms.total_gross += s["gross"]
            ms.total_sold += s["sold"]
            ms.total_reserved += s["reserved"]
            ms.total_seats += s["seats"]
            ms.total_shows += 1

    movie_docs: Dict[str, Any] = {}
    index_rows: List[Dict[str, Any]] = []

    for mid, ms in movies.items():
        db = []
        for d in sorted(ms.daily.keys()):
            day = ms.daily[d]
            db.append([
                compact_day(d),
                day["gross"],
                occupancy(day["sold"], day["reserved"], day["seats"]),
                day["reserved"],
                day["seats"],
                day["shows"],
                day["sold"],
                len(day["venues"]),
            ])

        overall_occ = occupancy(ms.total_sold, ms.total_reserved, ms.total_seats)
        movie_docs[mid] = {
            "mn": ms.movie_name,  # movie name
            "mi": mid,           # movie id
            "t": {               # totals
                "g": ms.total_gross,
                "l": ms.total_sold,
                "r": ms.total_reserved,
                "h": ms.total_shows,
                "v": len(ms.venues),
                "s": ms.total_seats,
                "o": overall_occ,
            },
            "r": {               # run window
                "f": ms.first_date,
                "l": ms.last_date,
                "d": len(db),
            },
            "db": db,            # compact daywise breakdown
        }

        index_rows.append({
            "mi": mid,
            "mn": ms.movie_name,
            "g": ms.total_gross,
            "l": ms.total_sold,
            "r": ms.total_reserved,
            "h": ms.total_shows,
            "v": len(ms.venues),
            "s": ms.total_seats,
            "o": overall_occ,
            "d": len(db),
            "f": ms.first_date,
            "e": ms.last_date,
        })

    index_rows.sort(key=lambda x: (-x["g"], x["mn"], x["mi"]))
    index_doc = {
        "g": datetime.now().astimezone().isoformat(timespec="seconds"),
        "n": len(movie_docs),
        "m": index_rows,
        "r": {
            "g": [x["mi"] for x in sorted(index_rows, key=lambda x: (-x["g"], x["mn"], x["mi"]))],
            "l": [x["mi"] for x in sorted(index_rows, key=lambda x: (-x["l"], x["mn"], x["mi"]))],
            "h": [x["mi"] for x in sorted(index_rows, key=lambda x: (-x["h"], x["mn"], x["mi"]))],
            "o": [x["mi"] for x in sorted(index_rows, key=lambda x: (-x["o"], x["mn"], x["mi"]))],
            "v": [x["mi"] for x in sorted(index_rows, key=lambda x: (-x["v"], x["mn"], x["mi"]))],
        },
        "sd": len(set(all_source_dates)),
    }

    return movie_docs, index_doc


# -------------------------
# slug registry
# -------------------------

def build_slug_registry(movie_docs: Dict[str, Any]) -> Dict[str, str]:
    """
    Return {movie_id: filename_slug}.
    Slug collisions are resolved by suffixing -2, -3, ...
    """
    taken: Dict[str, str] = {}
    out: Dict[str, str] = {}

    items = sorted(movie_docs.items(), key=lambda kv: (-kv[1]["t"]["g"], kv[1]["mn"], kv[0]))
    for mid, doc in items:
        base = slugify(doc["mn"])
        slug = base
        if slug in taken and taken[slug] != mid:
            i = 2
            while f"{base}-{i}" in taken:
                i += 1
            slug = f"{base}-{i}"
        taken[slug] = mid
        out[mid] = slug

    return out


# -------------------------
# pipeline
# -------------------------

async def build_dataset(
    base_dir: Path,
    start_date: Optional[date],
    end_date: Optional[date],
    out_dir: Path,
    concurrency: int,
    keep_missing: bool,
) -> None:
    files = discover_source_files(base_dir, start_date, end_date)
    if not files:
        print(f"[warn] no source files found under: {base_dir}")
        return

    print(f"[info] source files found: {len(files)}")
    payloads = await load_many(files, concurrency=concurrency)

    loaded: List[Dict[str, Any]] = []
    missing = 0
    for p, data in zip(files, payloads):
        if data is None:
            missing += 1
            if keep_missing:
                date_str = p.name[:10]
                loaded.append({"date": date_str, "shows": []})
            continue
        loaded.append(data)

    print(f"[info] loaded: {len(loaded)} files, failed: {missing}")

    movie_docs, index_doc = aggregate(loaded)
    slug_map = build_slug_registry(movie_docs)

    # movies.json: compact full history collection
    movies_doc = {
        "g": datetime.now().astimezone().isoformat(timespec="seconds"),
        "n": len(movie_docs),
        "m": [],
    }

    for mid, doc in sorted(movie_docs.items(), key=lambda kv: (-kv[1]["t"]["g"], kv[1]["mn"], kv[0])):
        row = dict(doc)
        row["sl"] = slug_map[mid]
        movies_doc["m"].append(row)

    movies_written = write_if_changed(out_dir / "movies.json", movies_doc)
    index_written = write_if_changed(out_dir / "index.json", index_doc)

    movie_dir = out_dir / "movie"
    movie_dir.mkdir(parents=True, exist_ok=True)

    updated_movie_files = 0
    for mid, doc in movie_docs.items():
        slug = slug_map[mid]
        movie_path = movie_dir / f"{slug}.json"
        file_doc = dict(doc)
        file_doc["sl"] = slug
        if write_if_changed(movie_path, file_doc):
            updated_movie_files += 1

    print(
        f"[done] movies.json={'updated' if movies_written else 'unchanged'}, "
        f"index.json={'updated' if index_written else 'unchanged'}, "
        f"movie files updated={updated_movie_files}/{len(movie_docs)}"
    )


# -------------------------
# CLI
# -------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compact Nepal Boxoffice aggregator (local files)")
    p.add_argument("--base-dir", default=DEFAULT_BASE_DIR, help='Root folder containing year folders, e.g. "Nepal Boxoffice"')
    p.add_argument("--out", default=DEFAULT_OUT_DIR, help="Output directory")
    p.add_argument("--start", default=None, help="Optional start date YYYY-MM-DD")
    p.add_argument("--end", default=None, help="Optional end date YYYY-MM-DD")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrent local file reads")
    p.add_argument("--keep-missing", action="store_true", help="Keep missing dates as empty entries")
    return p.parse_args()


def parse_date_or_none(v: Optional[str]) -> Optional[date]:
    if not v:
        return None
    return datetime.strptime(v, "%Y-%m-%d").date()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir)
    out_dir = Path(args.out)
    start_date = parse_date_or_none(args.start)
    end_date = parse_date_or_none(args.end)

    if start_date and end_date and end_date < start_date:
        raise SystemExit("--end must be >= --start")

    asyncio.run(
        build_dataset(
            base_dir=base_dir,
            start_date=start_date,
            end_date=end_date,
            out_dir=out_dir,
            concurrency=args.concurrency,
            keep_missing=args.keep_missing,
        )
    )


if __name__ == "__main__":
    main()
