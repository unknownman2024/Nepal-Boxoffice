import json
import os
import random
import time
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

# Try to import curl_cffi, fallback to cloudscraper
try:
    from curl_cffi import requests as curl_req
    USE_CURL_CFFI = True
    print("✅ Using curl_cffi (TLS impersonation)")
except ImportError:
    import cloudscraper
    USE_CURL_CFFI = False
    print("⚠️ curl_cffi not installed, falling back to cloudscraper")

#########################################
# CONFIG
#########################################
MAX_THREADS = 5
RETRY_PER_REQUEST = 6
SCRAPE_PASSES = 5
TIMEOUT_SEC = 30
CUT_OFF_MINUTES = 200
REGION_CODE = "SNLK"

IST = ZoneInfo("Asia/Kolkata")
YEAR = datetime.now(IST).strftime("%Y")
OUT_DIR = os.path.join("Sri Lanka Boxoffice", YEAR)
os.makedirs(OUT_DIR, exist_ok=True)

def atomic_dump(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)

#########################################
# RANDOM HEADERS (exact match of working request)
#########################################
def random_user_agent():
    ios = f"Mozilla/5.0 (iPhone; CPU iPhone OS {random.randint(15,18)}_{random.randint(0,7)} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{random.randint(16,18)}.0 Mobile/15E148 Safari/604.1"
    android = f"Mozilla/5.0 (Linux; Android {random.choice(['10','11','12','13','14','15'])}; Pixel {random.randint(3,9)}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(110,129)}.0.{random.randint(1000,7000)}.{random.randint(50,250)} Mobile Safari/537.36"
    windows = f"Mozilla/5.0 (Windows NT {random.choice(['10.0','11.0'])}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(110,129)}.0.{random.randint(1000,7000)}.{random.randint(50,250)} Safari/537.36"
    mac = f"Mozilla/5.0 (Macintosh; Intel Mac OS X {random.choice(['10_15_7','11_6','12_6','13_4','14_0','15_0'])}) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{random.randint(14,18)}.0 Safari/605.1.15"
    return random.choice([ios, android, windows, mac])

def build_headers(extra=None):
    ua = random_user_agent()
    is_mobile = "Mobile" in ua or "iPhone" in ua or "Android" in ua
    platform = "iOS" if "iPhone" in ua else "Android" if "Android" in ua else "macOS" if "Mac" in ua else "Windows"
    chrome_ver = random.randint(110, 129)

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": random.choice(["en-GB,en;q=0.9", "en-US,en;q=0.8", "en-IN,en;q=0.9"]),
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "User-Agent": ua,
        "Referer": random.choice([
            "https://lk.bookmyshow.com/",
            "https://www.google.com/",
            "https://m.bookmyshow.com/"
        ]),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Origin": "https://lk.bookmyshow.com",
        "Sec-CH-UA": f'"Google Chrome";v="{chrome_ver}", "Chromium";v="{chrome_ver}", "Not)A;Brand";v="{random.randint(24,99)}"',
        "Sec-CH-UA-Mobile": "?1" if is_mobile else "?0",
        "Sec-CH-UA-Platform": f'"{platform}"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Priority": "u=1, i",
        "Connection": "keep-alive",
    }
    if extra:
        headers.update(extra)
    return {k: v for k, v in headers.items() if v is not None}

#########################################
# SESSION CREATION (with cookie from homepage)
#########################################
def create_session_with_cookie():
    """Get a session that has a valid cookie by visiting the homepage."""
    if USE_CURL_CFFI:
        session = curl_req.Session(impersonate="chrome124", timeout=TIMEOUT_SEC)
    else:
        session = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
    # First visit homepage to get session cookie
    home_url = "https://lk.bookmyshow.com/"
    headers_home = {
        "User-Agent": random_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(["en-GB,en;q=0.9", "en-US,en;q=0.8"]),
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        resp = session.get(home_url, headers=headers_home, timeout=TIMEOUT_SEC)
        print(f"🏠 Homepage status: {resp.status_code}")
        if resp.status_code != 200:
            print("⚠️ Homepage did not return 200, cookies may be missing")
        else:
            print("✅ Homepage fetched, cookies set")
    except Exception as e:
        print(f"⚠️ Homepage visit failed: {e}")
    return session

#########################################
# SAFE REQUEST (with retry and logging)
#########################################
def safe_request(url, method="GET", payload=None, session=None, retries=RETRY_PER_REQUEST):
    if session is None:
        session = create_session_with_cookie()
    last_err = "UNKNOWN"
    for attempt in range(retries):
        try:
            headers = build_headers()
            if method == "POST":
                resp = session.post(url, json=payload, headers=headers)
            else:
                resp = session.get(url, headers=headers)

            # Log response status and snippet
            print(f"  ➜ {method} {url} → {resp.status_code}")
            if resp.status_code != 200:
                snippet = resp.text[:200] if resp.text else "empty"
                print(f"  ⚠️ Response snippet: {snippet}")

            if resp.status_code == 200:
                # Check if it's HTML (Cloudflare challenge)
                if resp.text.strip().startswith("<!DOCTYPE"):
                    print("  ❌ Received HTML instead of JSON – likely blocked")
                    return None, "HTML_RESPONSE"
                # Try to parse JSON
                try:
                    return resp.json(), None
                except json.JSONDecodeError:
                    print(f"  ❌ Invalid JSON: {resp.text[:200]}")
                    return None, "INVALID_JSON"
            elif resp.status_code == 403:
                # Refresh session and retry
                print("  🔄 403 detected, refreshing session...")
                session = create_session_with_cookie()
                last_err = "HTTP_403"
            else:
                last_err = f"HTTP_{resp.status_code}"
            time.sleep(random.uniform(1.0, 3.0))
        except Exception as e:
            print(f"  ❌ Request exception: {e}")
            last_err = str(e)
            time.sleep(random.uniform(1.0, 3.0))
    return None, last_err

#########################################
# API CALLS
#########################################
def get_movies(session=None):
    url = "https://lk.bookmyshow.com/pwa/api/uapi/movies/"
    body = {
        "regionCode": REGION_CODE,
        "subCode": "",
        "filters": {},
        "genres": [],
        "languages": [],
        "formats": [],
        "page": 1,
        "limit": 200
    }
    return safe_request(url, "POST", payload=body, session=session)

def get_showtimes(event_code, date, session=None):
    url = f"https://lk.bookmyshow.com/pwa/api/de/showtimes/byevent?regionCode={REGION_CODE}&subCode=&eventCode={event_code}&dateCode={date}"
    return safe_request(url, "GET", session=session)

#########################################
# PARSERS (unchanged)
#########################################
def extract_movies(raw):
    if not isinstance(raw, dict):
        return []
    if "nowShowing" in raw and "arrEvents" in raw["nowShowing"]:
        return raw["nowShowing"]["arrEvents"]
    if "arrEvents" in raw:
        return raw["arrEvents"]
    if "movies" in raw:
        return raw["movies"]
    return []

def extract_venues(raw, date):
    details = raw.get("BookMyShow", {}).get("ShowDetails", [])
    for d in details:
        if str(d.get("Date")) == str(date):
            return d.get("Venues", [])
    return []

def flatten(movie_obj, venue, sh, date):
    session_id = sh.get("SessionId") or sh.get("Id") or ""
    total = sum(int(c.get("MaxSeats", 0)) for c in sh.get("Categories", []))
    avail = sum(int(c.get("SeatsAvail", 0)) for c in sh.get("Categories", []))
    price = float(sh.get("MinPrice", 0))

    sold = total - avail
    gross = sold * price
    occupancy = round((sold / total * 100), 2) if total else 0

    bad = False
    if sold < 0 or gross < 0 or avail > total or total == 0:
        sold, gross, occupancy = 0, 0, 0
        bad = True

    return {
        "movie": movie_obj["title"],
        "format": movie_obj["format"],
        "language": movie_obj["language"],
        "eventCode": movie_obj["eventCode"],
        "venue": venue.get("VenueName"),
        "sessionId": str(session_id),
        "time": sh.get("ShowTime"),
        "totalSeats": total,
        "available": avail,
        "sold": sold,
        "gross": gross,
        "occupancy": occupancy,
        "date": date,
        "badData": bad
    }

def scrape_event(movie, date, attempt, session_pool):
    session = session_pool.get()
    title = f"{movie['title']} ({movie['format'] or 'Standard'})"
    code = movie["eventCode"]
    res, err = get_showtimes(code, date, session=session)
    session_pool.put(session)
    if not res:
        return title, [], False
    venues = extract_venues(res, date)
    if not venues:
        return title, [], False
    rows = []
    for v in venues:
        for sh in v.get("ShowTimes", []):
            rows.append(flatten(movie, v, sh, date))
    return title, rows, True

#########################################
# MAIN
#########################################
print("\n🚀 Sri Lanka Boxoffice Tracker Started...\n")

target_date = datetime.now(IST).strftime("%Y%m%d")
summary_file = f"{OUT_DIR}/{target_date}_Summary.json"
detail_file = f"{OUT_DIR}/{target_date}_Detailed.json"

# Load existing DB
existing_rows = []
if os.path.exists(detail_file):
    try:
        existing_rows = json.load(open(detail_file)).get("shows", [])
        print(f"📂 Loaded {len(existing_rows)} existing shows")
    except:
        print("⚠️ Old DB corrupted, starting fresh...")

# Create session pool
session_pool = Queue()
for _ in range(MAX_THREADS + 2):
    session_pool.put(create_session_with_cookie())

# Fetch movies using a session
movies_session = session_pool.get()
movies_raw, err = get_movies(session=movies_session)
session_pool.put(movies_session)

if not movies_raw:
    print(f"❌ Failed to fetch movies. Error: {err}")
    sys.exit(1)

parent_movies = extract_movies(movies_raw)
print(f"📽️ Found {len(parent_movies)} parent movies")

expanded_movies = []
for movie in parent_movies:
    for c in movie["ChildEvents"]:
        expanded_movies.append({
            "title": movie["EventTitle"],
            "eventCode": c["EventCode"],
            "format": c.get("EventDimension", ""),
            "language": c.get("EventLanguage", ""),
            "release": c.get("EventDate", "9999-99-99")
        })
print(f"🎬 Expanded to {len(expanded_movies)} event variants")

if not expanded_movies:
    print("⚠️ No event variants found – check API response")
    sys.exit(0)

# Multi-pass scraping
all_rows = []
pending = expanded_movies.copy()

for attempt in range(1, SCRAPE_PASSES + 1):
    if not pending:
        break
    print(f"\n🔄 Scrape pass {attempt}/{SCRAPE_PASSES} – {len(pending)} events pending")
    next_round = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        futures = {pool.submit(scrape_event, m, target_date, attempt, session_pool): m for m in pending}
        for job in as_completed(futures):
            _, rows, ok = job.result()
            if ok:
                all_rows.extend(rows)
            else:
                next_round.append(futures[job])
    pending = next_round

# Apply cutoff
def parse_time(date_str, t):
    for fmt in ["%I:%M %p", "%H:%M"]:
        try:
            return datetime.strptime(f"{date_str} {t}", f"%Y%m%d {fmt}").replace(tzinfo=IST)
        except:
            pass
    return None

def is_within_cutoff(show):
    st = parse_time(target_date, show["time"])
    if not st:
        return True
    mins_left = int((st - datetime.now(IST)).total_seconds() / 60)
    show["minsLeft"] = mins_left
    return mins_left < CUT_OFF_MINUTES

eligible_new = [s for s in all_rows if is_within_cutoff(s)]
print(f"✅ New shows scraped: {len(eligible_new)}")

# Merge (never delete)
data_map = {
    (s["eventCode"], s["venue"], s["sessionId"]): s
    for s in existing_rows
}
for s in eligible_new:
    key = (s["eventCode"], s["venue"], s["sessionId"])
    data_map[key] = s

all_rows = list(data_map.values())
print(f"📊 Total shows stored: {len(all_rows)}")

# Build summary (same as before) – omitted for brevity, keep your original summary builder
# (Assume you have it below, I'll just copy the relevant part)

# ... (rest of summary building code from your script) ...

# Save
timestamp = datetime.now(IST).strftime("%I:%M %p, %d %B %Y")
atomic_dump(detail_file, {
    "date": target_date,
    "lastUpdated": timestamp,
    "shows": all_rows,
    "autoCorrected": bad_fix_count
})
atomic_dump(summary_file, {
    "date": target_date,
    "lastUpdated": timestamp,
    "movies": summary
})

print("\n================================================")
print(f"🎬 Event Variants Fetched: {len(expanded_movies)}")
print(f"🎟 Lifetime Shows Stored: {len(all_rows)}")
print(f"✅ Newly Added This Run: {len(eligible_new)}")
print(f"⚠ Invalid API auto-corrected: {bad_fix_count}")
print(f"📁 Summary → {summary_file}")
print(f"📁 Detailed → {detail_file}")
print("================================================")
print("🎉 DONE — CUT-OFF ADD ONLY | PERMANENT DB ACTIVE\n")
