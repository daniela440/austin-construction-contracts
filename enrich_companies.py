"""
Company Website Enrichment
1) Matches companies in Sheet A against Companies_Enrichment (Sheet B) by name.
2) For remaining companies without a website, searches Brave Search API.
Fills in Website (column G) in Sheet A where empty.

Append-only — only updates empty website cells, never deletes data.
"""

import json
import os
import re
import ssl
import time
from urllib.parse import urlparse, urlencode
from urllib.request import urlopen, Request

from google.oauth2 import service_account
from googleapiclient.discovery import build

try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE

# --- Config ---
SERVICE_ACCOUNT_FILE = "service-account-key.json"

# Sheet A: Dashboard data
SCRAPER_SHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SCRAPER_SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

# Sheet B: USA Spending enrichment data
USASPENDING_SHEET_ID = "1wn90bdenhTMhPgyxeJMQXMy6G5D_Ox3eJvh3aW74RaA"
ENRICHMENT_TAB = "Companies_Enrichment"

# Companies_Enrichment column indices (0-based)
COL_COMPANY = 0   # A: Recipient (Company)
COL_ROW_TYPE = 9  # J: row_type
COL_WEBSITE = 14  # O: Back up Source: DDG

# Sheet A column indices
SHEET_A_COMPANY = 1  # B: Company Name
SHEET_A_WEBSITE = 6  # G: Website

# Brave Search
def _load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())

_load_env()
BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY", "")
BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
SEARCH_DELAY = 1.5  # seconds between searches

# Suffixes to strip for fuzzy matching
STRIP_SUFFIXES = [
    "INCORPORATED", "INC", "CORPORATION", "CORP", "COMPANY", "CO",
    "LIMITED", "LTD", "LLC", "LP", "LLP", "PLLC", "PC", "PA",
    "GROUP", "HOLDINGS", "ENTERPRISES", "SERVICES",
    "JOINT VENTURE", "JV", "DBA",
]

# Blacklist_Rules tab in Sheet B
BLACKLIST_TAB = "Blacklist_Rules"

# Fallback hardcoded domains (used if Blacklist_Rules tab can't be read)
JUNK_DOMAINS = {
    "google.com", "support.google.com", "docs.google.com", "maps.google.com",
    "play.google.com", "bing.com", "yahoo.com", "duckduckgo.com",
    "facebook.com", "meta.com", "twitter.com", "x.com",
    "linkedin.com", "instagram.com", "youtube.com", "reddit.com",
    "wikipedia.org", "amazon.com", "netflix.com", "chatgpt.com",
    "yelp.com", "bbb.org", "glassdoor.com", "indeed.com",
    "api.sam.gov", "sam.gov", "usaspending.gov",
    "zhihu.com", "zhidao.baidu.com", "baidu.com", "bbs.csdn.net",
    "jingyan.baidu.com", "tieba.baidu.com",
}

JUNK_TLDS = {
    ".de", ".fr", ".pl", ".hu", ".gr", ".dk", ".ru", ".cn", ".jp",
    ".kr", ".br", ".it", ".es", ".pt", ".nl", ".se", ".no", ".fi",
    ".cz", ".sk", ".ro", ".bg", ".hr", ".rs", ".ua", ".by",
}

# Populated at runtime from Blacklist_Rules tab
BLACKLIST_EXACT = set()
BLACKLIST_CONTAINS = set()


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def load_blacklist(service):
    """Load blacklist rules from Blacklist_Rules tab in Sheet B."""
    global BLACKLIST_EXACT, BLACKLIST_CONTAINS
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=USASPENDING_SHEET_ID,
            range=f"'{BLACKLIST_TAB}'!A:E",
        ).execute()
        rows = result.get("values", [])
        if len(rows) < 2:
            print("  Blacklist_Rules tab empty, using hardcoded list.")
            return

        for row in rows[1:]:
            rule_type = (row[0] if len(row) > 0 else "").strip().upper()
            match_val = (row[1] if len(row) > 1 else "").strip().lower()
            enabled = (row[4] if len(row) > 4 else "TRUE").strip().upper()

            if not match_val or enabled != "TRUE":
                continue

            if rule_type == "EXACT_DOMAIN":
                BLACKLIST_EXACT.add(match_val)
            elif rule_type == "DOMAIN_CONTAINS":
                BLACKLIST_CONTAINS.add(match_val)

        print(f"  Loaded blacklist: {len(BLACKLIST_EXACT)} exact, {len(BLACKLIST_CONTAINS)} contains rules")
    except Exception as e:
        print(f"  Could not load Blacklist_Rules: {e}")
        print("  Using hardcoded junk domains only.")


def normalize(name):
    """Normalize company name: uppercase, strip suffixes, remove punctuation."""
    if not name:
        return ""
    n = name.upper().strip()
    n = re.sub(r"[.,;:'\"\-/\\()&]", " ", n)
    for _ in range(3):
        for suffix in STRIP_SUFFIXES:
            n = re.sub(r"\b" + re.escape(suffix) + r"\b", "", n)
    return re.sub(r"\s+", " ", n).strip()


def is_url(val):
    """Check if a value looks like a URL (not 'no url' or empty)."""
    if not val:
        return False
    v = val.strip().lower()
    if v in ("no url", "n/a", "none", ""):
        return False
    return "." in v


def extract_domain(url):
    """Extract clean domain from URL."""
    try:
        domain = urlparse(url if "://" in url else f"https://{url}").netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def is_junk_domain(url):
    """Check if a URL is a junk/irrelevant domain using hardcoded + Blacklist_Rules tab."""
    domain = extract_domain(url)
    if not domain:
        return True
    # Hardcoded checks
    if domain in JUNK_DOMAINS:
        return True
    for tld in JUNK_TLDS:
        if domain.endswith(tld):
            return True
    # Sheet-based blacklist rules
    if domain in BLACKLIST_EXACT:
        return True
    for pattern in BLACKLIST_CONTAINS:
        if pattern in domain:
            return True
    return False


def brave_search(company_name):
    """Search Brave for a company website. Returns URL or empty string."""
    if not BRAVE_API_KEY:
        return ""

    query = f"{company_name} construction company official website"
    params = urlencode({"q": query, "count": 10, "country": "us", "search_lang": "en"})
    url = f"{BRAVE_SEARCH_URL}?{params}"

    req = Request(url, headers={
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    })

    try:
        import gzip
        resp = urlopen(req, timeout=15, context=SSL_CTX)
        data = resp.read()
        if resp.headers.get("Content-Encoding") == "gzip":
            data = gzip.decompress(data)
        results = json.loads(data)

        for r in results.get("web", {}).get("results", []):
            result_url = r.get("url", "")
            if not result_url or is_junk_domain(result_url):
                continue
            parsed = urlparse(result_url)
            return f"{parsed.scheme}://{parsed.netloc}"

    except Exception as e:
        print(f"    Brave error: {e}")

    return ""


def read_enrichment_websites(service):
    """Read company websites from Companies_Enrichment tab.
    Only rows where row_type = 'company'. Returns dict of normalized_name -> url.
    """
    result = service.spreadsheets().values().get(
        spreadsheetId=USASPENDING_SHEET_ID,
        range=f"'{ENRICHMENT_TAB}'!A:O",
    ).execute()
    rows = result.get("values", [])
    if len(rows) < 2:
        return {}

    lookup = {}
    company_rows = 0
    with_url = 0

    for row in rows[1:]:
        row_type = (row[COL_ROW_TYPE] if len(row) > COL_ROW_TYPE else "").strip().lower()
        if row_type != "company":
            continue
        company_rows += 1

        name = (row[COL_COMPANY] if len(row) > COL_COMPANY else "").strip()
        url = (row[COL_WEBSITE] if len(row) > COL_WEBSITE else "").strip()

        if not name or not is_url(url):
            continue

        with_url += 1
        norm = normalize(name)
        if norm:
            lookup[norm] = url

    print(f"  Company rows: {company_rows}")
    print(f"  With URLs: {with_url}")
    print(f"  Unique normalized names: {len(lookup)}")
    return lookup


def read_scraper_data(service):
    """Read all rows from Sheet A."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SCRAPER_SHEET_ID,
        range=f"'{SCRAPER_SHEET_NAME}'!A:N",
    ).execute()
    return result.get("values", [])


def write_updates(service, updates):
    """Write website updates to column G in batches."""
    batch_data = []
    for row_num, url in updates.items():
        batch_data.append({
            "range": f"'{SCRAPER_SHEET_NAME}'!G{row_num}",
            "values": [[url]],
        })

    for start in range(0, len(batch_data), 100):
        chunk = batch_data[start:start + 100]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SCRAPER_SHEET_ID,
            body={"valueInputOption": "RAW", "data": chunk},
        ).execute()
        print(f"  Batch {start // 100 + 1}: {len(chunk)} cells")


def enrich():
    service = get_sheets_service()

    # Step 0: Load blacklist rules from Sheet B
    print("Loading blacklist rules from Blacklist_Rules tab...")
    load_blacklist(service)

    # Step 1: Build website lookup from Sheet B
    print("\nReading Companies_Enrichment (companies with websites)...")
    lookup = read_enrichment_websites(service)

    # Step 2: Read Sheet A
    print("\nReading dashboard data from Sheet A...")
    rows = read_scraper_data(service)
    if len(rows) < 2:
        print("No data in Sheet A.")
        return
    print(f"  Total rows: {len(rows) - 1}")

    # Step 3: Match against enrichment data first
    updates = {}
    already_has = 0
    matched = 0
    need_search = []  # (row_number, company_name) for Brave search

    for i, row in enumerate(rows[1:], start=2):
        company = (row[SHEET_A_COMPANY] if len(row) > SHEET_A_COMPANY else "").strip()
        website = (row[SHEET_A_WEBSITE] if len(row) > SHEET_A_WEBSITE else "").strip()

        if not company:
            continue

        if website and website.lower() not in ("", "n/a", "none"):
            already_has += 1
            continue

        norm = normalize(company)
        if lookup and norm in lookup:
            updates[i] = lookup[norm]
            matched += 1
        else:
            need_search.append((i, company))

    print(f"\n--- Enrichment Match ---")
    print(f"  Already had website: {already_has}")
    print(f"  Matched from Sheet B: {matched}")
    print(f"  Need Brave search: {len(need_search)}")

    # Write enrichment matches immediately
    if updates:
        print(f"\nWriting {len(updates)} enrichment matches...")
        write_updates(service, updates)

    # Step 4: Brave Search for remaining companies
    if not need_search:
        print("\nNo companies need Brave search.")
        return

    if not BRAVE_API_KEY:
        print("\nBRAVE_API_KEY not set — skipping search.")
        print(f"  {len(need_search)} companies still without websites.")
        return

    print(f"\nSearching Brave for {len(need_search)} companies (no daily limit)...")
    brave_updates = {}
    found = 0
    not_found = 0

    for idx, (row_num, company) in enumerate(need_search, 1):
        if idx % 25 == 0 or idx == 1:
            print(f"  [{idx}/{len(need_search)}]...")

        url = brave_search(company)
        if url:
            brave_updates[row_num] = url
            found += 1
            print(f"    {company} -> {url}")
        else:
            not_found += 1

        time.sleep(SEARCH_DELAY)

        # Write in batches of 50 to avoid losing progress
        if len(brave_updates) >= 50:
            print(f"\n  Writing batch of {len(brave_updates)} Brave results...")
            write_updates(service, brave_updates)
            brave_updates = {}

    # Write remaining
    if brave_updates:
        print(f"\n  Writing final {len(brave_updates)} Brave results...")
        write_updates(service, brave_updates)

    print(f"\n--- Brave Search Results ---")
    print(f"  Found: {found}")
    print(f"  Not found: {not_found}")
    print(f"Done! Total websites added: {matched + found}")


if __name__ == "__main__":
    enrich()
