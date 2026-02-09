"""
Company Enrichment Script
Connects the scraper Google Sheet with the USA Spending enrichment Google Sheet.
Matches companies by normalized name, copies known websites, and runs
Brave Search for unmatched companies (capped at 50 searches/day).
Respects domain blacklist from USA Spending sheet.
"""

import json
import os
import re
import time
from datetime import date
from urllib.parse import urlparse
from urllib.request import urlopen, Request

import ssl

from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- SSL setup (macOS Python often lacks default certs) ---
try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE

# --- Config ---
SERVICE_ACCOUNT_FILE = "service-account-key.json"

# Sheet A: Scraper dashboard data
SCRAPER_SHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SCRAPER_SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

# Sheet B: USA Spending enrichment data
USASPENDING_SHEET_ID = "1wn90bdenhTMhPgyxeJMQXMy6G5D_Ox3eJvh3aW74RaA"
ENRICHMENT_TAB = "Companies_Enrichment"
BLACKLIST_TAB = "Blacklist_Rules"

# Brave Search API - loads from .env file or environment variable
def _load_env():
    """Load variables from .env file if it exists."""
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
SEARCH_DAILY_LIMIT = 50
SEARCH_DELAY = 1.0  # seconds between searches
SEARCH_COUNTER_FILE = "search_counter.json"

# Company name suffixes to strip during normalization
STRIP_SUFFIXES = [
    "INCORPORATED", "INC", "CORPORATION", "CORP", "COMPANY", "CO",
    "LIMITED", "LTD", "LLC", "LP", "LLP", "PLLC", "PC", "PA",
    "GROUP", "HOLDINGS", "ENTERPRISES", "SERVICES",
    "JOINT VENTURE", "JV", "DBA",
]


def get_sheets_service():
    """Authenticate and return Google Sheets API service."""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def normalize_company_name(name):
    """Normalize a company name for matching.
    Uppercase, strip suffixes (LLC, Inc, etc.), remove punctuation, collapse whitespace.
    """
    if not name:
        return ""
    n = name.upper().strip()
    # Remove punctuation
    n = re.sub(r"[.,;:'\"\-/\\()&]", " ", n)
    # Strip common suffixes (iterate multiple times for compounds like "INC LLC")
    for _ in range(3):
        for suffix in STRIP_SUFFIXES:
            pattern = r"\b" + re.escape(suffix) + r"\b"
            n = re.sub(pattern, "", n)
    # Collapse whitespace
    n = re.sub(r"\s+", " ", n).strip()
    return n


# --- Sheet reading ---

def read_scraper_data(service):
    """Read all rows from the scraper sheet. Returns list of lists."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SCRAPER_SHEET_ID,
        range=f"'{SCRAPER_SHEET_NAME}'!A:N",
    ).execute()
    return result.get("values", [])


def read_enrichment_data(service):
    """Read Companies_Enrichment tab from USA Spending sheet.
    Returns dict of normalized_name -> website_url.
    """
    result = service.spreadsheets().values().get(
        spreadsheetId=USASPENDING_SHEET_ID,
        range=f"'{ENRICHMENT_TAB}'!A:Z",
    ).execute()
    rows = result.get("values", [])
    if not rows:
        return {}

    # Find column indices from header row
    header = [h.strip().lower() for h in rows[0]]

    # Company name column
    name_col = None
    for i, h in enumerate(header):
        if "recipient" in h and "company" in h:
            name_col = i
            break
    if name_col is None:
        for i, h in enumerate(header):
            if "company" in h or "recipient" in h:
                name_col = i
                break
    if name_col is None:
        print("WARNING: Could not find company name column in enrichment data")
        return {}

    # Find website column
    website_col = None
    for i, h in enumerate(header):
        if "website" in h or "url" in h or "domain" in h:
            website_col = i
            break

    # If no header match, scan the first data row for URL-like values
    if website_col is None and len(rows) > 1:
        for i, val in enumerate(rows[1]):
            if val and ("http" in val.lower() or ".com" in val.lower() or ".org" in val.lower()):
                url_count = sum(
                    1 for r in rows[1:min(20, len(rows))]
                    if len(r) > i and r[i] and ("http" in r[i].lower() or "." in r[i])
                )
                if url_count >= 3:
                    website_col = i
                    break

    if website_col is None:
        print("WARNING: Could not find website column in enrichment data")
        return {}

    print(f"Enrichment: name_col={name_col} ({header[name_col]}), website_col={website_col}")

    lookup = {}
    skipped_junk = 0
    for row in rows[1:]:
        if len(row) <= max(name_col, website_col):
            continue
        name = row[name_col].strip() if name_col < len(row) else ""
        website = row[website_col].strip() if website_col < len(row) else ""
        if name and website and website.lower() not in ("", "no url", "n/a", "none"):
            # Skip junk/government domains in enrichment data
            try:
                domain = urlparse(website if "://" in website else f"https://{website}").netloc.lower()
                if domain.startswith("www."):
                    domain = domain[4:]
                if domain in JUNK_DOMAINS:
                    skipped_junk += 1
                    continue
            except Exception:
                pass
            norm = normalize_company_name(name)
            if norm:
                lookup[norm] = website
    if skipped_junk:
        print(f"  Skipped {skipped_junk} enrichment entries with junk/gov domains")
    return lookup


def read_blacklist(service):
    """Read Blacklist_Rules tab. Returns (exact_domains set, contains_patterns list)."""
    result = service.spreadsheets().values().get(
        spreadsheetId=USASPENDING_SHEET_ID,
        range=f"'{BLACKLIST_TAB}'!A:E",
    ).execute()
    rows = result.get("values", [])
    if not rows:
        return set(), []

    header = [h.strip().lower() for h in rows[0]]

    type_col = next((i for i, h in enumerate(header) if "type" in h or "rule" in h), 0)
    value_col = next((i for i, h in enumerate(header) if "value" in h or "match" in h or "domain" in h), 1)
    enabled_col = next((i for i, h in enumerate(header) if "enabled" in h or "active" in h), None)

    exact_domains = set()
    contains_patterns = []

    for row in rows[1:]:
        if len(row) <= max(type_col, value_col):
            continue

        if enabled_col is not None and len(row) > enabled_col:
            if row[enabled_col].strip().upper() not in ("TRUE", "YES", "1"):
                continue

        rule_type = row[type_col].strip().upper()
        match_value = row[value_col].strip().lower()

        if not match_value:
            continue

        if "CONTAINS" in rule_type:
            contains_patterns.append(match_value)
        else:
            exact_domains.add(match_value)

    return exact_domains, contains_patterns


# Domains that are never valid company websites
JUNK_DOMAINS = {
    # Search engines & big tech
    "google.com", "support.google.com", "docs.google.com", "maps.google.com",
    "play.google.com", "bing.com", "yahoo.com", "duckduckgo.com",
    "facebook.com", "meta.com", "twitter.com", "x.com",
    "linkedin.com", "instagram.com", "youtube.com", "reddit.com",
    "wikipedia.org", "amazon.com", "netflix.com", "chatgpt.com",
    # Business directories / review sites
    "yelp.com", "bbb.org", "glassdoor.com", "indeed.com",
    # Government
    "api.sam.gov", "sam.gov", "usaspending.gov",
    # Chinese/foreign spam
    "zhihu.com", "zhidao.baidu.com", "baidu.com", "bbs.csdn.net",
    "jingyan.baidu.com", "tieba.baidu.com",
    # Other junk
    "sporzip.com", "101-help.com", "tr.101-help.com",
    "lubimyczytac.pl", "globle.org",
}

# TLDs that are unlikely to be US construction company websites
JUNK_TLDS = {
    ".de", ".fr", ".pl", ".hu", ".gr", ".dk", ".ru", ".cn", ".jp",
    ".kr", ".br", ".it", ".es", ".pt", ".nl", ".se", ".no", ".fi",
    ".cz", ".sk", ".ro", ".bg", ".hr", ".rs", ".ua", ".by",
}


def extract_domain(url):
    """Extract clean domain from URL, stripping www."""
    try:
        domain = urlparse(url if "://" in url else f"https://{url}").netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def is_blacklisted(url, exact_domains, contains_patterns):
    """Check if a URL's domain is blacklisted or is a known junk domain."""
    domain = extract_domain(url)
    if not domain:
        return False

    if domain in JUNK_DOMAINS:
        return True

    for tld in JUNK_TLDS:
        if domain.endswith(tld):
            return True

    if domain in exact_domains:
        return True

    for pattern in contains_patterns:
        if pattern in domain:
            return True

    return False


# --- Brave Search with daily limit ---

def load_search_counter():
    """Load the daily search counter from file."""
    if not os.path.exists(SEARCH_COUNTER_FILE):
        return {"date": str(date.today()), "count": 0}
    try:
        with open(SEARCH_COUNTER_FILE) as f:
            data = json.load(f)
        if data.get("date") != str(date.today()):
            return {"date": str(date.today()), "count": 0}
        return data
    except Exception:
        return {"date": str(date.today()), "count": 0}


def save_search_counter(counter):
    """Save the daily search counter to file."""
    with open(SEARCH_COUNTER_FILE, "w") as f:
        json.dump(counter, f)


def brave_search(query, count=10):
    """Search using Brave Search API. Returns list of {url, title, description}."""
    if not BRAVE_API_KEY:
        print("  ERROR: BRAVE_API_KEY not set. Export it or add to environment.")
        return []

    from urllib.parse import urlencode
    params = urlencode({"q": query, "count": count, "country": "us", "search_lang": "en"})
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

        web_results = results.get("web", {}).get("results", [])
        return [
            {"url": r.get("url", ""), "title": r.get("title", ""), "description": r.get("description", "")}
            for r in web_results
        ]
    except Exception as e:
        print(f"  Brave search error: {e}")
        return []


def search_company_website(company_name, exact_domains, contains_patterns, counter):
    """Search Brave for a company's website. Returns URL or empty string.
    Respects daily limit and blacklist.
    """
    if counter["count"] >= SEARCH_DAILY_LIMIT:
        return ""

    query = f'{company_name} construction company website'
    time.sleep(SEARCH_DELAY)
    results = brave_search(query)
    counter["count"] += 1
    save_search_counter(counter)

    if not results:
        print(f"  BRAVE: no results for '{company_name}'")
        return ""

    for r in results:
        url = r.get("url", "")
        if not url:
            continue
        if is_blacklisted(url, exact_domains, contains_patterns):
            continue
        # Accept first non-blacklisted result
        parsed = urlparse(url)
        clean_url = f"{parsed.scheme}://{parsed.netloc}"
        return clean_url

    return ""


# --- Main enrichment ---

def enrich():
    """Main enrichment pipeline."""
    if not BRAVE_API_KEY:
        print("ERROR: Set BRAVE_API_KEY environment variable.")
        print("  Get a free key at: https://brave.com/search/api/")
        print("  Then run: export BRAVE_API_KEY=your_key_here")
        print("  (Enrichment matching will still run without it.)\n")

    service = get_sheets_service()

    # Step 1: Read scraper data
    print("Reading scraper data from Sheet A...")
    scraper_rows = read_scraper_data(service)
    if len(scraper_rows) < 2:
        print("No data found in scraper sheet.")
        return

    header = scraper_rows[0]
    data_rows = scraper_rows[1:]
    print(f"  Found {len(data_rows)} rows (columns: {len(header)})")

    # Column G (index 6) = Website
    WEBSITE_COL = 6
    COMPANY_COL = 1

    # Step 2: Read enrichment data
    print("Reading enrichment data from Sheet B (Companies_Enrichment)...")
    enrichment_lookup = read_enrichment_data(service)
    print(f"  Loaded {len(enrichment_lookup)} companies with websites")

    # Step 3: Read blacklist
    print("Reading blacklist rules from Sheet B (Blacklist_Rules)...")
    exact_domains, contains_patterns = read_blacklist(service)
    print(f"  Loaded {len(exact_domains)} exact domain rules, {len(contains_patterns)} contains rules")

    # Step 4: Match and enrich
    print("\nMatching companies...")
    counter = load_search_counter()
    print(f"  Brave searches today: {counter['count']}/{SEARCH_DAILY_LIMIT}")

    updates = {}  # row_index -> website_url
    stats = {"already_has": 0, "matched": 0, "brave_found": 0, "limit_skipped": 0, "no_match": 0}

    for i, row in enumerate(data_rows):
        while len(row) <= WEBSITE_COL:
            row.append("")

        company = row[COMPANY_COL].strip() if len(row) > COMPANY_COL else ""
        current_website = row[WEBSITE_COL].strip() if len(row) > WEBSITE_COL else ""

        if not company:
            continue

        # Check if current website is junk that should be cleared
        if current_website and is_blacklisted(current_website, exact_domains, contains_patterns):
            print(f"  CLEARING junk website for {company}: {current_website}")
            updates[i] = ""
            current_website = ""

        # Skip if already has a website
        if current_website and current_website.lower() not in ("", "n/a", "none"):
            stats["already_has"] += 1
            continue

        norm_name = normalize_company_name(company)

        # Try enrichment lookup first
        if norm_name in enrichment_lookup:
            website = enrichment_lookup[norm_name]
            if not is_blacklisted(website, exact_domains, contains_patterns):
                updates[i] = website
                stats["matched"] += 1
                print(f"  MATCH: {company} -> {website}")
                continue

        # Try Brave Search
        if BRAVE_API_KEY and counter["count"] < SEARCH_DAILY_LIMIT:
            website = search_company_website(
                company, exact_domains, contains_patterns, counter
            )
            if website:
                updates[i] = website
                stats["brave_found"] += 1
                print(f"  BRAVE: {company} -> {website}")
                continue
            stats["no_match"] += 1
        elif BRAVE_API_KEY:
            stats["limit_skipped"] += 1
        else:
            stats["no_match"] += 1

    # Step 5: Write updates back to Sheet A
    print(f"\n--- Results ---")
    print(f"  Already had website: {stats['already_has']}")
    print(f"  Matched from enrichment: {stats['matched']}")
    print(f"  Found via Brave Search: {stats['brave_found']}")
    print(f"  Search limit reached (skipped): {stats['limit_skipped']}")
    print(f"  No website found: {stats['no_match']}")
    print(f"  Total updates to write: {len(updates)}")

    if not updates:
        print("No updates needed.")
        return

    print(f"\nWriting {len(updates)} website updates to Sheet A...")

    batch_data = []
    for row_idx, website in updates.items():
        cell = f"'{SCRAPER_SHEET_NAME}'!G{row_idx + 2}"  # +2 for header row + 1-indexed
        batch_data.append({
            "range": cell,
            "values": [[website]],
        })

    for start in range(0, len(batch_data), 100):
        chunk = batch_data[start:start + 100]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SCRAPER_SHEET_ID,
            body={
                "valueInputOption": "RAW",
                "data": chunk,
            },
        ).execute()
        print(f"  Wrote batch {start // 100 + 1} ({len(chunk)} cells)")

    print(f"Done! Updated {len(updates)} company websites.")
    print(f"Brave searches used today: {counter['count']}/{SEARCH_DAILY_LIMIT}")


if __name__ == "__main__":
    enrich()
