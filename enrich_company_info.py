"""
Unified Company Enrichment via Apollo.io
Reads companies from Sheet A and enriches:
  - Website (col G) via Apollo org data
  - Company Info (col O) via Apollo org enrichment
  - Contact Name (col C), Phone (col D), Email (col E), Title (col P)
    via Apollo people search + match

First run: processes all companies.
Subsequent runs: max 50 companies per week.
"""

import json
import os
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Config ---
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SCRAPER_SHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SCRAPER_SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

# Sheet A column indices (0-based)
COL_COMPANY = 1   # B: Company Name
COL_CONTACT = 2   # C: Contact
COL_PHONE = 3     # D: Phone
COL_EMAIL = 4     # E: Email
COL_WEBSITE = 6   # G: Website
COL_INFO = 14     # O: Company Info
COL_TITLE = 15    # P: Title

APOLLO_PEOPLE_SEARCH_URL = "https://api.apollo.io/v1/mixed_people/api_search"
APOLLO_PEOPLE_MATCH_URL = "https://api.apollo.io/v1/people/match"
APOLLO_ORG_ENRICH_URL = "https://api.apollo.io/v1/organizations/enrich"
APOLLO_ORG_SEARCH_URL = "https://api.apollo.io/v1/organizations/search"
API_DELAY = 1.0  # seconds between API calls

# Weekly limit for subsequent runs (first run is unlimited)
WEEKLY_LIMIT = 50
COUNTER_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "apollo_enrichment_counter.json")

# Preferred titles in priority order (safety first, then management)
PREFERRED_TITLES = [
    "Safety Manager", "Safety Director", "HSE Manager", "EHS Manager",
    "Director of Safety", "VP of Safety", "Health and Safety",
    "Environmental Health and Safety", "Safety Coordinator",
    "Safety Officer", "Safety Specialist", "Safety Superintendent",
    "Risk Manager", "Loss Prevention",
    "Project Manager", "Construction Manager", "Operations Manager",
    "Superintendent", "General Manager",
    "Vice President", "President", "CEO", "Owner",
]


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
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")

HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": APOLLO_API_KEY,
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
}


# --- Weekly counter ---

def get_week_key():
    """ISO week like '2026-W07'."""
    now = datetime.now()
    return f"{now.year}-W{now.isocalendar()[1]:02d}"


def load_counter():
    """Load weekly enrichment counter. Returns (week_key, count)."""
    if os.path.exists(COUNTER_FILE):
        with open(COUNTER_FILE) as f:
            data = json.load(f)
        week = data.get("week", "")
        count = data.get("count", 0)
        if week == get_week_key():
            return week, count
    return get_week_key(), 0


def save_counter(count):
    """Save weekly enrichment counter."""
    with open(COUNTER_FILE, "w") as f:
        json.dump({"week": get_week_key(), "count": count}, f)


# --- Sheets ---

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def read_sheet_data(service):
    """Read all rows from Sheet A including columns A through P."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SCRAPER_SHEET_ID,
        range=f"'{SCRAPER_SHEET_NAME}'!A:P",
    ).execute()
    return result.get("values", [])


def write_batch(service, updates):
    """Write a batch of cell updates. updates = list of (range, value) tuples."""
    if not updates:
        return
    batch_data = [{"range": r, "values": [[v]]} for r, v in updates]
    for start in range(0, len(batch_data), 100):
        chunk = batch_data[start:start + 100]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SCRAPER_SHEET_ID,
            body={"valueInputOption": "RAW", "data": chunk},
        ).execute()
        print(f"  Wrote batch: {len(chunk)} cells")


# --- Apollo API ---

def extract_domain(url):
    """Extract clean domain from URL."""
    if not url:
        return ""
    try:
        domain = urlparse(url if "://" in url else f"https://{url}").netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def search_people(company_name):
    """Search Apollo for people at a company. Returns list of people dicts."""
    try:
        resp = requests.post(
            APOLLO_PEOPLE_SEARCH_URL,
            headers=HEADERS,
            json={
                "q_organization_name": company_name,
                "page": 1,
                "per_page": 25,
            },
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json().get("people", [])
        elif resp.status_code == 429:
            print("    Rate limited, waiting 60s...")
            time.sleep(60)
    except Exception as e:
        print(f"    People search error: {e}")
    return []


def match_person(person_id):
    """Match a person by ID to reveal full contact details + org data."""
    try:
        resp = requests.post(
            APOLLO_PEOPLE_MATCH_URL,
            headers=HEADERS,
            json={"id": person_id, "reveal_personal_emails": False},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json().get("person")
        elif resp.status_code == 429:
            print("    Rate limited, waiting 60s...")
            time.sleep(60)
    except Exception as e:
        print(f"    People match error: {e}")
    return None


def enrich_org_by_domain(domain):
    """Enrich organization by domain. Returns org dict or None."""
    try:
        resp = requests.post(
            APOLLO_ORG_ENRICH_URL,
            headers=HEADERS,
            json={"domain": domain},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json().get("organization")
    except Exception as e:
        print(f"    Org enrich error: {e}")
    return None


def search_org_by_name(company_name):
    """Search Apollo for an org by name. Returns org dict or None."""
    try:
        resp = requests.post(
            APOLLO_ORG_SEARCH_URL,
            headers=HEADERS,
            json={"q_organization_name": company_name, "page": 1, "per_page": 1},
            timeout=30,
        )
        if resp.status_code == 200:
            orgs = resp.json().get("organizations", [])
            if orgs:
                return orgs[0]
    except Exception as e:
        print(f"    Org search error: {e}")
    return None


# --- Formatting ---

def pick_best_person(people):
    """Pick the best person from search results based on title priority."""
    if not people:
        return None

    # Score each person by title match
    scored = []
    for p in people:
        title = (p.get("title") or "").lower()
        if not title:
            continue
        best_score = len(PREFERRED_TITLES) + 1  # worst
        for i, pref in enumerate(PREFERRED_TITLES):
            if pref.lower() in title:
                best_score = i
                break
        scored.append((best_score, p))

    if not scored:
        # No title matches — return first person with a title
        for p in people:
            if p.get("title"):
                return p
        return people[0] if people else None

    scored.sort(key=lambda x: x[0])
    return scored[0][1]


def format_info(org):
    """Format org data into a readable Company Info string."""
    if not org:
        return ""

    parts = []

    industry = org.get("industry", "")
    if industry:
        parts.append(industry.title())

    employees = org.get("estimated_num_employees")
    if employees:
        parts.append(f"{employees:,} employees")

    revenue = org.get("annual_revenue_printed", "")
    if revenue:
        parts.append(f"${revenue} revenue")

    founded = org.get("founded_year")
    if founded:
        parts.append(f"Est. {founded}")

    city = org.get("city", "")
    state = org.get("state", "")
    if city and state:
        parts.append(f"{city}, {state}")
    elif state:
        parts.append(state)

    summary = " | ".join(parts)

    desc = org.get("short_description", "")
    if desc:
        desc = desc.strip()
        # Keep full description up to 500 chars for richer info
        if len(desc) > 500:
            desc = desc[:497] + "..."
        if summary:
            return f"{summary} — {desc}"
        return desc

    return summary


def format_website(org):
    """Extract website URL from org data."""
    url = org.get("website_url", "") or ""
    if not url:
        domain = org.get("primary_domain", "")
        if domain:
            url = f"https://{domain}"
    return url


# --- Main enrichment ---

def enrich(first_run=False):
    if not APOLLO_API_KEY:
        print("APOLLO_API_KEY not set in .env — exiting.")
        return

    # Check weekly limit
    week_key, week_count = load_counter()
    if not first_run and week_count >= WEEKLY_LIMIT:
        print(f"Weekly limit reached ({week_count}/{WEEKLY_LIMIT} for {week_key}).")
        print("Use --first-run flag to override, or wait until next week.")
        return

    remaining = None if first_run else (WEEKLY_LIMIT - week_count)
    if not first_run:
        print(f"Weekly budget: {remaining} remaining ({week_count}/{WEEKLY_LIMIT} used in {week_key})")

    service = get_sheets_service()

    # Ensure headers exist for new columns
    print("Checking column headers...")
    rows = read_sheet_data(service)
    if rows:
        header = rows[0]
        header_updates = []
        if len(header) <= COL_INFO or not header[COL_INFO].strip():
            header_updates.append((f"'{SCRAPER_SHEET_NAME}'!O1", "Company Info"))
        if len(header) <= COL_TITLE or not header[COL_TITLE].strip():
            header_updates.append((f"'{SCRAPER_SHEET_NAME}'!P1", "Title"))
        if header_updates:
            write_batch(service, header_updates)
            print(f"  Added {len(header_updates)} column headers")
            rows = read_sheet_data(service)

    if len(rows) < 2:
        print("No data in Sheet A.")
        return

    print(f"Total rows: {len(rows) - 1}")

    # Find companies that need enrichment
    need_enrichment = []
    complete = 0

    for i, row in enumerate(rows[1:], start=2):
        company = (row[COL_COMPANY] if len(row) > COL_COMPANY else "").strip()
        if not company:
            continue

        website = (row[COL_WEBSITE] if len(row) > COL_WEBSITE else "").strip()
        contact = (row[COL_CONTACT] if len(row) > COL_CONTACT else "").strip()
        email = (row[COL_EMAIL] if len(row) > COL_EMAIL else "").strip()
        info = (row[COL_INFO] if len(row) > COL_INFO else "").strip()
        title = (row[COL_TITLE] if len(row) > COL_TITLE else "").strip()

        has_website = bool(website and website.lower() not in ("", "n/a", "none"))
        has_contact = bool(contact)
        has_info = bool(info)

        if first_run:
            # First run: enrich all — update missing fields
            if has_website and has_contact and has_info:
                complete += 1
                continue
        else:
            # Subsequent runs: only process companies with no enrichment at all
            if has_info or has_contact:
                complete += 1
                continue

        need_enrichment.append({
            "row": i,
            "company": company,
            "website": website if has_website else "",
            "has_website": has_website,
            "has_contact": has_contact,
            "has_info": has_info,
        })

    print(f"  Already complete: {complete}")
    print(f"  Need enrichment: {len(need_enrichment)}")

    if not need_enrichment:
        print("Nothing to do.")
        return

    # Apply weekly limit
    if remaining is not None and len(need_enrichment) > remaining:
        print(f"  Limiting to {remaining} (weekly budget)")
        need_enrichment = need_enrichment[:remaining]

    # Process companies
    processed = 0
    stats = {"website": 0, "info": 0, "contact": 0, "no_result": 0}
    all_updates = []

    for idx, item in enumerate(need_enrichment, 1):
        row_num = item["row"]
        company = item["company"]
        domain = extract_domain(item["website"]) if item["website"] else ""

        if idx % 10 == 0 or idx == 1:
            print(f"\n  [{idx}/{len(need_enrichment)}] {company}...")

        org = None
        person_data = None

        # Step 1: Search for people at this company
        if not item["has_contact"]:
            people = search_people(company)
            time.sleep(API_DELAY)

            if people:
                best = pick_best_person(people)
                if best and best.get("id"):
                    person_data = match_person(best["id"])
                    time.sleep(API_DELAY)

                    # Person match also returns full org data
                    if person_data:
                        org = person_data.get("organization")

        # Step 2: If no org from person match, get org data directly
        if not org and not item["has_info"]:
            if domain:
                org = enrich_org_by_domain(domain)
                time.sleep(API_DELAY)
            else:
                found_org = search_org_by_name(company)
                time.sleep(API_DELAY)
                if found_org:
                    found_domain = found_org.get("primary_domain", "")
                    if found_domain:
                        org = enrich_org_by_domain(found_domain)
                        time.sleep(API_DELAY)

        # Build updates for this row
        row_updates = []

        # Website
        if not item["has_website"] and org:
            website_url = format_website(org)
            if website_url:
                row_updates.append((f"'{SCRAPER_SHEET_NAME}'!G{row_num}", website_url))
                stats["website"] += 1

        # Company Info
        if not item["has_info"] and org:
            info_text = format_info(org)
            if info_text:
                row_updates.append((f"'{SCRAPER_SHEET_NAME}'!O{row_num}", info_text))
                stats["info"] += 1

        # Contact, Title, Email, Phone
        if not item["has_contact"] and person_data:
            name = person_data.get("name") or ""
            if not name:
                first = person_data.get("first_name", "") or ""
                last = person_data.get("last_name", "") or ""
                name = f"{first} {last}".strip()

            title = person_data.get("title", "") or ""
            email = person_data.get("email", "") or ""

            # Phone from person or org
            phone = ""
            phone_numbers = person_data.get("phone_numbers", [])
            if phone_numbers:
                phone = phone_numbers[0].get("sanitized_number", "") or phone_numbers[0].get("number", "")
            if not phone and org:
                phone = org.get("phone", "") or ""

            if name:
                row_updates.append((f"'{SCRAPER_SHEET_NAME}'!C{row_num}", name))
                stats["contact"] += 1
            if title:
                row_updates.append((f"'{SCRAPER_SHEET_NAME}'!P{row_num}", title))
            if email:
                row_updates.append((f"'{SCRAPER_SHEET_NAME}'!E{row_num}", email))
            if phone:
                row_updates.append((f"'{SCRAPER_SHEET_NAME}'!D{row_num}", phone))

        if row_updates:
            all_updates.extend(row_updates)
            fields = [u[0].split("!")[1][0] for u in row_updates]
            print(f"    {company} -> cols {','.join(fields)}")
        else:
            stats["no_result"] += 1

        processed += 1

        # Write in batches of 200 cells
        if len(all_updates) >= 200:
            print(f"\n  Writing batch of {len(all_updates)} cell updates...")
            write_batch(service, all_updates)
            all_updates = []

    # Write remaining
    if all_updates:
        print(f"\n  Writing final {len(all_updates)} cell updates...")
        write_batch(service, all_updates)

    # Update counter
    _, current_count = load_counter()
    save_counter(current_count + processed)

    print(f"\n--- Enrichment Summary ---")
    print(f"  Processed: {processed}")
    print(f"  Websites added: {stats['website']}")
    print(f"  Company Info added: {stats['info']}")
    print(f"  Contacts found: {stats['contact']}")
    print(f"  No results: {stats['no_result']}")
    week_key, new_count = load_counter()
    print(f"  Weekly usage: {new_count}/{WEEKLY_LIMIT} ({week_key})")
    print("Done!")


if __name__ == "__main__":
    import sys
    first_run = "--first-run" in sys.argv
    if first_run:
        print("=== FIRST RUN MODE — processing all companies ===\n")
    enrich(first_run=first_run)
