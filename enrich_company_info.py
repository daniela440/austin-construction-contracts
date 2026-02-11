"""
Company Info Enrichment via Apollo.io
Reads companies from Sheet A, enriches via Apollo org endpoints,
writes to column O (Company Info).

Append-only — only fills empty Company Info cells, never deletes data.
"""

import json
import os
import re
import time
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
COL_WEBSITE = 6   # G: Website
COL_INFO = 14     # O: Company Info

APOLLO_ENRICH_URL = "https://api.apollo.io/v1/organizations/enrich"
APOLLO_SEARCH_URL = "https://api.apollo.io/v1/organizations/search"
SEARCH_DELAY = 1.0  # seconds between API calls


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


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


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


def enrich_by_domain(domain):
    """Call Apollo org enrichment by domain. Returns org dict or None."""
    try:
        resp = requests.post(
            APOLLO_ENRICH_URL,
            headers=HEADERS,
            json={"domain": domain},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json().get("organization")
    except Exception as e:
        print(f"    Apollo error: {e}")
    return None


def search_by_name(company_name):
    """Search Apollo for a company by name. Returns domain or empty string."""
    try:
        resp = requests.post(
            APOLLO_SEARCH_URL,
            headers=HEADERS,
            json={
                "q_organization_name": company_name,
                "page": 1,
                "per_page": 1,
            },
            timeout=30,
        )
        if resp.status_code == 200:
            orgs = resp.json().get("organizations", [])
            if orgs:
                return orgs[0].get("primary_domain", "")
    except Exception as e:
        print(f"    Apollo search error: {e}")
    return ""


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
        # Trim to first sentence or 200 chars
        desc = desc.strip()
        first_sentence_end = desc.find(". ")
        if 0 < first_sentence_end < 200:
            desc = desc[:first_sentence_end + 1]
        elif len(desc) > 200:
            desc = desc[:197] + "..."

        if summary:
            return f"{summary} — {desc}"
        return desc

    return summary


def read_sheet_data(service):
    """Read all rows from Sheet A including column O."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SCRAPER_SHEET_ID,
        range=f"'{SCRAPER_SHEET_NAME}'!A:O",
    ).execute()
    return result.get("values", [])


def write_updates(service, updates):
    """Write Company Info updates to column O in batches."""
    batch_data = []
    for row_num, info in updates.items():
        batch_data.append({
            "range": f"'{SCRAPER_SHEET_NAME}'!O{row_num}",
            "values": [[info]],
        })

    for start in range(0, len(batch_data), 100):
        chunk = batch_data[start:start + 100]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SCRAPER_SHEET_ID,
            body={"valueInputOption": "RAW", "data": chunk},
        ).execute()
        print(f"  Batch {start // 100 + 1}: {len(chunk)} cells")


def enrich():
    if not APOLLO_API_KEY:
        print("APOLLO_API_KEY not set in .env — exiting.")
        return

    service = get_sheets_service()

    # Add header if missing
    print("Checking column O header...")
    rows = read_sheet_data(service)
    if rows:
        header = rows[0]
        if len(header) <= COL_INFO or not header[COL_INFO].strip():
            service.spreadsheets().values().update(
                spreadsheetId=SCRAPER_SHEET_ID,
                range=f"'{SCRAPER_SHEET_NAME}'!O1",
                valueInputOption="RAW",
                body={"values": [["Company Info"]]},
            ).execute()
            print("  Added 'Company Info' header to column O")
            # Re-read after header update
            rows = read_sheet_data(service)

    if len(rows) < 2:
        print("No data in Sheet A.")
        return

    print(f"Total rows: {len(rows) - 1}")

    # Find companies that need enrichment (empty column O)
    need_enrichment = []
    already_has = 0

    for i, row in enumerate(rows[1:], start=2):
        company = (row[COL_COMPANY] if len(row) > COL_COMPANY else "").strip()
        existing_info = (row[COL_INFO] if len(row) > COL_INFO else "").strip()

        if not company:
            continue

        if existing_info:
            already_has += 1
            continue

        website = (row[COL_WEBSITE] if len(row) > COL_WEBSITE else "").strip()
        domain = extract_domain(website) if website else ""
        need_enrichment.append((i, company, domain))

    print(f"  Already have Company Info: {already_has}")
    print(f"  Need enrichment: {len(need_enrichment)}")

    if not need_enrichment:
        print("Nothing to do.")
        return

    # Deduplicate domains to avoid redundant API calls
    domain_cache = {}  # domain -> formatted info
    name_domain_cache = {}  # company_name -> domain (from search)

    updates = {}
    found = 0
    not_found = 0

    for idx, (row_num, company, domain) in enumerate(need_enrichment, 1):
        if idx % 25 == 0 or idx == 1:
            print(f"  [{idx}/{len(need_enrichment)}]...")

        info = ""

        # Step 1: Try enrichment by domain if we have one
        if domain:
            if domain in domain_cache:
                info = domain_cache[domain]
            else:
                org = enrich_by_domain(domain)
                info = format_info(org)
                domain_cache[domain] = info
                time.sleep(SEARCH_DELAY)

        # Step 2: If no domain or enrichment failed, search by name
        if not info:
            if company in name_domain_cache:
                found_domain = name_domain_cache[company]
            else:
                found_domain = search_by_name(company)
                name_domain_cache[company] = found_domain
                time.sleep(SEARCH_DELAY)

            if found_domain and found_domain not in domain_cache:
                org = enrich_by_domain(found_domain)
                info = format_info(org)
                domain_cache[found_domain] = info
                time.sleep(SEARCH_DELAY)
            elif found_domain:
                info = domain_cache.get(found_domain, "")

        if info:
            updates[row_num] = info
            found += 1
            print(f"    {company} -> {info[:80]}...")
        else:
            not_found += 1

        # Write in batches of 50
        if len(updates) >= 50:
            print(f"\n  Writing batch of {len(updates)} results...")
            write_updates(service, updates)
            updates = {}

    # Write remaining
    if updates:
        print(f"\n  Writing final {len(updates)} results...")
        write_updates(service, updates)

    print(f"\n--- Company Info Enrichment ---")
    print(f"  Found: {found}")
    print(f"  Not found: {not_found}")
    print(f"  API calls saved by cache: {sum(1 for _, _, d in need_enrichment if d in domain_cache) - len(domain_cache)}")
    print("Done!")


if __name__ == "__main__":
    enrich()
