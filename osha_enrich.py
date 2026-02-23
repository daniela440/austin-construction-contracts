"""
One-time Apollo enrichment for the OSHA company audits sheet.
Enriches 2026 batch only (rows 5–658).
Adds columns R–W: Website, Contact Name, Title, Email, Phone, Company Info.
"""

import json
import os
import time
from urllib.parse import urlparse

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Config ---
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "OSHA company audits"

HEADER_ROW = 4
DATA_START_ROW = 5
DATA_END_ROW = 658  # last 2026 row

# Column indices (0-based) in the sheet
COL_COMPANY = 0   # A
COL_WEBSITE = 17  # R
COL_CONTACT = 18  # S
COL_TITLE   = 19  # T
COL_EMAIL   = 20  # U
COL_PHONE   = 21  # V
COL_INFO    = 22  # W

NEW_HEADERS = ["Website", "Contact Name", "Title", "Email", "Phone", "Company Info"]

APOLLO_PEOPLE_SEARCH_URL = "https://api.apollo.io/v1/mixed_people/api_search"
APOLLO_PEOPLE_MATCH_URL  = "https://api.apollo.io/v1/people/match"
APOLLO_ORG_ENRICH_URL    = "https://api.apollo.io/v1/organizations/enrich"
APOLLO_ORG_SEARCH_URL    = "https://api.apollo.io/v1/organizations/search"
API_DELAY = 1.2

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

# Write results to sheet every N companies to preserve progress
FLUSH_EVERY = 10


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


def read_2026_rows(service):
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A{DATA_START_ROW}:W{DATA_END_ROW}",
    ).execute()
    return result.get("values", [])


def write_batch(service, updates):
    if not updates:
        return
    batch_data = [{"range": r, "values": [[v]]} for r, v in updates]
    for start in range(0, len(batch_data), 100):
        chunk = batch_data[start:start + 100]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": chunk},
        ).execute()


def extract_domain(url):
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
    try:
        resp = requests.post(
            APOLLO_PEOPLE_SEARCH_URL,
            headers=HEADERS,
            json={"q_organization_name": company_name, "page": 1, "per_page": 25},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json().get("people", [])
        elif resp.status_code == 429:
            print("    Rate limited, waiting 60s...")
            time.sleep(60)
            return search_people(company_name)
    except Exception as e:
        print(f"    People search error: {e}")
    return []


def match_person(person_id):
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
            return match_person(person_id)
    except Exception as e:
        print(f"    People match error: {e}")
    return None


def enrich_org_by_domain(domain):
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


def pick_best_person(people):
    if not people:
        return None
    scored = []
    for p in people:
        title = (p.get("title") or "").lower()
        if not title:
            continue
        best_score = len(PREFERRED_TITLES) + 1
        for i, pref in enumerate(PREFERRED_TITLES):
            if pref.lower() in title:
                best_score = i
                break
        scored.append((best_score, p))
    if not scored:
        for p in people:
            if p.get("title"):
                return p
        return people[0] if people else None
    scored.sort(key=lambda x: x[0])
    return scored[0][1]


def format_info(org):
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
    desc = (org.get("short_description", "") or "").strip()
    if len(desc) > 500:
        desc = desc[:497] + "..."
    if summary and desc:
        return f"{summary} — {desc}"
    return summary or desc


def format_website(org):
    url = org.get("website_url", "") or ""
    if not url:
        domain = org.get("primary_domain", "")
        if domain:
            url = f"https://{domain}"
    return url


def main():
    if not APOLLO_API_KEY:
        print("APOLLO_API_KEY not set in .env — exiting.")
        return

    service = get_sheets_service()
    sheet = service.spreadsheets()

    # Add new column headers to row 4
    print("Adding column headers R–W to row 4...")
    header_updates = [
        (f"'{SHEET_NAME}'!R{HEADER_ROW}", "Website"),
        (f"'{SHEET_NAME}'!S{HEADER_ROW}", "Contact Name"),
        (f"'{SHEET_NAME}'!T{HEADER_ROW}", "Title"),
        (f"'{SHEET_NAME}'!U{HEADER_ROW}", "Email"),
        (f"'{SHEET_NAME}'!V{HEADER_ROW}", "Phone"),
        (f"'{SHEET_NAME}'!W{HEADER_ROW}", "Company Info"),
    ]
    write_batch(service, header_updates)
    print("  Headers added.")

    # Read all 2026 rows
    rows = read_2026_rows(service)
    total = len(rows)
    print(f"2026 rows to process: {total}")

    stats = {"website": 0, "contact": 0, "info": 0, "no_result": 0}
    pending_updates = []
    processed = 0

    for i, row in enumerate(rows):
        sheet_row = DATA_START_ROW + i
        company = row[COL_COMPANY].strip() if len(row) > COL_COMPANY else ""
        if not company:
            continue

        # Skip if already enriched (has website or info)
        already_website = (row[COL_WEBSITE].strip() if len(row) > COL_WEBSITE else "")
        already_info    = (row[COL_INFO].strip()    if len(row) > COL_INFO    else "")
        if already_website or already_info:
            processed += 1
            continue

        if (i + 1) % 10 == 0 or i == 0:
            print(f"\n  [{i+1}/{total}] {company}...")

        org = None
        person_data = None

        # Step 1: Search for people
        people = search_people(company)
        time.sleep(API_DELAY)
        if people:
            best = pick_best_person(people)
            if best and best.get("id"):
                person_data = match_person(best["id"])
                time.sleep(API_DELAY)
                if person_data:
                    org = person_data.get("organization")

        # Step 2: Org enrichment if no org from person
        if not org:
            found_org = search_org_by_name(company)
            time.sleep(API_DELAY)
            if found_org:
                domain = found_org.get("primary_domain", "")
                if domain:
                    org = enrich_org_by_domain(domain)
                    time.sleep(API_DELAY)

        # Build updates
        enriched_cols = []

        if org:
            website_url = format_website(org)
            if website_url:
                pending_updates.append((f"'{SHEET_NAME}'!R{sheet_row}", website_url))
                stats["website"] += 1
                enriched_cols.append("R")

            info_text = format_info(org)
            if info_text:
                pending_updates.append((f"'{SHEET_NAME}'!W{sheet_row}", info_text))
                stats["info"] += 1
                enriched_cols.append("W")
        else:
            stats["no_result"] += 1

        if person_data:
            name = person_data.get("name") or ""
            if not name:
                first = person_data.get("first_name", "") or ""
                last  = person_data.get("last_name", "") or ""
                name  = f"{first} {last}".strip()
            title = person_data.get("title", "") or ""
            email = person_data.get("email", "") or ""
            phone = ""
            for num in (person_data.get("phone_numbers") or []):
                raw = num.get("sanitized_number") or num.get("raw_number") or ""
                if raw:
                    phone = raw
                    break

            if name:
                pending_updates.append((f"'{SHEET_NAME}'!S{sheet_row}", name))
                enriched_cols.append("S")
            if title:
                pending_updates.append((f"'{SHEET_NAME}'!T{sheet_row}", title))
                enriched_cols.append("T")
            if email:
                pending_updates.append((f"'{SHEET_NAME}'!U{sheet_row}", email))
                enriched_cols.append("U")
            if phone:
                pending_updates.append((f"'{SHEET_NAME}'!V{sheet_row}", phone))
                enriched_cols.append("V")
            if enriched_cols:
                stats["contact"] += 1

        if enriched_cols:
            print(f"    {company} -> cols {','.join(enriched_cols)}")

        processed += 1

        # Flush to sheet every FLUSH_EVERY companies
        if processed % FLUSH_EVERY == 0 and pending_updates:
            write_batch(service, pending_updates)
            print(f"  [Flushed {len(pending_updates)} updates to sheet]")
            pending_updates = []

    # Final flush
    if pending_updates:
        write_batch(service, pending_updates)
        print(f"  [Final flush: {len(pending_updates)} updates]")

    print(f"\n--- OSHA Enrichment Complete ---")
    print(f"  Processed:     {processed}")
    print(f"  Websites:      {stats['website']}")
    print(f"  Company Info:  {stats['info']}")
    print(f"  Contacts:      {stats['contact']}")
    print(f"  No result:     {stats['no_result']}")


if __name__ == "__main__":
    main()
