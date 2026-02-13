"""
USA Spending Federal Contracts Scraper
Queries the USASpending.gov API for construction contracts by NAICS code,
filters for new awards from 2026, and appends to the Google Sheet that feeds
the Netlify dashboard at texasconstructiondata.netlify.app.

Append-only — never deletes existing rows.
"""

import json
import ssl
import time
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
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

API_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
PAGE_LIMIT = 100

NAICS_CODES = ["238210", "236220", "237310", "238220", "238120"]
NAICS_LABELS = {
    "238210": "Electrical",
    "236220": "Commercial Building",
    "237310": "Highway/Street/Bridge",
    "238220": "Plumbing/HVAC",
    "238120": "Structural Steel",
}

FILTER_YEAR = 2026


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def fetch_awards(page=1):
    """Fetch one page of new contract awards from the USA Spending API."""
    payload = {
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "naics_codes": {"require": NAICS_CODES},
            "time_period": [{
                "start_date": f"{FILTER_YEAR}-01-01",
                "end_date": f"{FILTER_YEAR}-12-31",
                "date_type": "new_awards_only",
            }],
            "award_amounts": [{"lower_bound": 250000}],
        },
        "fields": [
            "Recipient Name",
            "Place of Performance State Code",
            "Award Amount",
            "Total Outlays",
            "Start Date",
            "Description",
            "Award ID",
            "NAICS",
            "generated_internal_id",
        ],
        "limit": PAGE_LIMIT,
        "page": page,
        "sort": "Start Date",
        "order": "desc",
    }

    data = json.dumps(payload).encode("utf-8")
    for attempt in range(3):
        try:
            req = Request(API_URL, data=data, headers={"Content-Type": "application/json"})
            resp = urlopen(req, timeout=60, context=SSL_CTX)
            return json.loads(resp.read())
        except Exception as e:
            if attempt < 2:
                wait = 5 * (attempt + 1)
                print(f"    Request failed ({e}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def fetch_all_awards():
    """Fetch all pages of results from the API."""
    all_results = []
    page = 1

    while True:
        print(f"  Fetching page {page}...")
        resp = fetch_awards(page)
        results = resp.get("results", [])
        all_results.extend(results)

        has_next = resp.get("page_metadata", {}).get("hasNext", False)
        if not has_next or not results:
            break
        page += 1

    return all_results


def is_2026_start(award):
    """Check that the award's Start Date is actually in 2026."""
    start = award.get("Start Date") or ""
    return start.startswith(str(FILTER_YEAR))


def naics_label(naics):
    """Format NAICS as 'Label (NAICS code)'. e.g. 'Electrical (NAICS 238210)'."""
    if not naics:
        return ""
    if isinstance(naics, dict):
        code = naics.get("code", "")
    else:
        code = str(naics).strip()
    if not code:
        return ""
    label = NAICS_LABELS.get(code, code)
    return f"{label} (NAICS {code})"


KEEP_UPPER = {"LLC", "LP", "LLP", "PLLC", "LTD", "JV", "II", "III", "IV", "PC", "PA"}


def title_case(name):
    """Convert 'FISHER SAND & GRAVEL CO' to 'Fisher Sand & Gravel Co'."""
    if not name:
        return ""
    words = name.title().split()
    return " ".join(w.upper() if w.upper() in KEEP_UPPER else w for w in words)


def map_to_row(award):
    """Map an API result to the 14-column scraper sheet format."""
    state = award.get("Place of Performance State Code") or ""
    company = title_case(award.get("Recipient Name") or "")
    award_id = award.get("Award ID") or ""
    amount = award.get("Award Amount")
    outlays = award.get("Total Outlays")
    start_date = award.get("Start Date") or ""
    description = award.get("Description") or ""
    naics = award.get("NAICS") or ""
    internal_id = award.get("generated_internal_id") or ""

    link = f"https://www.usaspending.gov/award/{internal_id}" if internal_id else ""
    amount_str = f"${amount:,.2f}" if amount is not None else ""
    outlays_str = f"${outlays:,.2f}" if outlays is not None else ""

    return [
        state,          # A: State
        company,        # B: Company
        "",             # C: Contact
        "",             # D: Phone
        "",             # E: Email
        "",             # F: Address
        "",             # G: Website
        award_id,       # H: Contract Name
        amount_str,     # I: Amount
        outlays_str,    # J: Amount Expended
        start_date,     # K: Date
        link,           # L: Link
        description,    # M: Description
        naics_label(naics),  # N: Commodity Type
    ]


def read_existing_data(service):
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A:N",
    ).execute()
    return result.get("values", [])


def build_dedup_set(existing_rows):
    dupes = set()
    for row in existing_rows[1:]:
        company = (row[1] if len(row) > 1 else "").strip().upper()
        date_val = (row[10] if len(row) > 10 else "").strip()
        amount = (row[8] if len(row) > 8 else "").strip()
        if company:
            dupes.add((company, date_val, amount))
    return dupes


MIN_AMOUNT = 250_000


def parse_amount(val):
    """Parse a dollar string like '$1,250,000.00' to float."""
    if not val:
        return 0
    try:
        return float(val.replace("$", "").replace(",", ""))
    except ValueError:
        return 0


def remove_under_threshold(service):
    """Remove existing USA Spending rows under MIN_AMOUNT from Sheet A."""
    existing = read_existing_data(service)
    if len(existing) < 2:
        return

    rows_to_keep = [existing[0]]
    removed = 0
    for row in existing[1:]:
        link = (row[11] if len(row) > 11 else "").strip()
        amount = (row[8] if len(row) > 8 else "").strip()
        if "usaspending.gov" in link and parse_amount(amount) < MIN_AMOUNT:
            removed += 1
        else:
            rows_to_keep.append(row)

    if removed == 0:
        return

    print(f"Removing {removed} USA Spending rows under ${MIN_AMOUNT:,}...")
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A:N",
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A1",
        valueInputOption="RAW",
        body={"values": rows_to_keep},
    ).execute()
    print(f"  Done. {len(rows_to_keep) - 1} rows remain.")


def main():
    service = get_sheets_service()

    # Step 0: Clean out existing USA Spending rows under $250K
    remove_under_threshold(service)

    # Step 1: Fetch new awards from API
    print(f"Fetching {FILTER_YEAR} NEW construction contracts from USASpending.gov API...")
    print(f"  NAICS codes: {', '.join(NAICS_CODES)}")
    awards = fetch_all_awards()
    print(f"  Total from API: {len(awards)}")

    # Step 2: Post-filter — only keep awards with Start Date in 2026
    awards = [a for a in awards if is_2026_start(a)]
    print(f"  After Start Date {FILTER_YEAR} filter: {len(awards)}")

    if not awards:
        print("No 2026 awards found.")
        return

    # Step 3: Map to sheet rows
    mapped_rows = [map_to_row(a) for a in awards]

    # Step 4: Deduplicate against existing data
    print("\nChecking for duplicates against existing sheet data...")
    existing = read_existing_data(service)
    print(f"  Existing rows: {max(0, len(existing) - 1)}")
    dedup_set = build_dedup_set(existing)

    new_rows = []
    dupes = 0
    for row in mapped_rows:
        key = (row[1].strip().upper(), row[10].strip(), row[8].strip())
        if key in dedup_set:
            dupes += 1
        else:
            new_rows.append(row)

    print(f"  Duplicates skipped: {dupes}")
    print(f"  New rows to append: {len(new_rows)}")

    if not new_rows:
        print("All awards already exist in sheet. Nothing to do.")
        return

    # Step 5: Append to sheet (never clears existing data)
    start_row = len(existing) + 1
    write_range = f"'{SHEET_NAME}'!A{start_row}"

    print(f"\nAppending {len(new_rows)} rows (starting at row {start_row})...")
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=write_range,
        valueInputOption="RAW",
        body={"values": new_rows},
    ).execute()

    print(f"Done! Imported {len(new_rows)} federal construction contracts.")


if __name__ == "__main__":
    main()
