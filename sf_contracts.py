"""
San Francisco Construction Contracts Scraper
Pulls construction contract data from SF Open Data portal using Socrata API.
Filters via SoQL query for efficiency.
Exports directly to Google Sheets.
"""

import json
import ssl
import sys
import urllib.parse
from datetime import datetime
from urllib.request import urlopen, Request

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
# Socrata API endpoint for SF Supplier Contracts dataset
SOCRATA_ENDPOINT = "https://data.sfgov.org/resource/cqi5-hm2d.json"

# Filter settings
MIN_AMOUNT = 1_000_000  # Award amount >= $1M
FILTER_YEAR = 2026      # Contracts starting in 2026

# Google Sheets config (same as Austin)
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

# Dataset base URL for constructing links
DATASET_URL = "https://data.sfgov.org/City-Management-and-Ethics/Supplier-Contracts/cqi5-hm2d"


def build_soql_query():
    """Build the SoQL $where clause for filtering."""
    conditions = [
        f"date_extract_y(term_start_date)={FILTER_YEAR}",
        "contract_type='Construction Contracts'",
        "project_team_constituent='Prime Contractor'",
        f"agreed_amt>={MIN_AMOUNT}",
    ]
    return " AND ".join(conditions)


def fetch_from_socrata():
    """Fetch data from Socrata API with SoQL filters."""
    where_clause = build_soql_query()
    params = {
        "$where": where_clause,
        "$limit": 10000,  # Max records
    }
    url = f"{SOCRATA_ENDPOINT}?{urllib.parse.urlencode(params)}"

    print(f"Socrata API Query:")
    print(f"  $where={where_clause}")
    print(f"Fetching from: {SOCRATA_ENDPOINT}")

    req = Request(url, headers={
        "User-Agent": "SFContractScraper/1.0",
        "Accept": "application/json",
    })
    with urlopen(req, context=SSL_CTX, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_date(date_str):
    """Parse Socrata date format (ISO 8601)."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except ValueError:
        return None


def parse_amount(amount_str):
    """Parse dollar amount string."""
    if not amount_str:
        return 0.0
    try:
        return float(str(amount_str).replace(",", "").replace("$", ""))
    except ValueError:
        return 0.0


def build_description(row):
    """Build a rich project description from available fields."""
    parts = []

    # Contract title is the main description
    title = row.get("contract_title", "").strip()
    if title and title.lower() not in ["x", "prime", "unspecified"]:
        parts.append(title)

    # Add scope of work if different and meaningful
    scope = row.get("scope_of_work", "").strip()
    if scope and scope.lower() not in ["x", "prime", "unspecified", title.lower()]:
        parts.append(scope)

    # Add department context
    dept = row.get("department", "").strip()
    if dept:
        parts.append(f"Dept: {dept}")

    # Add purchasing authority for context
    authority = row.get("purchasing_authority", "").strip()
    if authority:
        parts.append(f"({authority})")

    return " | ".join(parts) if parts else "Construction Contract"


def build_award_link(contract_number):
    """Build a link to view the contract in SF Open Data."""
    if contract_number:
        return f"{DATASET_URL}/data?Contract%20Number={contract_number}"
    return DATASET_URL


def scrape_all():
    """Main scrape pipeline: fetch from Socrata API -> transform -> return results."""
    data = fetch_from_socrata()

    print(f"\n--- Socrata API returned {len(data)} records ---")

    results = []
    seen = set()

    for row in data:
        contract_num = row.get("contract_no", "")
        supplier = row.get("prime_contractor", "")

        # Deduplicate by contract number + supplier
        dedup_key = (contract_num, supplier)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        start_date = parse_date(row.get("term_start_date", ""))
        award_amount = parse_amount(row.get("agreed_amt", "0"))
        payments_made = parse_amount(row.get("pmt_amt", "0"))

        results.append({
            "company_name": row.get("prime_contractor", ""),
            "contract_name": row.get("contract_title", ""),
            "award_amount": award_amount,
            "amount_expended": payments_made,
            "begin_date": start_date.strftime("%Y-%m-%d") if start_date else "",
            "award_link": build_award_link(contract_num),
            "description": build_description(row),
            "commodity_type": "Construction",
            "contact_name": "",  # Not available
            "address": "",       # Not available
            "phone": "",         # Not available
            "email": "",         # Not available
            "website": "",       # Not available
            "city": "San Francisco",
        })

    print(f"After deduplication: {len(results)} unique contracts")
    return results


# Sheet field mapping (same as Austin for consistency)
SHEET_FIELDS = [
    "city", "company_name", "contact_name", "phone", "email",
    "address", "website",
    "contract_name", "award_amount", "amount_expended", "begin_date",
    "award_link", "description", "commodity_type",
]
SHEET_HEADERS = [
    "City", "Company Name", "Contact Name", "Phone", "Email",
    "Address", "Website",
    "Contract Name", "Award Amount", "Amount Expended", "Begin Date",
    "Award Link", "Project Description", "Commodity Type",
]


def get_sheets_service():
    """Authenticate and return Google Sheets API service."""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)


def get_existing_data(service):
    """Fetch existing data from the sheet to append to."""
    sheet = service.spreadsheets()
    try:
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A:N"
        ).execute()
        return result.get("values", [])
    except Exception:
        return []


def write_to_google_sheets(results, append=True):
    """Write results to Google Sheets. If append=True, add to existing data."""
    service = get_sheets_service()
    sheet = service.spreadsheets()

    # Get the sheet ID for our target sheet
    spreadsheet = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id = None
    for s in spreadsheet.get("sheets", []):
        if s["properties"]["title"] == SHEET_NAME:
            sheet_id = s["properties"]["sheetId"]
            break

    if sheet_id is None:
        # Create the sheet if it doesn't exist
        request = {
            "requests": [{
                "addSheet": {
                    "properties": {"title": SHEET_NAME}
                }
            }]
        }
        sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=request).execute()
        print(f"Created new sheet: {SHEET_NAME}")

    if append:
        # Get existing data to find where to append
        existing = get_existing_data(service)
        start_row = len(existing) + 1

        # Deduplicate: skip rows already in the sheet
        existing_fps = {
            (row[1].strip().lower(), row[7].strip().lower())
            for row in (existing[1:] if len(existing) > 1 else [])
            if len(row) > 7
        }
        new_results = [
            r for r in results
            if (r.get("company_name", "").strip().lower(),
                r.get("contract_name", "").strip().lower()) not in existing_fps
        ]
        if len(new_results) < len(results):
            print(f"  Skipped {len(results) - len(new_results)} already-existing entries")
        results = new_results

        # If sheet is empty, add headers first
        if not existing:
            header_range = f"'{SHEET_NAME}'!A1"
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=header_range,
                valueInputOption="RAW",
                body={"values": [SHEET_HEADERS]}
            ).execute()
            start_row = 2

        # Build data rows
        rows = []
        for r in results:
            rows.append([r.get(f, "") for f in SHEET_FIELDS])

        if rows:
            write_range = f"'{SHEET_NAME}'!A{start_row}"
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=write_range,
                valueInputOption="RAW",
                body={"values": rows}
            ).execute()
    else:
        # Replace all data
        rows = [SHEET_HEADERS]
        for r in results:
            rows.append([r.get(f, "") for f in SHEET_FIELDS])

        # Clear existing data
        try:
            clear_range = f"'{SHEET_NAME}'!A1:Z1000"
            sheet.values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=clear_range,
                body={}
            ).execute()
        except Exception:
            pass

        # Write new data
        write_range = f"'{SHEET_NAME}'!A1"
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=write_range,
            valueInputOption="RAW",
            body={"values": rows}
        ).execute()

    print(f"Wrote {len(results)} rows to Google Sheet: {SHEET_NAME}")


def preview_results(results, limit=10):
    """Print a preview of the results."""
    print(f"\n--- Preview (first {min(limit, len(results))} results) ---\n")
    for i, r in enumerate(results[:limit], 1):
        print(f"{i}. {r['company_name']}")
        print(f"   Contract: {r['contract_name']}")
        print(f"   Award: ${r['award_amount']:,.2f} | Paid: ${r['amount_expended']:,.2f}")
        print(f"   Date: {r['begin_date']}")
        print(f"   Description: {r['description'][:100]}...")
        print()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="SF Construction Contracts Scraper")
    parser.add_argument("--preview", action="store_true", help="Preview results without writing to sheet")
    parser.add_argument("--append", action="store_true", help="Append to existing sheet data instead of replacing")
    args = parser.parse_args()

    results = scrape_all()

    if not results:
        print("\nNo contracts matched all filters.")
        print("Try adjusting MIN_DATE or MIN_AMOUNT if this is unexpected.")
        sys.exit(0)

    preview_results(results)

    if args.preview:
        print("Preview mode - not writing to Google Sheets")
        print(f"Run without --preview to write {len(results)} rows to the sheet")
    else:
        write_to_google_sheets(results, append=args.append)
        print(f"\nView results at: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
