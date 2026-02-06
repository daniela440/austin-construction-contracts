"""
TxDOT (Texas Department of Transportation) Contracts Scraper
Pulls awarded contract data from Texas Open Data Portal using Socrata API.
Filters for low bidders (winners) via SoQL query.
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
# Socrata API endpoint for Texas Bid Tabulations dataset
SOCRATA_ENDPOINT = "https://data.texas.gov/resource/de7b-7dna.json"

# Filter settings
MIN_AMOUNT = 1_000_000  # Bid amount >= $1M
FILTER_YEAR = 2026      # Contracts let in 2026

# Google Sheets config (same as other scrapers)
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

# Dataset base URL for constructing links
DATASET_URL = "https://data.texas.gov/Transportation/Bid-Tabulations/de7b-7dna"


def build_soql_query():
    """Build the SoQL $where clause for filtering."""
    conditions = [
        "low_bidder_flag=true",  # Only winning bidders
        f"date_extract_y(project_actual_let_date)={FILTER_YEAR}",
        f"bid_total_amount>={MIN_AMOUNT}",
    ]
    return " AND ".join(conditions)


def fetch_from_socrata():
    """Fetch data from Socrata API with SoQL filters and grouping."""
    where_clause = build_soql_query()

    # Select distinct projects (the dataset has multiple rows per project for each bid item)
    params = {
        "$where": where_clause,
        "$select": "vendor_name,bid_total_amount,project_name,short_description,project_actual_let_date,county,district_division,highway,project_id",
        "$group": "vendor_name,bid_total_amount,project_name,short_description,project_actual_let_date,county,district_division,highway,project_id",
        "$limit": 10000,
    }
    url = f"{SOCRATA_ENDPOINT}?{urllib.parse.urlencode(params)}"

    print(f"Socrata API Query:")
    print(f"  $where={where_clause}")
    print(f"Fetching from: {SOCRATA_ENDPOINT}")

    req = Request(url, headers={
        "User-Agent": "TxDOTContractScraper/1.0",
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

    # Short description is the main work type
    short_desc = row.get("short_description", "").strip()
    if short_desc:
        parts.append(short_desc)

    # Project name adds context
    proj_name = row.get("project_name", "").strip()
    if proj_name and proj_name.lower() != short_desc.lower():
        parts.append(proj_name)

    # Highway info
    highway = row.get("highway", "").strip()
    if highway:
        parts.append(f"Hwy: {highway}")

    # Location
    county = row.get("county", "").strip()
    district = row.get("district_division", "").strip()
    if county and district:
        parts.append(f"{county} County, {district} District")
    elif county:
        parts.append(f"{county} County")

    return " | ".join(parts) if parts else "TxDOT Construction Contract"


def build_award_link(project_id):
    """Build a link to view the project in Texas Open Data."""
    if project_id:
        return f"{DATASET_URL}/data?project_id={project_id}"
    return DATASET_URL


def scrape_all():
    """Main scrape pipeline: fetch from Socrata API -> transform -> return results."""
    data = fetch_from_socrata()

    print(f"\n--- Socrata API returned {len(data)} records ---")

    results = []
    seen = set()

    for row in data:
        vendor = row.get("vendor_name", "")
        project_id = row.get("project_id", "")

        # Deduplicate by vendor + project
        dedup_key = (vendor, project_id)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        let_date = parse_date(row.get("project_actual_let_date", ""))
        bid_amount = parse_amount(row.get("bid_total_amount", "0"))

        results.append({
            "company_name": vendor,
            "contract_name": row.get("project_name", ""),
            "award_amount": bid_amount,
            "amount_expended": 0.0,  # Not available in bid data
            "begin_date": let_date.strftime("%Y-%m-%d") if let_date else "",
            "award_link": build_award_link(project_id),
            "description": build_description(row),
            "commodity_type": "Highway/Transportation Construction",
            "contact_name": "",  # Not available
            "address": "",       # Not available
            "phone": "",         # Not available
            "email": "",         # Not available
            "website": "",       # Not available
            "city": "Texas (TxDOT)",
        })

    print(f"After deduplication: {len(results)} unique contracts")
    return results


# Sheet field mapping (same as other scrapers for consistency)
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
        print(f"   Bid: ${r['award_amount']:,.2f}")
        print(f"   Date: {r['begin_date']}")
        print(f"   Description: {r['description'][:80]}...")
        print()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="TxDOT Contracts Scraper")
    parser.add_argument("--preview", action="store_true", help="Preview results without writing to sheet")
    parser.add_argument("--append", action="store_true", help="Append to existing sheet data instead of replacing")
    args = parser.parse_args()

    results = scrape_all()

    if not results:
        print("\nNo contracts matched all filters.")
        print("Try adjusting FILTER_YEAR or MIN_AMOUNT if this is unexpected.")
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
