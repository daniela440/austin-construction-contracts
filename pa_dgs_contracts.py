"""
Pennsylvania DGS Construction Contracts Scraper
Pulls awarded construction contracts from PA Department of General Services.
Filters by amount threshold.
Exports directly to Google Sheets.
"""

import html
import re
import ssl
import sys
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
DGS_AWARDS_URL = "https://www.pa.gov/agencies/dgs/submit-proposals-and-bids-for-commonwealth-projects"
MIN_AMOUNT = 1_000_000  # $1M minimum

# Google Sheets config
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"


def fetch_page(url):
    """Fetch a URL and return decoded HTML."""
    req = Request(url, headers={
        "User-Agent": "PAContractScraper/1.0",
        "Accept": "text/html",
    })
    with urlopen(req, context=SSL_CTX, timeout=120) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parse_amount(amount_str):
    """Parse dollar amount string."""
    if not amount_str:
        return 0.0
    try:
        # Remove $, commas, and whitespace
        cleaned = re.sub(r'[$,\s]', '', amount_str)
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_date(date_str):
    """Parse date in various formats."""
    if not date_str:
        return None

    # Try common formats
    formats = [
        "%B %d, %Y",      # November 12, 2025
        "%b %d, %Y",      # Nov 12, 2025
        "%m/%d/%Y",       # 11/12/2025
        "%Y-%m-%d",       # 2025-11-12
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def categorize_project(project_num, contractor_name):
    """Categorize project by NAICS based on project number prefix or contractor type."""
    project_num = project_num.upper() if project_num else ""
    contractor = contractor_name.lower() if contractor_name else ""

    # DGS project number prefixes often indicate type
    # P1 = General Construction, P2 = HVAC, P3 = Plumbing, P4 = Electrical
    if "P4" in project_num or "electrical" in contractor:
        return "238210", "Electrical"
    elif "P2" in project_num or "mechanical" in contractor or "hvac" in contractor:
        return "238220", "Plumbing/HVAC"
    elif "P3" in project_num or "plumbing" in contractor:
        return "238220", "Plumbing/HVAC"
    elif "P1" in project_num:
        return "236220", "Commercial Building"
    else:
        return "236220", "Commercial Building"  # Default for state buildings


def scrape_awards():
    """Scrape DGS construction contract awards page."""
    print(f"Fetching DGS awards from: {DGS_AWARDS_URL}")
    page = fetch_page(DGS_AWARDS_URL)

    results = []
    seen = set()

    # The awards are in <li> tags with comma-separated format:
    # "<li>November 12, 2025, DGS 401-63.1 P1, Terra Technical Services, LLC, $3,417,287.00</li>"

    # Pattern matches: Date, DGS Project, Contractor, $Amount
    award_pattern = re.compile(
        r'<li>([A-Z][a-z]+\s+\d{1,2},?\s*\d{4}),\s*'      # Date (Month DD, YYYY)
        r'DGS\s+([\w\-\.]+(?:\s*P\d+)?),\s*'              # Project number
        r'(.+?),\s*'                                       # Contractor name
        r'\$?([0-9,]+(?:\.\d{2})?)',                       # Amount
        re.IGNORECASE
    )

    for m in award_pattern.finditer(page):
        date_str, project_num, contractor, amount_str = m.groups()

        project_num = project_num.strip()
        contractor = html.unescape(contractor.strip())
        # Clean up HTML entities
        contractor = contractor.replace("&amp;", "&")
        amount = parse_amount(amount_str)
        award_date = parse_date(date_str)

        # Apply amount filter
        if amount < MIN_AMOUNT:
            continue

        # Deduplicate
        dedup_key = (project_num, contractor)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        naics_code, naics_label = categorize_project(project_num, contractor)

        results.append({
            "company_name": contractor,
            "contract_name": f"DGS {project_num}",
            "award_amount": amount,
            "amount_expended": "",  # Not available
            "begin_date": award_date.strftime("%Y-%m-%d") if award_date else "",
            "award_link": DGS_AWARDS_URL,
            "description": "PA DGS Construction Contract",
            "commodity_type": f"{naics_label} (NAICS {naics_code})",
            "contact_name": "",
            "address": "",
            "phone": "",
            "email": "",
            "website": "",
            "city": "Pennsylvania",
        })

    print(f"Found {len(results)} awards >= ${MIN_AMOUNT:,}")
    return results


# Sheet field mapping
SHEET_FIELDS = [
    "city", "company_name", "contact_name", "phone", "email",
    "address", "website",
    "contract_name", "award_amount", "amount_expended", "begin_date",
    "award_link", "description", "commodity_type",
]
SHEET_HEADERS = [
    "State", "Company Name", "Contact Name", "Phone", "Email",
    "Address", "Website",
    "Contract Name", "Award Amount", "Amount Expended", "Award Date",
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
        existing = get_existing_data(service)
        start_row = len(existing) + 1

        if not existing:
            header_range = f"'{SHEET_NAME}'!A1"
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=header_range,
                valueInputOption="RAW",
                body={"values": [SHEET_HEADERS]}
            ).execute()
            start_row = 2

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
        rows = [SHEET_HEADERS]
        for r in results:
            rows.append([r.get(f, "") for f in SHEET_FIELDS])

        try:
            clear_range = f"'{SHEET_NAME}'!A1:Z1000"
            sheet.values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=clear_range,
                body={}
            ).execute()
        except Exception:
            pass

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
        print(f"   Award: ${r['award_amount']:,.2f}")
        print(f"   Date: {r['begin_date']}")
        print(f"   Type: {r['commodity_type']}")
        print()


def main():
    global MIN_AMOUNT

    import argparse
    parser = argparse.ArgumentParser(description="PA DGS Construction Contracts Scraper")
    parser.add_argument("--preview", action="store_true", help="Preview results without writing to sheet")
    parser.add_argument("--append", action="store_true", help="Append to existing sheet data instead of replacing")
    parser.add_argument("--min-amount", type=int, default=MIN_AMOUNT, help=f"Minimum award amount (default: ${MIN_AMOUNT:,})")
    args = parser.parse_args()

    MIN_AMOUNT = args.min_amount

    results = scrape_awards()

    if not results:
        print(f"\nNo contracts found >= ${MIN_AMOUNT:,}")
        print("Try lowering --min-amount if this is unexpected.")
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
