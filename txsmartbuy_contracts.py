"""
TxSmartBuy TXMAS Contracts Scraper
Scrapes TXMAS contracts from Texas SmartBuy (Page 1 only).
Filters by date (2026+) and NAICS construction codes.
Fetches contractor details from each contract page.
Exports directly to Google Sheets.
"""

import html
import re
import ssl
import sys
import time
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
BASE_URL = "https://www.txsmartbuy.gov"
LIST_URL = f"{BASE_URL}/browsecontracts?filterBy=TXMAS&page=1"
MIN_START_DATE = datetime(2026, 1, 1)  # Contracts starting Jan 1, 2026+
MIN_END_DATE = datetime(2026, 1, 1)    # OR contracts still active in 2026
REQUEST_DELAY = 0.5  # seconds between requests

# Google Sheets config
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "TxSmartBuy Construction Contracts"

# --- NAICS keyword mapping (same as Austin scraper) ---
NAICS_FILTERS = [
    ("238210", "Electrical", [
        "electrical", "electric", "lighting", "wiring",
    ]),
    ("236220", "Commercial Building", [
        "building construction", "commercial", "institutional",
        "fire station", "ems station", "library", "convention center",
        "facility", "facilities", "renovation", "remodel", "tenant improvement",
        "general construction", "construction services",
    ]),
    ("237310", "Highway/Street/Bridge", [
        "highway", "street", "road", "bridge", "intersection",
        "sidewalk", "pavement", "asphalt", "roundabout", "traffic signal",
        "parking lot",
    ]),
    ("238220", "Plumbing/HVAC", [
        "plumbing", "hvac", "mechanical", "heating", "cooling",
        "air conditioning", "chilled water", "boiler",
    ]),
    ("238120", "Structural Steel", [
        "structural steel", "steel erection", "steel fabricat",
        "iron work", "metal building",
    ]),
]


def fetch_page(url):
    """Fetch a URL and return decoded HTML."""
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    with urlopen(req, context=SSL_CTX, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parse_date(date_str):
    """Parse date string in M/D/YYYY format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except ValueError:
        try:
            return datetime.strptime(date_str.strip(), "%m/%d/%y")
        except ValueError:
            return None


def match_naics(text):
    """Match text against NAICS keywords."""
    lower = text.lower()
    for code, label, keywords in NAICS_FILTERS:
        for kw in keywords:
            if kw in lower:
                return code, label
    return None, None


def parse_contracts_list():
    """Parse the contracts list page."""
    print(f"Fetching contracts from: {LIST_URL}")
    page = fetch_page(LIST_URL)

    contracts = []

    # Parse the contract rows from the HTML
    row_pattern = re.compile(
        r'<div class="browse-contract-search-result-line"[^>]*>(.*?)</div>\s*(?=<div class="browse-contract-search-result-line"|<div class="browse-contract-search-list-paginator")',
        re.DOTALL | re.IGNORECASE
    )

    rows = row_pattern.findall(page)

    def extract_cell(row_html, header_text):
        """Extract cell value after a specific header."""
        pattern = rf'{re.escape(header_text)}:</span>\s*<span[^>]*>([^<]*(?:<a[^>]*>([^<]*)</a>)?[^<]*)</span>'
        match = re.search(pattern, row_html, re.IGNORECASE | re.DOTALL)
        if match:
            text = match.group(2) if match.group(2) else match.group(1)
            text = re.sub(r'<[^>]+>', '', text)
            return html.unescape(text.strip())
        return ""

    def extract_detail_url(row_html):
        """Extract the contract detail page URL."""
        match = re.search(r'href="(/browsecontracts/\d+)"', row_html)
        if match:
            return BASE_URL + match.group(1)
        return ""

    for row_html in rows:
        nigp_match = re.search(r'NIGP\(s\):</span>\s*<span[^>]*>([^<]+)</span>', row_html, re.IGNORECASE)
        nigp_codes = html.unescape(nigp_match.group(1).strip()) if nigp_match else ""

        contract = {
            "contract_number": extract_cell(row_html, "Contract #"),
            "description": extract_cell(row_html, "Description"),
            "contract_type": extract_cell(row_html, "Contract Type"),
            "contract_category": extract_cell(row_html, "Category"),
            "start_date": extract_cell(row_html, "Start Date"),
            "end_date": extract_cell(row_html, "End Date"),
            "nigp_codes": nigp_codes,
            "detail_url": extract_detail_url(row_html),
        }

        if contract["contract_number"]:
            contracts.append(contract)

    print(f"Found {len(contracts)} contracts on page")
    return contracts


def parse_contract_detail(url):
    """Parse a contract detail page for contractor information."""
    page = fetch_page(url)

    contractors = []

    # Find contractor blocks - they contain VID numbers
    # Pattern: Company name followed by VID
    contractor_pattern = re.compile(
        r'<h\d[^>]*>([^<]+)</h\d>\s*(?:<[^>]+>\s*)*VID:\s*(\d+)',
        re.IGNORECASE | re.DOTALL
    )

    for match in contractor_pattern.finditer(page):
        name = html.unescape(match.group(1).strip())
        vid = match.group(2).strip()

        # Extract contact info for this contractor
        # Look for email and phone near the VID
        block_start = match.start()
        block_end = min(match.end() + 2000, len(page))
        block = page[block_start:block_end]

        email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', block)
        phone_match = re.search(r'(\d{3}[-.]?\d{3}[-.]?\d{4})', block)

        contractors.append({
            "name": name,
            "vid": vid,
            "email": email_match.group(1) if email_match else "",
            "phone": phone_match.group(1) if phone_match else "",
        })

    # Also try alternative pattern for contractor names
    if not contractors:
        alt_pattern = re.compile(
            r'class="[^"]*contractor[^"]*"[^>]*>([^<]+)<',
            re.IGNORECASE
        )
        for match in alt_pattern.finditer(page):
            name = html.unescape(match.group(1).strip())
            if name and len(name) > 2:
                contractors.append({
                    "name": name,
                    "vid": "",
                    "email": "",
                    "phone": "",
                })

    # Extract any award amounts (if present)
    amount_match = re.search(r'\$\s*([\d,]+(?:\.\d{2})?)', page)
    award_amount = amount_match.group(1) if amount_match else ""

    return {
        "contractors": contractors,
        "award_amount": award_amount,
    }


def scrape_all():
    """Main scrape pipeline with filtering."""
    contracts = parse_contracts_list()
    if not contracts:
        print("No contracts found on list page.")
        return []

    results = []
    skipped_date = 0
    skipped_naics = 0
    errors = 0

    for i, c in enumerate(contracts, 1):
        # Filter by date - contract must be active in 2026 (started in 2026 OR end date >= 2026)
        start_date = parse_date(c["start_date"])
        end_date = parse_date(c["end_date"])

        # Contract qualifies if:
        # - Start date is in 2026+ OR
        # - End date is in 2026+ (meaning contract is still active in 2026)
        start_qualifies = start_date and start_date >= MIN_START_DATE
        end_qualifies = end_date and end_date >= MIN_END_DATE

        if not (start_qualifies or end_qualifies):
            skipped_date += 1
            continue

        # Check NAICS match on description
        naics_code, naics_label = match_naics(c["description"])
        if not naics_code:
            skipped_naics += 1
            continue

        print(f"  [{i}/{len(contracts)}] Fetching: {c['contract_number']} - {c['description'][:50]}...")

        # Fetch detail page for contractor info
        detail_info = {"contractors": [], "award_amount": ""}
        if c["detail_url"]:
            try:
                detail_info = parse_contract_detail(c["detail_url"])
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                print(f"    ERROR fetching detail: {e}")
                errors += 1

        # Create a result row for each contractor (or one row if no contractors found)
        contractor_list = detail_info["contractors"] if detail_info["contractors"] else [{"name": "", "vid": "", "email": "", "phone": ""}]

        for contractor in contractor_list:
            results.append({
                "contract_number": c["contract_number"],
                "description": c["description"],
                "contract_type": c["contract_type"],
                "contract_category": c["contract_category"],
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": c["end_date"],
                "nigp_codes": c["nigp_codes"],
                "naics_code": naics_code,
                "naics_category": naics_label,
                "contractor_name": contractor["name"],
                "contractor_vid": contractor["vid"],
                "contractor_email": contractor["email"],
                "contractor_phone": contractor["phone"],
                "award_amount": detail_info["award_amount"],
                "detail_url": c["detail_url"],
            })

    print(f"\n--- Summary ---")
    print(f"Total on list page:  {len(contracts)}")
    print(f"Skipped (not active in 2026): {skipped_date}")
    print(f"Skipped (no NAICS match): {skipped_naics}")
    print(f"Errors:              {errors}")
    print(f"Matched:             {len(results)}")

    return results


SHEET_FIELDS = [
    "contract_number", "description", "naics_code", "naics_category",
    "contractor_name", "contractor_email", "contractor_phone",
    "award_amount", "start_date", "end_date", "nigp_codes",
    "contract_type", "contract_category", "detail_url",
]
SHEET_HEADERS = [
    "Contract Number", "Description", "NAICS Code", "NAICS Category",
    "Contractor Name", "Email", "Phone",
    "Award Amount", "Start Date", "End Date", "NIGP Codes",
    "Contract Type", "Contract Category", "Contract URL",
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
            range=f"'{SHEET_NAME}'!A:Z"
        ).execute()
        return result.get("values", [])
    except Exception:
        return []


def write_to_google_sheets(results, append=True):
    """Write results to Google Sheets. If append=True, add to existing data."""
    service = get_sheets_service()
    sheet = service.spreadsheets()

    spreadsheet = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id = None
    for s in spreadsheet.get("sheets", []):
        if s["properties"]["title"] == SHEET_NAME:
            sheet_id = s["properties"]["sheetId"]
            break

    if sheet_id is None:
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
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{SHEET_NAME}'!A1",
                valueInputOption="RAW",
                body={"values": [SHEET_HEADERS]}
            ).execute()
            start_row = 2

        rows = [[r.get(f, "") for f in SHEET_FIELDS] for r in results]
        if rows:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{SHEET_NAME}'!A{start_row}",
                valueInputOption="RAW",
                body={"values": rows}
            ).execute()
    else:
        rows = [SHEET_HEADERS] + [[r.get(f, "") for f in SHEET_FIELDS] for r in results]
        try:
            sheet.values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{SHEET_NAME}'!A1:Z1000",
                body={}
            ).execute()
        except Exception:
            pass
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A1",
            valueInputOption="RAW",
            body={"values": rows}
        ).execute()

    print(f"Wrote {len(results)} rows to Google Sheet: {SHEET_NAME}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="TxSmartBuy Construction Contracts Scraper")
    parser.add_argument("--append", action="store_true", default=True, help="Append to existing data (default)")
    parser.add_argument("--replace", action="store_true", help="Replace existing data")
    args = parser.parse_args()

    results = scrape_all()
    if not results:
        print("\nNo contracts matched filters (date >= 2026-01-01 + NAICS construction codes).")
        print("This is expected if TxSmartBuy doesn't have construction contracts.")
        sys.exit(0)

    write_to_google_sheets(results, append=not args.replace)
    print(f"\nView results at: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
