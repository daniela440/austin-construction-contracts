"""
Pennsylvania PennDOT Construction Contracts Scraper
Pulls awarded highway/bridge construction contracts from PennDOT letting results.
Parses PDF bid results from Associated Pennsylvania Constructors.
Exports directly to Google Sheets.

Requires: pip install pypdf2
"""

import io
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
APC_LETTING_URL = "https://www.paconstructors.org/penndot-letting-information/"
PDF_BASE_URL = "https://www.paconstructors.org/01LETDOCUMENTS/PENNDOT/LETTING_RESULTS"
MIN_AMOUNT = 1_000_000  # $1M minimum

# Google Sheets config
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"


def fetch_page(url):
    """Fetch a URL and return decoded HTML."""
    req = Request(url, headers={
        "User-Agent": "PennDOTScraper/1.0",
        "Accept": "text/html,application/pdf",
    })
    with urlopen(req, context=SSL_CTX, timeout=120) as resp:
        return resp.read()


def fetch_pdf(url):
    """Fetch a PDF and return bytes."""
    req = Request(url, headers={
        "User-Agent": "PennDOTScraper/1.0",
        "Accept": "application/pdf",
    })
    with urlopen(req, context=SSL_CTX, timeout=120) as resp:
        return resp.read()


def get_letting_pdf_urls():
    """Get list of letting result PDF URLs from APC site."""
    page = fetch_page(APC_LETTING_URL).decode("utf-8", errors="replace")

    # Find PDF links for letting results
    # Pattern: /01LETDOCUMENTS/PENNDOT/LETTING_RESULTS/2026/012926.pdf
    pdf_pattern = re.compile(
        r'href=["\']([^"\']*LETTING_RESULTS/(\d{4})/(\d{6})\.pdf)["\']',
        re.IGNORECASE
    )

    pdfs = []
    for m in pdf_pattern.finditer(page):
        path, year, date_code = m.groups()
        # Build full URL
        if path.startswith("http"):
            url = path
        elif path.startswith("/"):
            url = f"https://www.paconstructors.org{path}"
        else:
            url = f"https://www.paconstructors.org/{path}"

        # Parse date from code (MMDDYY format)
        try:
            letting_date = datetime.strptime(date_code, "%m%d%y")
        except ValueError:
            letting_date = None

        pdfs.append({
            "url": url,
            "year": year,
            "date_code": date_code,
            "letting_date": letting_date,
        })

    # Sort by date, most recent first
    pdfs.sort(key=lambda x: x["date_code"], reverse=True)
    return pdfs


def extract_text_from_pdf(pdf_bytes):
    """Extract text from PDF using PyPDF2."""
    try:
        from PyPDF2 import PdfReader
    except ImportError:
        print("ERROR: PyPDF2 not installed. Run: pip install pypdf2")
        sys.exit(1)

    reader = PdfReader(io.BytesIO(pdf_bytes))
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    return text


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


def parse_letting_pdf(pdf_text, letting_date):
    """Parse letting results from PDF text.

    PennDOT letting PDFs typically contain:
    - Project ECMS number
    - County
    - Work description
    - Low bidder name
    - Low bid amount
    """
    results = []

    # Split into contract sections
    # Contracts often start with ECMS number like "ECMS:12345" or "Contract: 12345"
    # or with county names like "ALLEGHENY COUNTY"

    # Pattern for contract blocks - varies by PDF format
    # Try to find patterns like:
    # "ECMS: 12345 ... County: ALLEGHENY ... Low Bidder: CONTRACTOR NAME ... $1,234,567.89"

    # Pattern 1: Structured format with labeled fields
    contract_pattern = re.compile(
        r'(?:ECMS[:#\s]*|Contract[:#\s]*)(\d+)[^\n]*'  # ECMS/Contract number
        r'.*?(?:County[:#\s]*)?([A-Z]{3,}(?:\s+[A-Z]+)*)\s+COUNTY.*?'  # County
        r'(?:Low\s+Bidder[:#\s]*|Awarded\s+to[:#\s]*)([^$\n]+?)'  # Contractor
        r'\s*\$([0-9,]+(?:\.\d{2})?)',  # Amount
        re.IGNORECASE | re.DOTALL
    )

    # Pattern 2: Simpler - look for contractor + amount pairs
    simple_pattern = re.compile(
        r'([A-Z][A-Za-z\s&,\.]+(?:Inc|LLC|Corp|Company|Co|Ltd|Construction|Contracting|Paving|Excavating)\.?)'
        r'\s*[-–]?\s*'
        r'\$([0-9,]+(?:\.\d{2})?)',
        re.IGNORECASE
    )

    # Pattern 3: ECMS number followed by data on subsequent lines
    ecms_pattern = re.compile(r'ECMS[:#\s]*(\d+)', re.IGNORECASE)
    amount_pattern = re.compile(r'\$([0-9,]+(?:\.\d{2})?)')

    # Try pattern 1 first
    for m in contract_pattern.finditer(pdf_text):
        ecms, county, contractor, amount_str = m.groups()
        amount = parse_amount(amount_str)

        if amount >= MIN_AMOUNT:
            results.append({
                "ecms": ecms.strip(),
                "county": county.strip().title(),
                "contractor": contractor.strip(),
                "amount": amount,
                "letting_date": letting_date,
            })

    # If no structured matches, try simple pattern
    if not results:
        for m in simple_pattern.finditer(pdf_text):
            contractor, amount_str = m.groups()
            amount = parse_amount(amount_str)

            # Skip if too short (likely false positive)
            if len(contractor.strip()) < 5:
                continue

            if amount >= MIN_AMOUNT:
                results.append({
                    "ecms": "",
                    "county": "",
                    "contractor": contractor.strip(),
                    "amount": amount,
                    "letting_date": letting_date,
                })

    return results


def categorize_penndot_project(description=""):
    """Categorize PennDOT project - all are highway/bridge by default."""
    desc_lower = description.lower() if description else ""

    if "bridge" in desc_lower:
        return "237310", "Highway/Bridge"
    elif "signal" in desc_lower or "traffic" in desc_lower:
        return "237310", "Traffic Signals"
    elif "paving" in desc_lower or "asphalt" in desc_lower:
        return "237310", "Highway Paving"
    else:
        return "237310", "Highway/Street/Bridge"


def scrape_all():
    """Main scrape pipeline: get PDF list -> parse PDFs -> return results."""
    print(f"Fetching letting information from: {APC_LETTING_URL}")

    pdfs = get_letting_pdf_urls()
    print(f"Found {len(pdfs)} letting result PDFs")

    if not pdfs:
        print("No PDFs found. Check if the page structure has changed.")
        return []

    # Only process 2025-2026 PDFs
    recent_pdfs = [p for p in pdfs if p["year"] in ("2025", "2026")]
    print(f"Processing {len(recent_pdfs)} PDFs from 2025-2026")

    all_results = []
    seen = set()

    for pdf_info in recent_pdfs[:5]:  # Limit to 5 most recent
        url = pdf_info["url"]
        letting_date = pdf_info["letting_date"]

        print(f"\nFetching: {url}")
        try:
            pdf_bytes = fetch_pdf(url)
            pdf_text = extract_text_from_pdf(pdf_bytes)

            awards = parse_letting_pdf(pdf_text, letting_date)
            print(f"  Found {len(awards)} awards >= ${MIN_AMOUNT:,}")

            for award in awards:
                # Deduplicate
                dedup_key = (award["contractor"], award["amount"])
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                naics_code, naics_label = categorize_penndot_project()

                all_results.append({
                    "company_name": award["contractor"],
                    "contract_name": f"ECMS {award['ecms']}" if award["ecms"] else "PennDOT Contract",
                    "award_amount": award["amount"],
                    "amount_expended": "",
                    "begin_date": award["letting_date"].strftime("%Y-%m-%d") if award["letting_date"] else "",
                    "award_link": url,
                    "description": f"PennDOT Highway/Bridge - {award['county']} County" if award["county"] else "PennDOT Highway/Bridge Construction",
                    "commodity_type": f"{naics_label} (NAICS {naics_code})",
                    "contact_name": "",
                    "address": "",
                    "phone": "",
                    "email": "",
                    "website": "",
                    "city": "Pennsylvania",
                })

        except Exception as e:
            print(f"  ERROR: {e}")
            continue

    print(f"\nTotal awards found: {len(all_results)}")
    return all_results


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
    "Contract Name", "Award Amount", "Amount Expended", "Letting Date",
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
    """Write results to Google Sheets."""
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
        print(f"   Description: {r['description']}")
        print()


def main():
    global MIN_AMOUNT

    import argparse
    parser = argparse.ArgumentParser(description="PA PennDOT Construction Contracts Scraper")
    parser.add_argument("--preview", action="store_true", help="Preview results without writing to sheet")
    parser.add_argument("--append", action="store_true", help="Append to existing sheet data instead of replacing")
    parser.add_argument("--min-amount", type=int, default=MIN_AMOUNT, help=f"Minimum award amount (default: ${MIN_AMOUNT:,})")
    args = parser.parse_args()

    MIN_AMOUNT = args.min_amount

    results = scrape_all()

    if not results:
        print(f"\nNo contracts found >= ${MIN_AMOUNT:,}")
        print("This may be due to PDF parsing issues or no recent lettings.")
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
