"""
Tennessee TDOT (Department of Transportation) Contracts Scraper
Downloads yearly award PDFs from TDOT and parses contract data:
  - Contractor name, County, Total Bid, Project ID, Description
PDF URL pattern: https://www.tn.gov/content/dam/tn/tdot/construction/previous_lettings/Const_{YEAR}_Awards.pdf
Exports directly to Google Sheets in append mode.
"""

import re
import ssl
import sys
import tempfile
from datetime import datetime
from urllib.request import urlopen, Request

import fitz  # PyMuPDF

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
PDF_URL_TEMPLATE = "https://www.tn.gov/content/dam/tn/tdot/construction/previous_lettings/Const_{year}_Awards.pdf"
SCRAPE_YEAR = 2024  # Most recent available

# Google Sheets config (same as other scrapers)
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

# NAICS keyword mapping (same categories as other scrapers)
NAICS_FILTERS = [
    ("238210", "Electrical", [
        "electrical", "electric", "lighting", "signal", "its ", "intelligent transportation",
    ]),
    ("236220", "Commercial Building", [
        "building", "maintenance building", "rest area", "welcome center",
        "facility", "facilities", "renovation",
    ]),
    ("237310", "Highway/Street/Bridge", [
        "highway", "street", "road", "bridge", "resurfacing", "paving",
        "pavement", "asphalt", "concrete", "grading", "drainage",
        "interchange", "widening", "overlay", "guardrail", "slide",
        "retaining wall", "sidewalk", "intersection", "microsurfacing",
        "milling", "rumble strip", "crack seal",
    ]),
    ("238220", "Plumbing/HVAC", [
        "plumbing", "hvac", "mechanical", "water line", "sewer",
    ]),
    ("238120", "Structural Steel", [
        "structural steel", "steel erection", "steel bridge",
        "iron work", "metal",
    ]),
]

KEEP_UPPER = {"LLC", "LP", "LLP", "PLLC", "LTD", "JV", "II", "III", "IV", "PC", "PA", "INC", "CO", "DBA"}


def title_case(name):
    """Convert 'JONES BROS. CONTRACTORS, LLC' to 'Jones Bros. Contractors, LLC'."""
    if not name:
        return ""
    words = name.title().split()
    return " ".join(w.upper() if w.upper().rstrip(".,") in KEEP_UPPER else w for w in words)


def match_naics(text):
    """Match description text to a NAICS category."""
    lower = text.lower()
    for code, label, keywords in NAICS_FILTERS:
        for kw in keywords:
            if kw in lower:
                return code, label
    return "237310", "Highway/Street/Bridge"  # Default for TDOT contracts


# --- PDF Download & Parse ---

def download_pdf(year):
    """Download the TDOT awards PDF for a given year. Returns path to temp file."""
    url = PDF_URL_TEMPLATE.format(year=year)
    print(f"Downloading: {url}")
    req = Request(url, headers={"User-Agent": "TN-TDOT-Scraper/1.0"})
    with urlopen(req, context=SSL_CTX, timeout=120) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        data = resp.read()
    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    tmp.write(data)
    tmp.close()
    print(f"  Downloaded {len(data):,} bytes -> {tmp.name}")
    return tmp.name


def parse_awards_pdf(pdf_path):
    """Parse TDOT awards PDF into a list of contract dicts."""
    doc = fitz.open(pdf_path)
    print(f"  PDF has {len(doc)} pages")

    full_text = ""
    for page in doc:
        full_text += page.get_text() + "\n"
    doc.close()

    contracts = []
    current_date = ""

    # Split into blocks by the 3-digit call numbers preceded by "Estimate\n"
    # Pattern: $amount\n$amount\nEstimate\nCALL_NUM\nCONTRACTOR\n...
    block_pattern = re.compile(
        r'(\$[\d,]+\.\d{2})\s*\n'       # Amount 1 (estimate or bid)
        r'(\$[\d,]+\.\d{2})\s*\n'       # Amount 2
        r'Estimate\s*\n'                 # "Estimate" label
        r'(\d{3})\s*\n'                  # Call number
        r'([A-Z][A-Z &,.\-/\'0-9]+?)\s*\n'  # Contractor name (ALL CAPS)
        r'Project\s*\n'                  # "Project" label
        r'(\w+)\s*\n',                   # Project code
        re.MULTILINE,
    )

    # Find all letting dates
    date_positions = []
    for m in re.finditer(r'(\w+ \d{1,2}, \d{4})\s*\n\s*Contract Awards for Letting Of', full_text):
        date_positions.append((m.start(), m.group(1)))

    for m in block_pattern.finditer(full_text):
        pos = m.start()

        # Determine which letting date this block belongs to
        for dp, dd in reversed(date_positions):
            if dp <= pos:
                current_date = dd
                break

        amount1_str = m.group(1).replace("$", "").replace(",", "")
        amount2_str = m.group(2).replace("$", "").replace(",", "")
        call_num = m.group(3)
        contractor = m.group(4).strip()
        project_code = m.group(5).strip()

        # The larger amount is typically the total bid, smaller is estimate
        # But we want the actual bid amount
        try:
            amt1 = float(amount1_str)
            amt2 = float(amount2_str)
        except ValueError:
            amt1, amt2 = 0, 0

        # In TDOT PDFs, the layout columns are: Estimate (left) | Total Bid (right)
        # Text extraction reads left-to-right, so first = estimate, second = total bid
        # But actually the order varies. Use the larger of the two as the bid amount.
        total_bid = max(amt1, amt2)
        estimate = min(amt1, amt2)

        # Extract description (text after "Contractor\n" starting with "THE ")
        after_match = full_text[m.end():]
        desc = ""
        desc_match = re.search(r'(?:Contractor\s*\n)?(THE [A-Z].*?)(?=\nCounty\b)', after_match, re.DOTALL)
        if desc_match:
            desc = desc_match.group(1).strip()
            desc = re.sub(r'\s+', ' ', desc)

        # Extract county (after "County\nTotal Bid\n")
        county = ""
        county_match = re.search(r'County\s*\n\s*Total Bid\s*\n([^\n]+)', after_match)
        if county_match:
            county = county_match.group(1).strip()
            # Clean up "ETC." suffixes and extra spaces
            county = re.sub(r'\s*,\s*ETC\.?', ' et al.', county, flags=re.IGNORECASE)
            county = re.sub(r'\s{2,}', ' ', county)
            county = county.strip(' ,&')

        contracts.append({
            "letting_date": current_date,
            "call": call_num,
            "contractor": contractor,
            "project_code": project_code,
            "total_bid": total_bid,
            "estimate": estimate,
            "description": desc,
            "county": county,
        })

    return contracts


# --- Transform to sheet format ---

def transform_results(raw_contracts, year=SCRAPE_YEAR):
    """Transform parsed PDF data into the 14-column sheet format."""
    results = []
    seen = set()

    for c in raw_contracts:
        # Deduplicate by contractor + project
        dedup_key = (c["contractor"], c["project_code"])
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        # Parse letting date
        try:
            date_obj = datetime.strptime(c["letting_date"], "%B %d, %Y")
            date_str = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            date_str = c["letting_date"]

        # Classify by NAICS
        naics_code, naics_label = match_naics(c["description"])
        commodity_type = f"{naics_label} (NAICS {naics_code})"

        # Format description
        desc = c["description"]
        if desc.startswith("THE "):
            desc = desc[4:]
        # Title-case the description for readability
        desc = desc.capitalize()
        if len(desc) > 500:
            desc = desc[:497] + "..."

        county = title_case(c["county"]) if c["county"] else ""
        pdf_url = PDF_URL_TEMPLATE.format(year=year)

        results.append({
            "city": f"Tennessee ({county})" if county else "Tennessee",
            "company_name": title_case(c["contractor"]),
            "contact_name": "",
            "phone": "",
            "email": "",
            "address": "",
            "website": "",
            "contract_name": f"TDOT {c['project_code']} (Call {c['call']})",
            "award_amount": c["total_bid"],
            "amount_expended": "",
            "begin_date": date_str,
            "award_link": pdf_url,
            "description": desc,
            "commodity_type": commodity_type,
        })

    return results


# --- Google Sheets ---

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
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def get_existing_data(service):
    sheet = service.spreadsheets()
    try:
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A:N",
        ).execute()
        return result.get("values", [])
    except Exception:
        return []


def write_to_google_sheets(results, append=True):
    service = get_sheets_service()
    sheet = service.spreadsheets()

    spreadsheet = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id = None
    for s in spreadsheet.get("sheets", []):
        if s["properties"]["title"] == SHEET_NAME:
            sheet_id = s["properties"]["sheetId"]
            break

    if sheet_id is None:
        sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]},
        ).execute()
        print(f"Created new sheet: {SHEET_NAME}")

    if append:
        existing = get_existing_data(service)
        start_row = len(existing) + 1

        if not existing:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{SHEET_NAME}'!A1",
                valueInputOption="RAW",
                body={"values": [SHEET_HEADERS]},
            ).execute()
            start_row = 2

        rows = [[r.get(f, "") for f in SHEET_FIELDS] for r in results]
        if rows:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{SHEET_NAME}'!A{start_row}",
                valueInputOption="RAW",
                body={"values": rows},
            ).execute()
    else:
        rows = [SHEET_HEADERS] + [[r.get(f, "") for f in SHEET_FIELDS] for r in results]
        sheet.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A1:Z1000",
            body={},
        ).execute()
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A1",
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()

    print(f"Wrote {len(results)} rows to Google Sheet: {SHEET_NAME}")


def preview_results(results, limit=10):
    print(f"\n--- Preview (first {min(limit, len(results))} results) ---\n")
    for i, r in enumerate(results[:limit], 1):
        print(f"{i}. {r['company_name']}")
        print(f"   {r['contract_name']} | ${r['award_amount']:,.2f}")
        print(f"   {r['city']} | {r['begin_date']}")
        print(f"   {r['description'][:100]}")
        print()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="TN TDOT Construction Contracts Scraper")
    parser.add_argument("--year", type=int, default=SCRAPE_YEAR, help="Award year to scrape (default: 2024)")
    parser.add_argument("--preview", action="store_true", help="Preview results without writing to sheet")
    parser.add_argument("--append", action="store_true", default=True, help="Append to existing data (default)")
    parser.add_argument("--replace", action="store_true", help="Replace existing data")
    args = parser.parse_args()

    year = args.year

    pdf_path = download_pdf(year)
    raw = parse_awards_pdf(pdf_path)
    print(f"\nParsed {len(raw)} contract awards from PDF")

    results = transform_results(raw, year)
    print(f"After dedup/transform: {len(results)} contracts")

    if not results:
        print("No contracts found.")
        sys.exit(0)

    preview_results(results)

    if args.preview:
        print(f"Preview mode — not writing to sheet. Run without --preview to write {len(results)} rows.")
    else:
        write_to_google_sheets(results, append=not args.replace)
        print(f"\nView results at: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
