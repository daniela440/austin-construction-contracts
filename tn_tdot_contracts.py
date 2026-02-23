"""
Tennessee TDOT (Department of Transportation) Contracts Scraper
Scrapes 2026 contract awards from individual TDOT bid letting pages.
Two PDF formats supported:
  - Contract Awards PDF (finalized awards with estimate + bid)
  - Apparent Bid Results PDF (low bidder = winner, used before awards are finalized)
Exports directly to Google Sheets in append mode.
"""

import re
import ssl
import sys
import tempfile
from datetime import datetime
from urllib.error import HTTPError
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
BASE_URL = "https://www.tn.gov/content/dam/tn/tdot/construction/2026_bid_lettings"

# 2026 lettings: (folder_name, date_prefix, letting_date_str)
LETTINGS_2026 = [
    ("january-9,-2026-letting", "20260109", "January 09, 2026"),
    ("february-6,-2026-letting", "20260206", "February 06, 2026"),
]

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

def download_url(url):
    """Download a URL to a temp file. Returns path or None on 404."""
    req = Request(url, headers={"User-Agent": "TN-TDOT-Scraper/1.0"})
    try:
        with urlopen(req, context=SSL_CTX, timeout=120) as resp:
            data = resp.read()
    except HTTPError as e:
        if e.code == 404:
            return None
        raise
    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    tmp.write(data)
    tmp.close()
    return tmp.name


def extract_pdf_text(pdf_path):
    """Extract full text from a PDF file."""
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text() + "\n"
    doc.close()
    return text


def parse_contract_awards(text, letting_date, pdf_url):
    """Parse a Contract Awards PDF (finalized awards with estimate + bid amounts)."""
    contracts = []

    block_pattern = re.compile(
        r'(\$[\d,]+\.\d{2})\s*\n'
        r'(\$[\d,]+\.\d{2})\s*\n'
        r'Estimate\s*\n'
        r'(\d{3})\s*\n'
        r'([A-Z][A-Z &,.\-/\'0-9]+?)\s*\n'
        r'Project\s*\n'
        r'(\w+)\s*\n',
        re.MULTILINE,
    )

    for m in block_pattern.finditer(text):
        amount1_str = m.group(1).replace("$", "").replace(",", "")
        amount2_str = m.group(2).replace("$", "").replace(",", "")
        call_num = m.group(3)
        contractor = m.group(4).strip()
        project_code = m.group(5).strip()

        try:
            amt1 = float(amount1_str)
            amt2 = float(amount2_str)
        except ValueError:
            amt1, amt2 = 0, 0
        total_bid = max(amt1, amt2)

        after_match = text[m.end():]
        desc = ""
        desc_match = re.search(r'(?:Contractor\s*\n)?(THE [A-Z].*?)(?=\nCounty\b)', after_match, re.DOTALL)
        if desc_match:
            desc = re.sub(r'\s+', ' ', desc_match.group(1).strip())

        county = ""
        county_match = re.search(r'County\s*\n\s*Total Bid\s*\n([^\n]+)', after_match)
        if county_match:
            county = county_match.group(1).strip()
            county = re.sub(r'\s*,\s*ETC\.?', ' et al.', county, flags=re.IGNORECASE)
            county = re.sub(r'\s{2,}', ' ', county).strip(' ,&')

        contracts.append({
            "letting_date": letting_date,
            "call": call_num,
            "contractor": contractor,
            "project_code": project_code,
            "total_bid": total_bid,
            "description": desc,
            "county": county,
            "pdf_url": pdf_url,
        })

    return contracts


def parse_apparent_bid_results(text, letting_date, pdf_url):
    """Parse an Apparent Bid Results PDF (first bidder per call = low bidder / winner)."""
    contracts = []

    # Split by "Call\nNNN\n" blocks
    blocks = re.split(r'\nCall\n(\d{3})\n', text)

    for i in range(1, len(blocks), 2):
        call_num = blocks[i]
        content = blocks[i + 1] if i + 1 < len(blocks) else ""

        # Contract/Project code
        contract_match = re.search(r'Contract\n(\w+)\n', content)
        project_code = contract_match.group(1) if contract_match else ""

        # County (appears after "Project\n")
        county_match = re.search(r'Project\n([^\n]+)\n\s*County\n', content)
        county = ""
        if county_match:
            county = county_match.group(1).strip()
            county = re.sub(r'\s*,\s*ETC\.?', ' et al.', county, flags=re.IGNORECASE)
            county = re.sub(r'\s{2,}', ' ', county).strip(' ,&')

        # Description (starts with "THE ")
        desc = ""
        desc_match = re.search(r'County\n(THE [A-Z].*?)(?=\n[A-Z0-9])', content, re.DOTALL)
        if desc_match:
            desc = re.sub(r'\s+', ' ', desc_match.group(1).strip())

        # Bidders listed after "Total Bid\n" — first one is the low bidder (winner)
        bidder_section = content.split("Total Bid\n")[-1] if "Total Bid\n" in content else ""
        lines = [l.strip() for l in bidder_section.split("\n") if l.strip()]

        # Pattern: COMPANY_NAME\n$AMOUNT\n
        contractor = ""
        total_bid = 0.0
        for j, line in enumerate(lines):
            amount_match = re.match(r'\$([\d,]+(?:\.\d{2})?)', line)
            if amount_match and contractor:
                total_bid = float(amount_match.group(1).replace(",", ""))
                break
            elif not amount_match and not re.match(r'Page\b|\d+ of\b', line, re.IGNORECASE):
                contractor = line

        if not contractor:
            continue

        contracts.append({
            "letting_date": letting_date,
            "call": call_num,
            "contractor": contractor,
            "project_code": project_code,
            "total_bid": total_bid,
            "description": desc,
            "county": county,
            "pdf_url": pdf_url,
        })

    return contracts


# --- Transform to sheet format ---

def transform_results(raw_contracts):
    """Transform parsed PDF data into the 14-column sheet format."""
    results = []
    seen = set()

    for c in raw_contracts:
        dedup_key = (c["contractor"], c["project_code"])
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        try:
            date_obj = datetime.strptime(c["letting_date"], "%B %d, %Y")
            date_str = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            date_str = c["letting_date"]

        naics_code, naics_label = match_naics(c["description"])
        commodity_type = f"{naics_label} (NAICS {naics_code})"

        desc = c["description"]
        if desc.startswith("THE "):
            desc = desc[4:]
        desc = desc.capitalize()
        if len(desc) > 500:
            desc = desc[:497] + "..."

        county = title_case(c["county"]) if c["county"] else ""

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
            "award_link": c["pdf_url"],
            "description": desc,
            "commodity_type": commodity_type,
        })

    return results


def scrape_2026_lettings():
    """Download and parse all available 2026 letting PDFs."""
    all_contracts = []

    for folder, prefix, letting_date in LETTINGS_2026:
        print(f"\n--- {letting_date} ---")

        # Try Contract Awards first (finalized), fall back to Apparent Bid Results
        awards_url = f"{BASE_URL}/{folder}/{prefix}_ContractAwards.pdf"
        bid_results_url = f"{BASE_URL}/{folder}/{prefix}_ApparentBidResults.pdf"

        print(f"  Trying Contract Awards: {awards_url}")
        pdf_path = download_url(awards_url)
        if pdf_path:
            text = extract_pdf_text(pdf_path)
            contracts = parse_contract_awards(text, letting_date, awards_url)
            print(f"  Parsed {len(contracts)} awards (finalized)")
            all_contracts.extend(contracts)
            continue

        print(f"  No Contract Awards PDF. Trying Apparent Bid Results...")
        pdf_path = download_url(bid_results_url)
        if pdf_path:
            text = extract_pdf_text(pdf_path)
            contracts = parse_apparent_bid_results(text, letting_date, bid_results_url)
            print(f"  Parsed {len(contracts)} apparent low bids")
            all_contracts.extend(contracts)
        else:
            print(f"  No PDFs available yet for this letting.")

    return all_contracts


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
    parser = argparse.ArgumentParser(description="TN TDOT Construction Contracts Scraper (2026)")
    parser.add_argument("--preview", action="store_true", help="Preview results without writing to sheet")
    parser.add_argument("--append", action="store_true", default=True, help="Append to existing data (default)")
    parser.add_argument("--replace", action="store_true", help="Replace existing data")
    args = parser.parse_args()

    raw = scrape_2026_lettings()
    print(f"\nTotal raw contracts parsed: {len(raw)}")

    results = transform_results(raw)
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
