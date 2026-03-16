"""
New Hampshire DOT – ABC Bid Data Scraper

Fetches awarded contracts from the NHDOT document library API, downloads
each PDF from mm.nh.gov, and extracts:
  - Project / City
  - State Project Number
  - Awarded company name + address
  - Award amount
  - Date bids opened
  - Scope of work
  - Location / District
  - Completion date

Writes to the same Google Sheet as other scrapers (append mode).
Deduplicates by (company_name, contract_name / project number).

API endpoint discovered via browser DevTools:
  GET https://www.dot.nh.gov/content/api/documents
  ?q=@field_document_category|=|6986@field_document_purpose|CONTAINS|{year_id}
  &sort=field_date_filed|desc|ALLOW_NULLS
  &iterate_nodes=true&filter_mode=exclusive&type=document

Year IDs (from the page's <select> options):
  2025 → 11236   2026 → 11741
"""

import gzip
import re
import ssl
import time
import urllib.request
from urllib.request import urlopen, Request

import fitz  # PyMuPDF

try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID       = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME           = "Webscraper Tool for Procurement Sites Two"

# Year IDs to scrape (add more from the page's <select> as needed)
YEAR_IDS = {
    "2026": "11741",
}

API_BASE = "https://www.dot.nh.gov/content/api/documents"
CATEGORY_ID = "6986"  # "NHDOT Bid Info"

SHEET_HEADERS = [
    "City", "Company", "Contact", "Phone", "Email",
    "Address", "Website", "Contract", "Amount", "Expended",
    "Date", "Link", "Description", "Type",
]
SHEET_FIELDS = [
    "city", "company", "contact", "phone", "email",
    "address", "website", "contract", "amount", "expended",
    "date", "link", "description", "type",
]

# Chrome-like headers so mm.nh.gov doesn't 403
PDF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "DNT": "1",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

# ---------------------------------------------------------------------------
# NAICS keyword mapping
# ---------------------------------------------------------------------------
NAICS_FILTERS = [
    ("238210", "Electrical", [
        "electrical", "electric", "lighting", "signal", "its ",
        "intelligent transportation",
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
        "milling", "rumble strip", "crack seal", "culvert", "cmp",
        "guardrail", "median", "ramp", "lane",
    ]),
    ("238220", "Plumbing/HVAC", [
        "plumbing", "hvac", "mechanical", "water line", "sewer",
    ]),
    ("238120", "Structural Steel", [
        "structural steel", "steel erection", "steel bridge",
        "iron work", "metal",
    ]),
]

KEEP_UPPER = {"LLC", "LP", "LLP", "PLLC", "LTD", "JV", "II", "III", "IV",
              "PC", "PA", "INC", "CO", "DBA"}


def match_naics(text):
    lower = text.lower()
    for code, label, kws in NAICS_FILTERS:
        if any(kw in lower for kw in kws):
            return code, label
    return "237310", "Highway/Street/Bridge"  # default for DOT contracts


def title_case(name):
    if not name:
        return ""
    words = name.title().split()
    return " ".join(w.upper() if w.upper().rstrip(".,") in KEEP_UPPER else w
                    for w in words)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def fetch_document_list(year_label, year_id):
    """Return list of dicts from the NHDOT document library API."""
    import json
    from urllib.parse import urlencode

    params = urlencode({
        "q": f"@field_document_category|=|{CATEGORY_ID}@field_document_purpose|CONTAINS|{year_id}",
        "sort": "field_date_filed|desc|ALLOW_NULLS",
        "iterate_nodes": "true",
        "filter_mode": "exclusive",
        "type": "document",
    })
    url = f"{API_BASE}?{params}"
    print(f"  Fetching document list for {year_label}: {url}")

    req = Request(url, headers={
        "User-Agent": PDF_HEADERS["User-Agent"],
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.dot.nh.gov/doing-business-nhdot/bid-nhdot-contracts/advertising-bid-results",
        "DNT": "1",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    })
    with urlopen(req, context=SSL_CTX, timeout=30) as resp:
        raw = resp.read()
        if resp.info().get("Content-Encoding") == "gzip" or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        data = json.loads(raw)

    docs = data.get("data", [])
    print(f"  Found {len(docs)} documents for {year_label}")
    return docs


def extract_pdf_url(doc):
    """Pull the mm.nh.gov PDF URL from the list_content HTML snippet."""
    m = re.search(r"href='(https://mm\.nh\.gov/[^']+\.pdf)'", doc.get("list_content", ""))
    return m.group(1) if m else None


def fetch_pdf_bytes(url):
    """Download a PDF with Chrome-like headers; return bytes or None."""
    req = Request(url, headers=PDF_HEADERS)
    try:
        with urlopen(req, context=SSL_CTX, timeout=30) as resp:
            return resp.read()
    except Exception as e:
        print(f"    WARNING: could not download {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------

def parse_pdf(pdf_bytes, pdf_url):
    """
    Extract contract fields from an NH DOT ABC Bid Data PDF.
    Returns a dict or None if this doesn't look like an awarded contract.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page1 = doc[0].get_text()

    # Must be an ABC Bid Data sheet
    if "ABC Bid Data" not in page1:
        return None

    def grab(pattern, text=page1, flags=re.IGNORECASE):
        m = re.search(pattern, text)
        return m.group(1).strip() if m else ""

    # Build a list of non-empty lines for positional parsing
    nlines = [l.strip() for l in page1.split("\n") if l.strip()]

    # Detect layout by where "ABC Bid Data" appears:
    #   Layout B (new): ABC Bid Data is the FIRST line → CITY, STATE_NUM, FED_NUM follow
    #   Layout A (old): ABC Bid Data appears mid-page → FED_NUM, STATE_NUM, CITY follow
    abc_idx = next((i for i, l in enumerate(nlines) if l == "ABC Bid Data"), None)
    layout_b = abc_idx is not None and abc_idx <= 2

    if layout_b:
        # Header block at top: ABC Bid Data / CITY / STATE_NUM / FED_NUM
        proj_val = nlines[abc_idx + 1] if abc_idx + 1 < len(nlines) else ""
        proj_num = nlines[abc_idx + 2] if abc_idx + 2 < len(nlines) else ""
    else:
        # Header block mid-page: ABC Bid Data / FED_NUM / STATE_NUM / CITY
        if abc_idx is not None:
            proj_num = nlines[abc_idx + 2] if abc_idx + 2 < len(nlines) else ""
            proj_val = nlines[abc_idx + 3] if abc_idx + 3 < len(nlines) else ""
        else:
            proj_num = ""
            proj_val = ""

    contract = f"NH {proj_num} – {proj_val}" if (proj_num and proj_val) else (proj_num or proj_val)

    # --- Company / Address: first non-label lines after "Awarded To:" ---
    awarded_idx = page1.find("Awarded To:")
    if awarded_idx == -1:
        print("    Skipping (no Awarded To found — pending approval?)")
        return None

    after_awarded = page1[awarded_idx + len("Awarded To:"):].lstrip()
    company_lines = []
    for line in after_awarded.split("\n"):
        l = line.strip()
        if not l:
            continue
        if l.endswith(":"):
            continue  # skip label lines
        if l.startswith("$") or l.startswith("Certified") or "Summary of Bidders" in l:
            break
        company_lines.append(l)
        if len(company_lines) >= 3:
            break

    if not company_lines:
        print("    Skipping (could not extract company name)")
        return None

    company = title_case(company_lines[0])
    address = ", ".join(title_case(l) for l in company_lines[1:3] if l)

    # --- Amount: first $X,XXX.XX in page (award amount before summary table) ---
    m_amount = re.search(r"\$(\d{1,3}(?:,\d{3})+(?:\.\d{2})?)", page1)
    amount_str = m_amount.group(1).replace(",", "") if m_amount else ""
    try:
        amount_float = float(amount_str)
    except ValueError:
        amount_float = 0.0

    _MONTHS = r"(January|February|March|April|May|June|July|August|September|October|November|December)"

    # --- Date bids open ---
    # In Layout A the value immediately follows the label.
    # In Layout B, values appear after ALL labels, so we must find a date-formatted line.
    date_label_idx = next((i for i, l in enumerate(nlines) if l == "DATE BIDS OPEN:"), None)
    date_clean = ""
    if date_label_idx is not None:
        for l in nlines[date_label_idx + 1:]:
            if re.match(_MONTHS, l):  # must start with a month name
                date_clean = re.sub(r",?\s*\d+:\d+.*$", "", l).strip()
                break

    # --- Scope of work ---
    scope_label_idx = next((i for i, l in enumerate(nlines) if l == "SCOPE OF WORK:"), None)
    scope = ""
    if scope_label_idx is not None:
        if layout_b:
            # In Layout B, values appear in label order after all labels.
            # Scope value is the line immediately after the DATE BIDS OPEN value.
            if date_label_idx is not None:
                # Find the date line index, then take the next non-month, non-label line
                found_date = False
                scope_parts = []
                for l in nlines[date_label_idx + 1:]:
                    if not found_date:
                        if re.match(_MONTHS, l):
                            found_date = True
                    else:
                        # We've passed the date — collect scope until next month or label
                        if re.match(_MONTHS, l) or l.endswith(":") or l.startswith("$") or l.startswith("Awarded"):
                            break
                        scope_parts.append(l)
            scope = " ".join(scope_parts).strip()
        else:
            # In Layout A, scope text appears BEFORE the labels (at start of page)
            scope_parts = []
            for l in nlines:
                if l == "SCOPE OF WORK:" or l.endswith(":") or l.startswith("$"):
                    break
                scope_parts.append(l)
            scope = " ".join(scope_parts).strip()

    # --- Location ---
    loc_label_idx = next((i for i, l in enumerate(nlines) if l == "LOCATION:"), None)
    location = ""
    if loc_label_idx is not None:
        for l in nlines[loc_label_idx + 1:]:
            if not l.endswith(":"):
                location = l
                break

    # --- NAICS ---
    naics_code, naics_label = match_naics(scope)
    commodity_type = f"{naics_label} (NAICS {naics_code})"

    # City for "City" column = project location (e.g. "CONCORD")
    city = f"NH – {title_case(proj_val)}" if proj_val else "NH – Unknown"

    return {
        "city":        city,
        "company":     company,
        "contact":     "",
        "phone":       "",
        "email":       "",
        "address":     address,
        "website":     "",
        "contract":    contract,
        "amount":      str(int(amount_float)) if amount_float else "",
        "expended":    "",
        "date":        date_clean,
        "link":        pdf_url,
        "description": scope,
        "type":        commodity_type,
    }


# ---------------------------------------------------------------------------
# Google Sheets helpers
# ---------------------------------------------------------------------------

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def get_existing_data(service):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A:N",
        ).execute()
        return result.get("values", [])
    except Exception:
        return []


def write_to_google_sheets(results):
    """Append new results; deduplicate by (company, contract)."""
    service = get_sheets_service()
    sheet   = service.spreadsheets()

    existing = get_existing_data(service)
    start_row = len(existing) + 1

    existing_fps = {
        (row[1].strip().lower(), row[7].strip().lower())
        for row in (existing[1:] if len(existing) > 1 else [])
        if len(row) > 7
    }
    new_results = [
        r for r in results
        if (r["company"].strip().lower(), r["contract"].strip().lower())
        not in existing_fps
    ]
    skipped = len(results) - len(new_results)
    if skipped:
        print(f"  Skipped {skipped} already-existing entries")

    if not existing:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A1",
            valueInputOption="RAW",
            body={"values": [SHEET_HEADERS]},
        ).execute()
        start_row = 2

    rows = [[r.get(f, "") for f in SHEET_FIELDS] for r in new_results]
    if rows:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A{start_row}",
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()
        print(f"  Wrote {len(rows)} new rows to '{SHEET_NAME}'")
    else:
        print("  No new rows to write.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    all_results = []

    for year_label, year_id in YEAR_IDS.items():
        docs = fetch_document_list(year_label, year_id)

        for doc in docs:
            title   = doc.get("title", "")
            pdf_url = extract_pdf_url(doc)
            if not pdf_url:
                print(f"  Skipping (no PDF URL): {title}")
                continue

            print(f"  Processing: {title}")
            pdf_bytes = fetch_pdf_bytes(pdf_url)
            if not pdf_bytes:
                continue

            record = parse_pdf(pdf_bytes, pdf_url)
            if record:
                all_results.append(record)
                print(f"    -> {record['company']} | ${record['amount']} | {record['date']}")

            time.sleep(0.5)  # be polite

    print(f"\nTotal contracts parsed: {len(all_results)}")

    if all_results:
        print("Writing to Google Sheets...")
        write_to_google_sheets(all_results)
        print(f"Done. https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    else:
        print("Nothing to write.")


if __name__ == "__main__":
    main()
