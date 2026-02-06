"""
Austin, TX Construction Contracts Scraper (Phase 1)
Scrapes awarded construction contracts from Austin Finance Online.
Filters by NAICS-mapped commodity types, date, and amount.
Enriches with vendor contact info from vendor profile pages.
CRITICAL: Only includes contracts where Amount Expended > $0 (actually awarded/paid).
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
BASE_URL = "https://financeonline.austintexas.gov/afo/contract_catalog"
LIST_URL = f"{BASE_URL}/OCCShowCat.cfm?cat=120"  # Construction category
MIN_AMOUNT = 50_000
MIN_DATE = datetime(2025, 12, 1)
REQUEST_DELAY = 0.5  # seconds between page requests

# Google Sheets config
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

# --- NAICS keyword mapping ---
NAICS_FILTERS = [
    ("238210", "Electrical", [
        "electrical", "electric", "lighting", "wiring",
    ]),
    ("236220", "Commercial Building", [
        "building construction", "commercial", "institutional",
        "fire station", "ems station", "library", "convention center",
        "facility", "facilities", "renovation", "remodel", "tenant improvement",
        "general construction", "construction services, general",
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
        "User-Agent": "AustinContractScraper/1.0",
        "Accept": "text/html",
    })
    with urlopen(req, context=SSL_CTX) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parse_list_page():
    """Parse the construction category list page for contract links."""
    print(f"Fetching contract list from: {LIST_URL}")
    page = fetch_page(LIST_URL)

    link_re = re.compile(
        r'href="(OCCViewMA\.cfm\?cd=(\w+)&dd=(\w+)&id=([^"&]+)[^"]*)"'
        r'\s*>\s*([^<]+)</a>\s*</strong>\s*<br\s*/?>\s*<small>\s*([^<]+)</small>',
        re.IGNORECASE,
    )
    contracts = []
    for m in link_re.finditer(page):
        rel_url, cd, dd, cid, link_text, description = m.groups()
        contracts.append({
            "detail_url": f"{BASE_URL}/{rel_url}",
            "cd": cd.strip(),
            "dd": dd.strip(),
            "contract_id": cid.strip(),
            "link_text": html.unescape(link_text.strip()),
            "list_description": html.unescape(description.strip()),
        })

    print(f"Found {len(contracts)} construction contracts on list page")
    return contracts


def parse_detail_page(url):
    """Scrape a single contract detail page for vendor, commodity, and vendor code."""
    page = fetch_page(url)

    def extract(pattern, default=""):
        m = re.search(pattern, page, re.IGNORECASE | re.DOTALL)
        return html.unescape(m.group(1).strip()) if m else default

    # Vendor name: "Name (V00000nnn)" or <strong>Name</strong> in Goods section
    vendor = extract(r"([A-Z][^<\n]+?)\s*\(V\d+\)")
    if not vendor:
        vendor = extract(r"Goods and Services.*?<strong>([^<]+)</strong>")

    # Vendor code: from "OCCViewVend.cfm?vc=XXXXX" button
    vendor_code = extract(r"OCCViewVend\.cfm\?vc=([^&'\"]+)")

    # Begin Date
    begin_date_str = extract(
        r"Begin\s+Date:</th>\s*<td>\s*(\d{1,2}/\d{1,2}/\d{4})\s*</td>"
    )

    # Authorized Amount
    amount_str = extract(
        r"Authorized\s+Amount:</th>\s*<td>\s*\$([\d,]+(?:\.\d{2})?)\s*</td>"
    )

    # Amount Expended
    amount_expended_str = extract(
        r"Amount\s+Expended:</th>\s*<td>\s*\$([\d,]+(?:\.\d{2})?)\s*</td>"
    )

    # Commodity description
    commodity_desc = extract(
        r"text-align:left[^>]*>\s*(.+?)\s*</td>\s*<td[^>]*>\d{5}</td>",
    )
    commodity_desc = re.sub(r"<br\s*/?>", ", ", commodity_desc)
    commodity_desc = re.sub(r"\s+", " ", commodity_desc).strip()

    commodity_code = extract(r"text-align:center[^>]*>(\d{5})</td>")

    return {
        "vendor": vendor,
        "vendor_code": vendor_code,
        "commodity_desc": commodity_desc,
        "commodity_code": commodity_code,
        "begin_date_str": begin_date_str,
        "amount_str": amount_str,
        "amount_expended_str": amount_expended_str,
    }


def parse_vendor_page(vendor_code):
    """Scrape vendor profile page for contact info."""
    url = f"{BASE_URL}/OCCViewVend.cfm?vc={vendor_code}&cat=120"
    page = fetch_page(url)

    def extract(pattern, default=""):
        m = re.search(pattern, page, re.IGNORECASE | re.DOTALL)
        return html.unescape(m.group(1).strip()) if m else default

    # Extract all capitalize spans between "Information" and "fa-phone"
    info_block = extract(r"Information</h4>(.*?)fa-phone")
    cap_spans = re.findall(
        r'capitalize">([^<]+)</span>', info_block, re.IGNORECASE
    )
    # First span = contact name, remaining = address parts (skip county)
    contact_name = cap_spans[0].strip() if len(cap_spans) > 0 else ""
    addr_parts = []
    for part in cap_spans[1:]:
        part = part.strip()
        # Skip county names (single word after "County:" context)
        if len(part.split()) <= 1 and not re.search(r'\d', part):
            continue
        addr_parts.append(part.title())
    full_address = ", ".join(addr_parts)

    # Phone: "(xxx) xxx-xxxx (Phone)"
    phone = extract(r"fa-phone\"></i>\s*([^<\n]+?)\s*\(Phone\)")

    # Email: after fa-at icon
    email = extract(r"fa-at\"></i>\s*([^\s<]+@[^\s<]+)")

    # Website: after fa-globe icon
    website = extract(r"fa-globe[^>]*></i>\s*(?:<a[^>]*>)?\s*([^\s<]+)")
    if website and website.upper() == "N/A":
        website = ""

    return {
        "contact_name": contact_name.title() if contact_name else "",
        "address": full_address,
        "phone": phone,
        "email": email,
        "website": website,
    }


def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%m/%d/%Y")
    except ValueError:
        return None


def parse_amount(amount_str):
    if not amount_str:
        return 0.0
    try:
        return float(amount_str.replace(",", ""))
    except ValueError:
        return 0.0


def match_naics(text):
    lower = text.lower()
    for code, label, keywords in NAICS_FILTERS:
        for kw in keywords:
            if kw in lower:
                return code, label
    return None, None


def enhance_description(contract_name, commodity_desc):
    name = contract_name.strip().title()
    commodity = commodity_desc.strip()
    if commodity and commodity.upper() != contract_name.upper():
        return f"{name} — {commodity}"
    return name


def scrape_all():
    """Main scrape pipeline: list -> detail -> vendor profiles -> filter -> CSV."""
    contracts = parse_list_page()
    if not contracts:
        print("No contracts found on list page.")
        sys.exit(1)

    results = []
    seen = set()
    vendor_cache = {}  # vendor_code -> vendor info dict
    skipped_naics = 0
    skipped_date = 0
    skipped_amount = 0
    skipped_not_expended = 0
    errors = 0

    for i, c in enumerate(contracts, 1):
        quick_text = f"{c['list_description']} {c['link_text']}"
        naics_code, naics_label = match_naics(quick_text)

        if not naics_code:
            skipped_naics += 1
            continue

        print(f"  [{i}/{len(contracts)}] Fetching: {c['list_description'][:60]}...")
        try:
            detail = parse_detail_page(c["detail_url"])
        except Exception as e:
            print(f"    ERROR fetching detail: {e}")
            errors += 1
            continue

        full_text = f"{c['list_description']} {detail['commodity_desc']}"
        naics_code, naics_label = match_naics(full_text)
        if not naics_code:
            skipped_naics += 1
            continue

        begin_date = parse_date(detail["begin_date_str"])
        if not begin_date or begin_date < MIN_DATE:
            skipped_date += 1
            continue

        amount = parse_amount(detail["amount_str"])
        if amount < MIN_AMOUNT:
            skipped_amount += 1
            continue

        # CRITICAL: Only include contracts with actual expenditure (truly awarded)
        amount_expended = parse_amount(detail["amount_expended_str"])
        if amount_expended <= 0:
            skipped_not_expended += 1
            continue

        vendor_name = detail["vendor"] or "Unknown"
        dedup_key = (c["contract_id"], vendor_name)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        # Fetch vendor contact info (cached per vendor_code)
        vendor_info = {"contact_name": "", "address": "", "phone": "", "email": "", "website": ""}
        vc = detail.get("vendor_code", "")
        if vc:
            if vc not in vendor_cache:
                print(f"    -> Fetching vendor profile: {vc}")
                try:
                    vendor_cache[vc] = parse_vendor_page(vc)
                    time.sleep(REQUEST_DELAY)
                except Exception as e:
                    print(f"    ERROR fetching vendor: {e}")
                    vendor_cache[vc] = vendor_info
            vendor_info = vendor_cache[vc]

        results.append({
            "company_name": vendor_name,
            "contract_name": c["list_description"],
            "award_amount": amount,
            "amount_expended": amount_expended,
            "begin_date": begin_date.strftime("%Y-%m-%d"),
            "award_link": c["detail_url"],
            "description": enhance_description(
                c["list_description"], detail["commodity_desc"]
            ),
            "commodity_type": f"{naics_label} (NAICS {naics_code})",
            "contact_name": vendor_info["contact_name"],
            "address": vendor_info["address"],
            "phone": vendor_info["phone"],
            "email": vendor_info["email"],
            "website": vendor_info["website"],
            "city": "Austin",
        })

        time.sleep(REQUEST_DELAY)

    print(f"\n--- Summary ---")
    print(f"Total on list page:  {len(contracts)}")
    print(f"Skipped (NAICS):     {skipped_naics}")
    print(f"Skipped (date):      {skipped_date}")
    print(f"Skipped (amount):    {skipped_amount}")
    print(f"Skipped ($0 expended): {skipped_not_expended}")
    print(f"Errors:              {errors}")
    print(f"Matched:             {len(results)}")
    print(f"Unique vendors:      {len(vendor_cache)}")

    return results


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


def write_to_google_sheets(results):
    """Write results directly to Google Sheets, replacing existing data."""
    service = get_sheets_service()
    sheet = service.spreadsheets()

    # Build rows: header + data
    rows = [SHEET_HEADERS]
    for r in results:
        rows.append([r.get(f, "") for f in SHEET_FIELDS])

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

    # Clear existing data in the sheet
    try:
        clear_range = f"'{SHEET_NAME}'!A1:Z1000"
        sheet.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=clear_range,
            body={}
        ).execute()
    except Exception:
        pass  # Sheet might be empty

    # Write new data starting at A1
    write_range = f"'{SHEET_NAME}'!A1"
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=write_range,
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

    print(f"Wrote {len(results)} rows to Google Sheet: {SHEET_NAME}")


def main():
    results = scrape_all()
    if not results:
        print("\nNo contracts matched all filters.")
        print("Try adjusting MIN_DATE or NAICS_FILTERS if this is unexpected.")
        sys.exit(0)
    write_to_google_sheets(results)
    print(f"\nView results at: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
