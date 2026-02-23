"""
Alaska DOT&PF Construction Contract Awards Scraper
Fetches awarded contracts from the Alaska DOT public JSON API.
Filters for 2026 award dates and amounts >= $250K.
API endpoint: https://dot.alaska.gov/procurement/awp/api/cas
Exports directly to Google Sheets in append mode.
"""

import json
import ssl
import sys
from urllib.request import urlopen, Request

from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- SSL setup ---
try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE

# --- Config ---
API_URL = "https://dot.alaska.gov/procurement/awp/api/cas"
MIN_AMOUNT = 250_000
AWARD_YEAR = 2026

# Google Sheets config
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

# NAICS keyword mapping
NAICS_FILTERS = [
    ("238210", "Electrical", [
        "electrical", "electric", "lighting", "signal", "flashing", "yellow arrow",
    ]),
    ("236220", "Commercial Building", [
        "building", "renovation", "renovate", "snack bar", "facility",
        "terminal", "office", "generator",
    ]),
    ("237310", "Highway/Street/Bridge", [
        "highway", "road", "bridge", "resurfac", "paving", "pavement",
        "taxiway", "runway", "apron", "culvert", "clearing",
        "intersection", "rehabilitation", "sidewalk", "curb ramp",
    ]),
    ("238220", "Plumbing/HVAC", [
        "plumbing", "hvac", "mechanical", "water", "sewer", "wastewater",
    ]),
    ("238120", "Structural Steel", [
        "structural steel", "steel erection", "steel bridge",
    ]),
]

KEEP_UPPER = {"LLC", "LP", "LLP", "LTD", "JV", "II", "III", "IV", "INC", "CO", "DBA"}


def title_case(name):
    if not name:
        return ""
    words = name.title().split()
    return " ".join(w.upper() if w.upper().rstrip(".,") in KEEP_UPPER else w for w in words)


def match_naics(text):
    lower = text.lower()
    for code, label, keywords in NAICS_FILTERS:
        for kw in keywords:
            if kw in lower:
                return code, label
    return "237310", "Highway/Street/Bridge"


# --- API ---

def fetch_contracts():
    """Fetch all contracts from Alaska DOT API."""
    print(f"Fetching from: {API_URL}")
    req = Request(API_URL, headers={
        "User-Agent": "AK-DOT-Scraper/1.0",
        "Content-Type": "application/json",
    })
    with urlopen(req, context=SSL_CTX, timeout=120) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    records = data.get("value", [])
    print(f"  API returned {len(records)} records")
    return records


def scrape_all():
    """Fetch, filter, and transform contracts."""
    records = fetch_contracts()
    results = []

    for r in records:
        letting = r.get("Letting") or {}
        award_date_raw = letting.get("BLDT12") or ""
        amount = r.get("AwardedAmount")
        vendor = r.get("Vendor")

        # Must have a vendor (awarded) and amount
        if not vendor or not amount:
            continue

        # Award date must be in 2026
        if not award_date_raw or award_date_raw[:4] != str(AWARD_YEAR):
            continue

        # Amount filter
        if amount < MIN_AMOUNT:
            continue

        # Format dates
        award_date = award_date_raw[:10]
        letting_date = (letting.get("LettingDate") or "")[:10]

        # Build description
        desc = r.get("LongDescr", "").strip()
        if len(desc) > 500:
            desc = desc[:497] + "..."

        short_desc = r.get("Description", "")
        community = r.get("Community", "")
        region = r.get("region", "")
        contract_num = r.get("Name", "")
        fed_proj = r.get("FederalProjectNumber", "")
        state_proj = r.get("StateProjectNumber", "")

        # NAICS classification
        full_text = f"{short_desc} {desc}"
        naics_code, naics_label = match_naics(full_text)

        # City field
        city_parts = ["Alaska"]
        if community:
            city_parts.append(community)
        city = f"{city_parts[0]} ({city_parts[1]})" if len(city_parts) > 1 else city_parts[0]

        # Link to BidX
        link = f"https://www.bidx.com/ak/proposal?contid={contract_num}"

        # Contract name
        contract_name = short_desc
        if fed_proj:
            contract_name = f"{short_desc} ({fed_proj})"

        results.append({
            "city": city,
            "company_name": title_case(vendor),
            "contact_name": "",
            "phone": "",
            "email": "",
            "address": "",
            "website": "",
            "contract_name": contract_name,
            "award_amount": amount,
            "amount_expended": "",
            "begin_date": award_date,
            "award_link": link,
            "description": desc,
            "commodity_type": f"{naics_label} (NAICS {naics_code})",
        })

    print(f"After filtering (2026, >=${MIN_AMOUNT:,}): {len(results)} contracts")
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
        print(f"   {r['city']} | Award: {r['begin_date']}")
        print(f"   {r['description'][:120]}")
        print()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Alaska DOT Construction Contracts Scraper")
    parser.add_argument("--preview", action="store_true", help="Preview without writing to sheet")
    parser.add_argument("--append", action="store_true", default=True, help="Append (default)")
    parser.add_argument("--replace", action="store_true", help="Replace existing data")
    args = parser.parse_args()

    results = scrape_all()

    if not results:
        print("No contracts matched filters.")
        sys.exit(0)

    preview_results(results)

    if args.preview:
        print(f"Preview mode — run without --preview to write {len(results)} rows.")
    else:
        write_to_google_sheets(results, append=not args.replace)
        print(f"\nView: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
