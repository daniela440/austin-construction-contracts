"""
Iowa Construction Contractors Scraper
Pulls registered construction contractors from Iowa Open Data portal using Socrata API.
Filters by NAICS codes matching construction trades.
Exports directly to Google Sheets.

NOTE: This is a contractor REGISTRATION list, not awarded contracts.
- Has contact info for outreach
- Does NOT have award amounts
- Useful for contractor lead generation
"""

import json
import ssl
import sys
import urllib.parse
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
# Socrata API endpoint for Iowa Construction Contractor Registrations
SOCRATA_ENDPOINT = "https://data.iowa.gov/resource/dpf3-iz94.json"

# Target NAICS codes for construction trades
TARGET_NAICS = [
    "238210 - Electrical and Wiring Installation",
    "236220 - Commercial & Institutional Bldg Construction",
    "237310 - Highway/Street and Bridge Construction",
    "238220 - Plumbing/Heating & A/C Contractors",
    "238120 - Structural Steel/Precast Concrete Contractors",
]

# Google Sheets config (same as other scrapers)
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Iowa Contractors"


def build_soql_query():
    """Build the SoQL $where clause for filtering by NAICS codes."""
    conditions = [f"primary_activity='{naics}'" for naics in TARGET_NAICS]
    return " OR ".join(conditions)


def fetch_from_socrata():
    """Fetch data from Socrata API with NAICS filters."""
    where_clause = build_soql_query()
    params = {
        "$where": where_clause,
        "$limit": 10000,  # Max records
        "$order": "business_name ASC",
    }
    url = f"{SOCRATA_ENDPOINT}?{urllib.parse.urlencode(params)}"

    print(f"Socrata API Query:")
    print(f"  Endpoint: {SOCRATA_ENDPOINT}")
    print(f"  NAICS filters: {len(TARGET_NAICS)} codes")

    req = Request(url, headers={
        "User-Agent": "IowaContractorScraper/1.0",
        "Accept": "application/json",
    })
    with urlopen(req, context=SSL_CTX, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def extract_naics_code(primary_activity):
    """Extract just the NAICS code from the full description."""
    if primary_activity and " - " in primary_activity:
        return primary_activity.split(" - ")[0]
    return primary_activity or ""


def extract_naics_label(primary_activity):
    """Extract the NAICS label from the full description."""
    if primary_activity and " - " in primary_activity:
        return primary_activity.split(" - ", 1)[1]
    return primary_activity or ""


def format_phone(phone):
    """Clean up phone number formatting."""
    if not phone:
        return ""
    # Remove non-digits
    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone


def build_full_address(row):
    """Build full address from components.

    Note: address_2 in this dataset duplicates city/state/zip, so we ignore it
    and build from the separate fields instead.
    """
    parts = []

    # Street address
    if row.get("address_1"):
        parts.append(row["address_1"].strip())

    # Build city, state zip from separate fields (ignore address_2 - it's a duplicate)
    city = row.get("city", "").strip()
    state = row.get("state", "").strip()
    zip_code = row.get("zip_code", "").strip()

    city_state_zip = ""
    if city:
        city_state_zip = city
        if state:
            city_state_zip += f", {state}"
        if zip_code:
            city_state_zip += f" {zip_code}"
    elif state or zip_code:
        city_state_zip = f"{state} {zip_code}".strip()

    if city_state_zip:
        parts.append(city_state_zip)

    return ", ".join(parts) if parts else ""


def scrape_all():
    """Main scrape pipeline: fetch from Socrata API -> transform -> return results."""
    data = fetch_from_socrata()

    print(f"\n--- Socrata API returned {len(data)} contractor registrations ---")

    results = []
    seen = set()

    # Count by NAICS
    naics_counts = {}

    for row in data:
        business_name = row.get("business_name", "").strip()

        # Deduplicate by business name
        if business_name.upper() in seen:
            continue
        seen.add(business_name.upper())

        primary_activity = row.get("primary_activity", "")
        naics_code = extract_naics_code(primary_activity)
        naics_label = extract_naics_label(primary_activity)

        # Count by NAICS
        naics_counts[naics_label] = naics_counts.get(naics_label, 0) + 1

        # Build contact name
        first_name = row.get("first_name", "").strip()
        last_name = row.get("last_name", "").strip()
        contact_name = f"{first_name} {last_name}".strip()

        results.append({
            "company_name": business_name,
            "contact_name": contact_name,
            "phone": format_phone(row.get("phone", "")),
            "email": row.get("email_address", "").strip(),
            "address": build_full_address(row),
            "website": "",  # Not available in this dataset
            "contract_name": "",  # N/A - this is registration data
            "award_amount": "",  # N/A - this is registration data
            "amount_expended": "",  # N/A
            "begin_date": row.get("issue_date", "")[:10] if row.get("issue_date") else "",
            "award_link": "",  # N/A
            "description": f"Registered contractor - {naics_label}",
            "commodity_type": f"{naics_label} (NAICS {naics_code})",
            "city": "Iowa",  # State-level data
            "registration_number": row.get("registration_number", ""),
            "expire_date": row.get("expire_date", "")[:10] if row.get("expire_date") else "",
        })

    print(f"\nAfter deduplication: {len(results)} unique contractors")
    print(f"\nBy NAICS category:")
    for label, count in sorted(naics_counts.items(), key=lambda x: -x[1]):
        print(f"  {label}: {count}")

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
    "Contract Name", "Award Amount", "Amount Expended", "Registration Date",
    "Award Link", "Description", "Trade Category",
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
            clear_range = f"'{SHEET_NAME}'!A1:Z5000"
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


def preview_results(results, limit=15):
    """Print a preview of the results."""
    print(f"\n--- Preview (first {min(limit, len(results))} results) ---\n")
    for i, r in enumerate(results[:limit], 1):
        print(f"{i}. {r['company_name']}")
        print(f"   Contact: {r['contact_name']}")
        print(f"   Phone: {r['phone']} | Email: {r['email']}")
        print(f"   Address: {r['address']}")
        print(f"   Trade: {r['commodity_type']}")
        print(f"   Registration: {r['registration_number']} (expires {r['expire_date']})")
        print()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Iowa Construction Contractors Scraper")
    parser.add_argument("--preview", action="store_true", help="Preview results without writing to sheet")
    parser.add_argument("--append", action="store_true", help="Append to existing sheet data instead of replacing")
    args = parser.parse_args()

    results = scrape_all()

    if not results:
        print("\nNo contractors matched the NAICS filters.")
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
