"""
NJ START Construction Contracts Scraper
Scrapes awarded construction contracts from New Jersey's NJSTART procurement portal.
Uses Playwright (headless Chrome) because the JSF/PrimeFaces app blocks raw HTTP.
Enriches with vendor contact info from public vendor profile pages.
Filters: begin date in 2026, dollars spent > $250K.
Exports directly to Google Sheets.
"""

import argparse
import re
import sys
import time

from playwright.sync_api import sync_playwright

from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Config ---
SEARCH_URL = "https://www.njstart.gov/bso/view/search/external/advancedSearchContractBlanket.xhtml?view=activeContracts"
VENDOR_PROFILE_URL = "https://www.njstart.gov/bso/external/vendor/vendorProfileOrgInfo.sda?external=true&vendorId={vendor_id}"
CONTRACT_LINK_TEMPLATE = "https://www.njstart.gov/bso/external/purchaseorder/poSummary.sda?docId={contract_num}&releaseNbr=0&external=true&parentUrl=close"

MIN_AMOUNT = 250_000
MIN_YEAR = 2026
REQUEST_DELAY = 2  # seconds between vendor profile fetches

# Construction-related categories on NJ START
SEARCH_CATEGORIES = {
    "05": "Building Equipment, Supplies, and Services",
    "13": "Highway Road Equipment, Materials, and Related Equipment",
    "22": "Public Works, Park Equipment, and Construction Services",
    "28": "The Trades: Electrical, Engineering, HVAC, Plumbing, and Welding",
}

# NAICS mapping based on NJ contract descriptions
NAICS_FILTERS = {
    "238210": ("Electrical", [
        "electrical", "electric", "lighting", "wiring", "power distribution",
    ]),
    "236220": ("Commercial Building", [
        "building construction", "construction services", "general construct",
        "facility", "facilities", "renovation", "dpmc", "building maintenance",
        "building solution",
    ]),
    "237310": ("Highway/Street/Bridge", [
        "highway", "street", "road", "bridge", "asphalt", "paving",
        "pavement", "sweeper", "snow plow", "snow removal", "salting",
        "traffic", "dot", "ground", "tree trimm", "tree removal",
        "excavat", "demolit",
    ]),
    "238220": ("Plumbing/HVAC", [
        "plumbing", "hvac", "mechanical", "heating", "cooling",
        "air conditioning", "boiler", "refrigerat", "water treatment",
    ]),
    "238120": ("Structural Steel/Trades", [
        "structural steel", "steel erection", "welding", "iron work",
        "metal building", "elevator", "escalator",
    ]),
}

# Google Sheets config
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

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


def parse_amount(text):
    """Parse dollar amount string like '$1,234,567.89' to float."""
    text = text.replace("$", "").replace(",", "").strip()
    try:
        return float(text)
    except ValueError:
        return 0.0


def match_naics(text):
    """Match description text to a NAICS code and label."""
    lower = text.lower()
    for code, (label, keywords) in NAICS_FILTERS.items():
        for kw in keywords:
            if kw in lower:
                return code, label
    return None, None


def search_contracts(playwright_page, category_id):
    """Search NJ START for contracts in a given category, return all rows."""
    playwright_page.goto(SEARCH_URL, timeout=60000)
    playwright_page.wait_for_load_state("networkidle")

    playwright_page.locator(
        "#contractBlanketSearchForm\\:categoryId"
    ).select_option(category_id)
    time.sleep(1)

    playwright_page.locator(
        "#contractBlanketSearchForm\\:btnPoSearch"
    ).click()
    time.sleep(5)
    playwright_page.wait_for_load_state("networkidle")

    rows_data = []
    page_num = 1

    while True:
        rows = playwright_page.locator("#advSearchResults tr[data-ri]")
        row_count = rows.count()

        for i in range(row_count):
            cells = rows.nth(i).locator("td")
            cell_count = cells.count()
            row = []
            for j in range(cell_count):
                row.append(cells.nth(j).inner_text().strip())
            rows_data.append(row)

        next_btn = playwright_page.locator(
            ".ui-paginator-next:not(.ui-state-disabled)"
        )
        if next_btn.count() > 0 and page_num < 50:
            next_btn.first.click()
            time.sleep(2)
            playwright_page.wait_for_load_state("networkidle")
            page_num += 1
        else:
            break

    return rows_data


def fetch_vendor_profile(playwright_page, vendor_name):
    """Search for a vendor by name and scrape their public profile page.
    Returns dict with contact_name, phone, email, address, website.
    """
    empty = {
        "contact_name": "", "phone": "", "email": "",
        "address": "", "website": "",
    }

    try:
        playwright_page.goto(SEARCH_URL, timeout=60000)
        playwright_page.wait_for_load_state("networkidle")

        # Switch to Vendors search
        playwright_page.locator(
            "#advancedSearchForm\\:documentTypeSelect"
        ).select_option("VENDORS")
        time.sleep(3)
        playwright_page.wait_for_load_state("networkidle")

        # Fill vendor name and search
        vendor_field = playwright_page.locator('input[id*="vendorName"]').first
        vendor_field.fill(vendor_name)

        playwright_page.locator('button:has-text("Search")').first.click()
        time.sleep(5)
        playwright_page.wait_for_load_state("networkidle")

        # Get first result's vendor link
        vendor_rows = playwright_page.locator("#advSearchResults tr[data-ri]")
        if vendor_rows.count() == 0:
            return empty

        # Extract from search results: Vendor ID, Name, Address, City, State, Zip, Contact, Phone
        cells = vendor_rows.first.locator("td")
        cell_count = cells.count()
        search_data = []
        for j in range(cell_count):
            search_data.append(cells.nth(j).inner_text().strip())

        # search_data layout: [VendorID, VendorID(dup), Name, Address, City, State, Zip, Contact, Phone]
        contact_from_search = search_data[7] if len(search_data) > 7 else ""
        phone_from_search = search_data[8] if len(search_data) > 8 else ""
        address_parts = [
            search_data[3] if len(search_data) > 3 else "",  # address
            search_data[4] if len(search_data) > 4 else "",  # city
            search_data[5] if len(search_data) > 5 else "",  # state
            search_data[6] if len(search_data) > 6 else "",  # zip
        ]
        address_str = ", ".join(p for p in address_parts if p)

        # Navigate to vendor profile for email
        vendor_link = vendor_rows.first.locator('a[href*="vendor"]').first
        href = vendor_link.get_attribute("href")
        if not href:
            return {
                "contact_name": contact_from_search,
                "phone": phone_from_search,
                "email": "",
                "address": address_str,
                "website": "",
            }

        full_url = f"https://www.njstart.gov{href}" if href.startswith("/") else href
        playwright_page.goto(full_url, timeout=60000)
        time.sleep(2)
        playwright_page.wait_for_load_state("networkidle")

        body_text = playwright_page.locator("body").inner_text()

        # Parse email from profile
        email = ""
        email_match = re.search(r"Vendor Email:\s*(\S+@\S+)", body_text)
        if email_match:
            email = email_match.group(1).strip()

        # Parse business description
        biz_desc = ""
        biz_match = re.search(r"Business Description:\s*(.+?)(?:\t|Preferred)", body_text)
        if biz_match:
            biz_desc = biz_match.group(1).strip()

        return {
            "contact_name": contact_from_search,
            "phone": phone_from_search,
            "email": email,
            "address": address_str,
            "website": "",  # not available on NJ START
            "business_description": biz_desc,
        }

    except Exception as e:
        print(f"    ERROR fetching vendor profile: {e}")
        return empty


def scrape_all():
    """Main scrape pipeline: search categories -> filter -> enrich vendors -> results."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Step 1: Collect contracts from all construction categories
        all_rows = []
        for cat_id, cat_name in SEARCH_CATEGORIES.items():
            print(f"\nSearching category {cat_id}: {cat_name}")
            rows = search_contracts(page, cat_id)
            for row in rows:
                all_rows.append((cat_id, cat_name, row))
            print(f"  Found {len(rows)} contracts")

        print(f"\nTotal contracts across all categories: {len(all_rows)}")

        # Step 2: Filter by begin date (2026) and amount (>$250K)
        # Row layout: [Contract#, Contract#(dup), Bid#, BidSolicitation#,
        #              Description, Vendor, TypeCode, DollarsSpent,
        #              Organization, Status, BeginDate, EndDate]
        filtered = []
        seen = set()
        skipped_year = 0
        skipped_amount = 0
        skipped_dup = 0

        for cat_id, cat_name, row in all_rows:
            if len(row) < 12:
                continue

            contract_num = row[0]
            description = row[4]
            vendor_name = row[5]
            dollars_spent = parse_amount(row[7])
            begin_date = row[10]
            end_date = row[11]

            # Filter: begin date must be in 2026
            if str(MIN_YEAR) not in begin_date:
                skipped_year += 1
                continue

            # Filter: amount > $250K
            if dollars_spent < MIN_AMOUNT:
                skipped_amount += 1
                continue

            # Deduplicate by contract number
            if contract_num in seen:
                skipped_dup += 1
                continue
            seen.add(contract_num)

            filtered.append({
                "contract_num": contract_num,
                "description": description,
                "vendor_name": vendor_name,
                "dollars_spent": dollars_spent,
                "begin_date": begin_date,
                "end_date": end_date,
                "category": cat_name,
                "cat_id": cat_id,
            })

        print(f"\n--- Filter Summary ---")
        print(f"Total scanned:       {len(all_rows)}")
        print(f"Skipped (year):      {skipped_year}")
        print(f"Skipped (amount):    {skipped_amount}")
        print(f"Skipped (duplicate): {skipped_dup}")
        print(f"Matched:             {len(filtered)}")

        if not filtered:
            browser.close()
            return []

        # Step 3: Enrich with vendor contact info
        print(f"\nEnriching {len(filtered)} contracts with vendor profiles...")
        vendor_cache = {}
        results = []

        for i, c in enumerate(filtered, 1):
            vname = c["vendor_name"]
            print(f"  [{i}/{len(filtered)}] {c['contract_num']} — {vname}")

            if vname not in vendor_cache:
                vendor_cache[vname] = fetch_vendor_profile(page, vname)
                time.sleep(REQUEST_DELAY)

            vi = vendor_cache[vname]

            # Match NAICS from description + category
            match_text = f"{c['description']} {c['category']}"
            naics_code, naics_label = match_naics(match_text)
            if not naics_code:
                naics_code, naics_label = "999999", "Construction (General)"

            contract_link = CONTRACT_LINK_TEMPLATE.format(
                contract_num=c["contract_num"]
            )

            results.append({
                "city": "New Jersey",
                "company_name": vname,
                "contact_name": vi.get("contact_name", ""),
                "phone": vi.get("phone", ""),
                "email": vi.get("email", ""),
                "address": vi.get("address", ""),
                "website": vi.get("website", ""),
                "contract_name": c["contract_num"],
                "award_amount": f"${c['dollars_spent']:,.2f}",
                "amount_expended": f"${c['dollars_spent']:,.2f}",
                "begin_date": c["begin_date"],
                "award_link": contract_link,
                "description": c["description"],
                "commodity_type": f"{naics_label} (NAICS {naics_code})",
            })

        browser.close()

    print(f"\n--- Results ---")
    print(f"Contracts with vendor info: {len(results)}")
    print(f"Unique vendors enriched:    {len(vendor_cache)}")

    return results


# --- Google Sheets ---

def get_sheets_service():
    """Authenticate and return Google Sheets API service."""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def get_existing_data(service):
    """Fetch existing data from the sheet."""
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
    """Write results to Google Sheets. If append=True, add to existing data."""
    service = get_sheets_service()
    sheet = service.spreadsheets()

    # Ensure sheet exists
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
        sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body=request
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

        rows = []
        for r in results:
            rows.append([r.get(f, "") for f in SHEET_FIELDS])

        if rows:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{SHEET_NAME}'!A{start_row}",
                valueInputOption="RAW",
                body={"values": rows},
            ).execute()
    else:
        rows = [SHEET_HEADERS]
        for r in results:
            rows.append([r.get(f, "") for f in SHEET_FIELDS])

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


def main():
    parser = argparse.ArgumentParser(
        description="Scrape NJ START construction contracts"
    )
    parser.add_argument(
        "--append", action="store_true", default=True,
        help="Append to existing sheet data (default)",
    )
    parser.add_argument(
        "--replace", action="store_true",
        help="Replace existing sheet data instead of appending",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Scrape and print results without writing to Google Sheets",
    )
    args = parser.parse_args()

    results = scrape_all()
    if not results:
        print("\nNo contracts matched filters "
              f"(year >= {MIN_YEAR}, amount >= ${MIN_AMOUNT:,}).")
        print("This is normal early in the fiscal year — re-run periodically.")
        sys.exit(0)

    if args.dry_run:
        print(f"\n--- Dry Run: {len(results)} contracts ---")
        for r in results:
            print(f"  {r['contract_name']:25s} | {r['company_name']:45s} | "
                  f"{r['award_amount']:>15s} | {r['begin_date']}")
            print(f"  {'':25s}   Contact: {r['contact_name']} | "
                  f"{r['phone']} | {r['email']}")
            print(f"  {'':25s}   Link: {r['award_link']}")
            print()
    else:
        write_to_google_sheets(results, append=not args.replace)
        print(f"\nView results at: "
              f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
