"""
Nevada ePro construction contracts scraper.

Public source:
  https://nevadaepro.com/bso/view/search/external/advancedSearchContractBlanket.xhtml?view=activeContracts

Filters:
  - Contract Begin Date is in 2026
  - Description clearly indicates larger construction / infrastructure work

Publicly available:
  - Contract number
  - Bid number
  - Description
  - Vendor
  - Dollars spent to date
  - Organization
  - Status
  - Begin / End dates
  - Vendor contact name / phone from vendor search
  - Vendor address / email from public vendor profile tabs

Not reliably public on the tested pages:
  - Original award amount
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

SEARCH_URL = "https://nevadaepro.com/bso/view/search/external/advancedSearchContractBlanket.xhtml?view=activeContracts"
VENDOR_SEARCH_URL = "https://nevadaepro.com/bso/view/search/external/advancedSearchVendor.xhtml"
VENDOR_PROFILE_URL = "https://nevadaepro.com/bso/external/vendor/vendorProfileOrgInfo.sda?external=true&vendorId={vendor_id}"
VENDOR_ADDRESS_URL = "https://nevadaepro.com/bso/external/vendor/vendorProfileAddressInfo.sda?external=true&vendorId={vendor_id}"
CONTRACT_LINK_TEMPLATE = "https://nevadaepro.com/bso/external/purchaseorder/poSummary.sda?docId={contract_num}&releaseNbr=0&external=true&parentUrl=close"

TARGET_YEAR = 2026
REQUEST_DELAY = 1.5

SEARCH_CATEGORIES = {
    "05": "Building Equipment, Supplies, and Services",
    "13": "Highway Road Equipment, Materials, and Related Equipment",
    "22": "Public Works, Park Equipment, and Construction Services",
    "28": "The Trades: Electrical, Engineering, HVAC, Plumbing, and Welding",
}

NAICS_FILTERS = [
    ("237310", "Highway/Street/Bridge (NAICS 237310)", [
        "highway", "bridge", "road", "street", "asphalt", "paving",
        "resurfacing", "roadway", "traffic control", "guardrail",
        "striping", "concrete", "culvert", "drainage", "intersection",
        "freeway", "interstate", "slope repair", "earthwork",
    ]),
    ("236220", "Commercial Building (NAICS 236220)", [
        "construction", "renovation", "rehabilitation", "facility",
        "building", "remodel", "tenant improvement", "site improvements",
        "expansion", "replacement facility", "capital improvement",
    ]),
    ("238210", "Electrical (NAICS 238210)", [
        "electrical", "lighting", "signal", "fiber", "wiring", "power",
        "generator", "substation", "intelligent transportation",
    ]),
    ("238220", "Plumbing/HVAC (NAICS 238220)", [
        "hvac", "plumbing", "mechanical", "boiler", "chiller",
        "air conditioning", "water line", "sewer", "pump station",
    ]),
    ("238120", "Structural Steel (NAICS 238120)", [
        "structural steel", "steel erection", "girder", "metal building",
        "iron work", "welding", "fabricated steel",
    ]),
]

EXCLUDE_KEYWORDS = [
    "grounds maintenance equipment",
    "fuel",
    "office",
    "software",
    "janitorial",
    "uniform",
    "furniture",
    "fire fuels reduction",
    "vegetation management",
    "seed restoration",
    "tree service",
    "parts only",
    "supplies only",
]

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


def parse_amount(text: str) -> float:
    cleaned = re.sub(r"[^0-9.]", "", text or "")
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def parse_date(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def normalize_name(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def classify_naics(text: str) -> tuple[str, str] | None:
    lower = (text or "").lower()

    if any(keyword in lower for keyword in EXCLUDE_KEYWORDS):
        return None

    for code, label, keywords in NAICS_FILTERS:
        if any(keyword in lower for keyword in keywords):
            return code, label
    return None


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


def write_to_google_sheets(results):
    service = get_sheets_service()
    sheet = service.spreadsheets()

    spreadsheet = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id = None
    for current_sheet in spreadsheet.get("sheets", []):
        if current_sheet["properties"]["title"] == SHEET_NAME:
            sheet_id = current_sheet["properties"]["sheetId"]
            break

    if sheet_id is None:
        request = {"requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]}
        sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=request).execute()

    existing = get_existing_data(service)
    start_row = len(existing) + 1
    existing_fps = {
        (row[1].strip().lower(), row[7].strip().lower())
        for row in (existing[1:] if len(existing) > 1 else [])
        if len(row) > 7
    }

    new_results = [
        row for row in results
        if (
            row.get("company_name", "").strip().lower(),
            row.get("contract_name", "").strip().lower(),
        ) not in existing_fps
    ]

    if not existing:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A1",
            valueInputOption="RAW",
            body={"values": [SHEET_HEADERS]},
        ).execute()
        start_row = 2

    rows = [[row.get(field, "") for field in SHEET_FIELDS] for row in new_results]
    if rows:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A{start_row}",
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()

    print(f"Wrote {len(new_results)} Nevada ePro rows to Google Sheet: {SHEET_NAME}")


def search_contracts(page, category_id: str) -> list[list[str]]:
    page.goto(SEARCH_URL, timeout=120000)
    page.wait_for_load_state("networkidle")

    page.select_option("#contractBlanketSearchForm\\:categoryId", category_id)
    page.wait_for_timeout(500)
    page.click("#contractBlanketSearchForm\\:btnPoSearch")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(2500)

    rows_data: list[list[str]] = []
    seen_markers = set()

    while True:
        marker_locator = page.locator("#contractBlanketSearchResultsForm\\:contractResultId_paginator_top .ui-paginator-current")
        marker = marker_locator.inner_text().strip() if marker_locator.count() else str(len(rows_data))
        if marker in seen_markers:
            break
        seen_markers.add(marker)

        rows = page.locator("#advSearchResults tr[data-ri]")
        row_count = rows.count()
        for idx in range(row_count):
            cells = rows.nth(idx).locator("td")
            rows_data.append([cells.nth(i).inner_text().strip() for i in range(cells.count())])

        next_btn = page.locator(".ui-paginator-next:not(.ui-state-disabled)").first
        if next_btn.count() == 0:
            break
        next_btn.click()
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1500)

    return rows_data


def search_vendor(page, vendor_name: str) -> dict[str, str]:
    empty = {
        "vendor_id": "",
        "contact_name": "",
        "phone": "",
        "email": "",
        "address": "",
    }

    try:
        page.goto(VENDOR_SEARCH_URL, timeout=120000)
        page.wait_for_load_state("networkidle")

        page.fill("#vendorSearchForm\\:vendorName", vendor_name)
        page.click("#vendorSearchForm\\:btnVendorSearch")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2500)

        row = page.locator("#advSearchResults tr[data-ri]").first
        if row.count() == 0:
            return empty

        cells = row.locator("td")
        values = [cells.nth(i).inner_text().strip() for i in range(cells.count())]
        vendor_link = row.locator("a[href*='vendorProfileOrgInfo']").first
        href = vendor_link.get_attribute("href") or ""
        vendor_id_match = re.search(r"vendorId=([^&]+)", href)
        vendor_id = vendor_id_match.group(1) if vendor_id_match else (values[0] if values else "")

        result = {
            "vendor_id": vendor_id,
            "contact_name": values[7] if len(values) > 7 else "",
            "phone": values[8] if len(values) > 8 else "",
            "email": "",
            "address": ", ".join(part for part in [
                values[3] if len(values) > 3 else "",
                values[4] if len(values) > 4 else "",
                values[5] if len(values) > 5 else "",
                values[6] if len(values) > 6 else "",
            ] if part),
        }

        if not vendor_id:
            return result

        page.goto(VENDOR_PROFILE_URL.format(vendor_id=vendor_id), timeout=120000)
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1000)
        body_text = page.locator("body").inner_text()

        email_match = re.search(r"Vendor Email:\s*([^\s]+@[^\s]+)", body_text)
        if email_match:
            result["email"] = email_match.group(1).strip()

        page.goto(VENDOR_ADDRESS_URL.format(vendor_id=vendor_id), timeout=120000)
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1000)
        address_body = page.locator("body").inner_text()

        block_match = re.search(
            r"Address Information\s+Name\s+Address Type\s+Address Information.*?\n(.*?)\n(?:Id:|Copyright)",
            address_body,
            re.S,
        )
        if block_match:
            block = block_match.group(1)
            lines = [line.strip() for line in block.splitlines() if line.strip()]
            if lines:
                filtered = [
                    line for line in lines
                    if not line.startswith("General")
                    and not line.startswith("Remit")
                    and not line.startswith("Email:")
                    and not line.startswith("Phone:")
                ]
                if filtered:
                    result["address"] = ", ".join(filtered[:4])

            if not result["email"]:
                email_match = re.search(r"Email:\s*([^\s]+@[^\s]+)", block)
                if email_match:
                    result["email"] = email_match.group(1).strip()

            phone_match = re.search(r"Phone:\s*([()\-\d\s]+)", block)
            if phone_match and not result["phone"]:
                result["phone"] = phone_match.group(1).strip()

        return result

    except PlaywrightTimeoutError:
        return empty
    except Exception as exc:
        print(f"    Vendor lookup failed for {vendor_name}: {exc}")
        return empty


def scrape_all() -> list[dict[str, str]]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1200})

        all_rows: list[tuple[str, str, list[str]]] = []
        for category_id, category_name in SEARCH_CATEGORIES.items():
            print(f"\nSearching Nevada category {category_id}: {category_name}")
            rows = search_contracts(page, category_id)
            print(f"  Found {len(rows)} rows")
            all_rows.extend((category_id, category_name, row) for row in rows)

        print(f"\nTotal rows scanned: {len(all_rows)}")

        filtered = []
        seen = set()
        skipped_year = 0
        skipped_nonconstruction = 0
        skipped_dup = 0

        for category_id, category_name, row in all_rows:
            if len(row) < 12:
                continue

            contract_num = row[0].replace("Contract #", "").strip()
            bid_num = row[2].replace("Bid Solicitation #", "").strip()
            description = row[4]
            vendor_name = row[5]
            dollars_spent = parse_amount(row[7])
            organization = row[8]
            status = row[9]
            begin_date_raw = row[10]
            end_date_raw = row[11]
            begin_date = parse_date(begin_date_raw)
            end_date = parse_date(end_date_raw)

            if not begin_date.startswith(str(TARGET_YEAR)):
                skipped_year += 1
                continue

            match_text = f"{category_name} {description}"
            naics = classify_naics(match_text)
            if not naics:
                skipped_nonconstruction += 1
                continue

            dedup_key = (contract_num.lower(), vendor_name.lower())
            if dedup_key in seen:
                skipped_dup += 1
                continue
            seen.add(dedup_key)

            filtered.append(
                {
                    "contract_num": contract_num,
                    "bid_num": bid_num,
                    "description": description,
                    "vendor_name": vendor_name,
                    "dollars_spent": dollars_spent,
                    "organization": organization,
                    "status": status,
                    "begin_date": begin_date,
                    "end_date": end_date,
                    "category_name": category_name,
                    "commodity_type": naics[1],
                }
            )

        print("\n--- Filter Summary ---")
        print(f"Skipped (begin date not 2026): {skipped_year}")
        print(f"Skipped (not large construction): {skipped_nonconstruction}")
        print(f"Skipped (duplicate): {skipped_dup}")
        print(f"Matched: {len(filtered)}")

        vendor_cache: dict[str, dict[str, str]] = {}
        results = []

        for idx, contract in enumerate(filtered, start=1):
            vendor_name = contract["vendor_name"]
            print(f"  [{idx}/{len(filtered)}] {contract['contract_num']} — {vendor_name}")

            if vendor_name not in vendor_cache:
                vendor_cache[vendor_name] = search_vendor(page, vendor_name)
                time.sleep(REQUEST_DELAY)

            vendor = vendor_cache[vendor_name]
            contract_link = CONTRACT_LINK_TEMPLATE.format(contract_num=contract["contract_num"])
            description = " | ".join(
                part for part in [
                    contract["description"],
                    f"Organization: {contract['organization']}",
                    f"Status: {contract['status']}",
                    f"End Date: {contract['end_date']}" if contract["end_date"] else "",
                ]
                if part
            )

            results.append(
                {
                    "city": "Nevada",
                    "company_name": vendor_name,
                    "contact_name": vendor.get("contact_name", ""),
                    "phone": vendor.get("phone", ""),
                    "email": vendor.get("email", ""),
                    "address": vendor.get("address", ""),
                    "website": "",
                    "contract_name": contract["contract_num"],
                    "award_amount": "",
                    "amount_expended": f"${contract['dollars_spent']:,.2f}" if contract["dollars_spent"] else "",
                    "begin_date": contract["begin_date"],
                    "award_link": contract_link,
                    "description": description,
                    "commodity_type": contract["commodity_type"],
                }
            )

        browser.close()
        print(f"\nContracts with vendor info: {len(results)}")
        return results


def main():
    parser = argparse.ArgumentParser(description="Nevada ePro construction contracts scraper")
    parser.add_argument("--dry-run", action="store_true", help="Scrape and print results without writing to Google Sheets")
    args = parser.parse_args()

    results = scrape_all()
    if not results:
        print("No Nevada ePro contracts matched the 2026 begin-date + large-construction filters.")
        sys.exit(0)

    if args.dry_run:
        for row in results[:25]:
            print(
                f"{row['contract_name']} | {row['company_name']} | "
                f"{row['begin_date']} | {row['commodity_type']}"
            )
            print(f"  {row['description']}")
            print(f"  {row['contact_name']} | {row['phone']} | {row['email']}")
            print(f"  {row['address']}")
            print()
        return

    write_to_google_sheets(results)


if __name__ == "__main__":
    main()
