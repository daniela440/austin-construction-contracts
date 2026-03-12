"""
Illinois BidBuy purchase order scraper.

Targets publicly searchable 2026 purchase orders on the BidBuy portal that:
  - are in construction-oriented NIGP classes
  - have totals >= $250,000
  - map to one of the tracked NAICS buckets

Publicly available from the portal:
  - PO number
  - description
  - vendor
  - buyer / organization
  - status
  - sent date
  - total
  - vendor address / city / state / zip / contact / phone (via vendor search)

Not publicly available on the tested pages:
  - vendor email
  - vendor website
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

SEARCH_URL = "https://www.bidbuy.illinois.gov/bso/view/search/external/advancedSearchPurchaseOrder.xhtml"
VENDOR_SEARCH_URL = "https://www.bidbuy.illinois.gov/bso/view/search/external/advancedSearchVendor.xhtml"

TARGET_YEAR = 2026
MIN_AMOUNT = 250_000.0
REQUEST_DELAY = 0.5

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

TARGET_CLASSES = {
    "909": "Building Construction Services",
    "910": "Building Maintenance / Install / Repair Services",
    "913": "Heavy Construction Services",
    "914": "Construction Trades",
}

CLASS_DEFAULT_NAICS = {
    "909": ("236220", "Commercial Building (NAICS 236220)"),
    "913": ("237310", "Highway/Street/Bridge (NAICS 237310)"),
}

NAICS_FILTERS = [
    ("238210", "Electrical (NAICS 238210)", [
        "electrical", "electric", "lighting", "light pole", "light poles",
        "camera installation", "cctv", "signal", "fiber", "wiring",
        "power", "generator", "outlets", "panel", "switchgear",
    ]),
    ("238220", "Plumbing/HVAC (NAICS 238220)", [
        "hvac", "heating", "cooling", "plumbing", "boiler", "chiller",
        "air conditioning", "air handler", "pipe", "piping", "sanitary sewer",
        "water line", "water main", "mechanical",
    ]),
    ("238120", "Structural Steel (NAICS 238120)", [
        "structural steel", "steel erection", "steel", "metal frame",
        "iron work", "ironwork", "fabricated steel",
    ]),
    ("237310", "Highway/Street/Bridge (NAICS 237310)", [
        "highway", "road", "bridge", "asphalt", "paving", "repave",
        "mill and repave", "parking lot", "street", "interchange",
        "traffic", "sidewalk", "curb", "median", "drainage", "culvert",
        "roadway", "bituminous", "concrete pavement",
    ]),
    ("236220", "Commercial Building (NAICS 236220)", [
        "construction", "renovation", "renovations", "rehabilitation",
        "rehab", "addition", "build-out", "facility", "facilities",
        "building", "remodel", "interior renovation", "restaurant repairs",
        "glass partitions", "door replacement", "door repair",
    ]),
]

EXCLUDE_KEYWORDS = [
    "snow removal", "de-icing", "calibration", "hosting services",
    "ethernet", "telecom", "internet", "supplies", "filter", "filters",
    "safe cracking", "lock upgrade", "light bulbs", "fuse panel",
    "switch supplies", "diagnostic", "parts and labor", "parts ",
    "sealant", "stump removal", "tree", "quarterly", "janitorial",
    "uniform", "fuel", "office", "software",
]


def parse_amount(text: str) -> float:
    cleaned = re.sub(r"[^0-9.]", "", text or "")
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def parse_sent_date(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    for fmt in ("%m/%d/%Y", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %I:%M:%S %p"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def normalize_name(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def classify_naics(description: str, class_id: str) -> tuple[str, str] | None:
    lower = (description or "").lower()

    if any(keyword in lower for keyword in EXCLUDE_KEYWORDS):
        return None

    for code, label, keywords in NAICS_FILTERS:
        if any(keyword in lower for keyword in keywords):
            return code, label

    return CLASS_DEFAULT_NAICS.get(class_id)


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
    for s in spreadsheet.get("sheets", []):
        if s["properties"]["title"] == SHEET_NAME:
            sheet_id = s["properties"]["sheetId"]
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
        r for r in results
        if (r.get("company_name", "").strip().lower(),
            r.get("contract_name", "").strip().lower()) not in existing_fps
    ]

    if not existing:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A1",
            valueInputOption="RAW",
            body={"values": [SHEET_HEADERS]},
        ).execute()
        start_row = 2

    rows = [[r.get(field, "") for field in SHEET_FIELDS] for r in new_results]
    if rows:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A{start_row}",
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()

    print(f"Wrote {len(new_results)} Illinois rows to Google Sheet: {SHEET_NAME}")


def collect_search_rows(page) -> list[dict]:
    rows = []
    seen_pages = set()

    while True:
        marker = page.locator(
            "#poSearchResultsForm\\:poResultId_paginator_top .ui-paginator-current"
        ).inner_text().strip()
        if marker in seen_pages:
            break
        seen_pages.add(marker)

        entries = page.locator("#poSearchResultsForm\\:poResultId_data tr[data-ri]")
        count = entries.count()
        for idx in range(count):
            row = entries.nth(idx)
            cells = row.locator("td:not([style*='display:none'])")
            values = [cells.nth(i).inner_text().strip() for i in range(cells.count())]
            href = ""
            link = row.locator("a").first
            if link.count():
                href = link.get_attribute("href") or ""
            rows.append({"cells": values, "href": href})

        next_btn = page.locator(
            "#poSearchResultsForm\\:poResultId_paginator_bottom .ui-paginator-next:not(.ui-state-disabled)"
        )
        if next_btn.count():
            next_btn.click()
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(800)
        else:
            break

    return rows


def search_class_purchase_orders(page, class_id: str) -> list[dict]:
    print(f"\nSearching Illinois class {class_id}: {TARGET_CLASSES[class_id]}")
    page.goto(SEARCH_URL, timeout=120000)
    page.wait_for_load_state("networkidle")

    page.select_option("#poSearchForm\\:classId", class_id)
    page.wait_for_timeout(500)
    page.fill("#poSearchForm\\:sentDateFrom_input", "01/01/2026")
    page.click("#poSearchForm\\:btnPoSearch")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1200)

    current = page.locator(
        "#poSearchResultsForm\\:poResultId_paginator_top .ui-paginator-current"
    ).inner_text().strip()
    print(f"  Results window: {current}")
    return collect_search_rows(page)


def choose_vendor_match(vendor_name: str, candidates: list[dict]) -> dict:
    target = normalize_name(vendor_name)
    if not candidates:
        return {}

    for candidate in candidates:
        if normalize_name(candidate["vendor_name"]) == target:
            return candidate

    for candidate in candidates:
        if target and target in normalize_name(candidate["vendor_name"]):
            return candidate

    return candidates[0]


def fetch_vendor_info(page, vendor_name: str) -> dict:
    empty = {
        "contact_name": "",
        "phone": "",
        "address": "",
        "city": "",
    }

    try:
        page.goto(VENDOR_SEARCH_URL, timeout=120000)
        page.wait_for_load_state("networkidle")
        page.fill("#vendorSearchForm\\:vendorName", vendor_name)
        page.select_option("#vendorSearchForm\\:country", "US")
        page.click("#vendorSearchForm\\:btnVendorSearch")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1200)

        rows = []
        entries = page.locator("#vendorSearchResultsForm\\:vendorResultId_data tr[data-ri]")
        count = entries.count()
        for idx in range(count):
            row = entries.nth(idx)
            cells = row.locator("td:not([style*='display:none'])")
            values = [cells.nth(i).inner_text().strip() for i in range(cells.count())]
            if len(values) < 8:
                continue
            rows.append({
                "vendor_id": values[0],
                "vendor_name": values[1],
                "address": values[2],
                "city": values[3],
                "state": values[4],
                "postal_code": values[5],
                "contact_name": values[6],
                "phone": values[7],
            })

        best = choose_vendor_match(vendor_name, rows)
        if not best:
            return empty

        address_parts = [
            best.get("address", ""),
            ", ".join(part for part in [best.get("city", ""), best.get("state", ""), best.get("postal_code", "")] if part),
        ]
        return {
            "contact_name": best.get("contact_name", ""),
            "phone": best.get("phone", ""),
            "address": ", ".join(part for part in address_parts if part),
            "city": best.get("city", ""),
        }
    except PlaywrightTimeoutError:
        return empty


def scrape_all() -> list[dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})

        all_rows = []
        for class_id in TARGET_CLASSES:
            rows = search_class_purchase_orders(page, class_id)
            print(f"  Raw rows: {len(rows)}")
            all_rows.extend((class_id, row) for row in rows)

        print(f"\nCollected {len(all_rows)} raw Illinois purchase-order rows")

        filtered = []
        seen_pos = set()
        skipped_amount = 0
        skipped_naics = 0
        skipped_dup = 0
        skipped_year = 0

        for class_id, row in all_rows:
            values = row["cells"]
            if len(values) < 9:
                continue

            po_number = values[0].replace("PO #", "").strip()
            description = values[1]
            vendor_name = values[2]
            organization = values[3]
            buyer = values[5] if len(values) > 5 else ""
            status = values[6] if len(values) > 6 else ""
            sent_date_raw = values[7] if len(values) > 7 else ""
            total_text = values[8] if len(values) > 8 else ""
            amount = parse_amount(total_text)
            sent_date = parse_sent_date(sent_date_raw)

            if not sent_date.startswith(str(TARGET_YEAR)):
                skipped_year += 1
                continue
            if amount < MIN_AMOUNT:
                skipped_amount += 1
                continue
            if po_number in seen_pos:
                skipped_dup += 1
                continue

            commodity = classify_naics(description, class_id)
            if not commodity:
                skipped_naics += 1
                continue

            seen_pos.add(po_number)
            filtered.append({
                "po_number": po_number,
                "description": description,
                "vendor_name": vendor_name,
                "organization": organization,
                "buyer": buyer,
                "status": status,
                "sent_date": sent_date,
                "total_text": total_text,
                "amount": amount,
                "class_id": class_id,
                "award_link": row["href"] if row["href"].startswith("http") else f"https://www.bidbuy.illinois.gov{row['href']}",
                "commodity_type": commodity[1],
            })

        print("\n--- Illinois Filter Summary ---")
        print(f"Total scanned:       {len(all_rows)}")
        print(f"Skipped (year):      {skipped_year}")
        print(f"Skipped (amount):    {skipped_amount}")
        print(f"Skipped (NAICS):     {skipped_naics}")
        print(f"Skipped (duplicate): {skipped_dup}")
        print(f"Matched:             {len(filtered)}")

        if not filtered:
            browser.close()
            return []

        vendor_cache = {}
        results = []
        for idx, contract in enumerate(filtered, 1):
            vendor_name = contract["vendor_name"]
            print(f"  [{idx}/{len(filtered)}] {contract['po_number']} | {vendor_name} | {contract['total_text']}")

            if vendor_name not in vendor_cache:
                vendor_cache[vendor_name] = fetch_vendor_info(page, vendor_name)
                time.sleep(REQUEST_DELAY)

            vendor = vendor_cache[vendor_name]
            results.append({
                "city": vendor.get("city", "") or "Illinois",
                "company_name": vendor_name,
                "contact_name": vendor.get("contact_name", ""),
                "phone": vendor.get("phone", ""),
                "email": "",
                "address": vendor.get("address", ""),
                "website": "",
                "contract_name": contract["po_number"],
                "award_amount": contract["total_text"],
                "amount_expended": contract["total_text"],
                "begin_date": contract["sent_date"],
                "award_link": contract["award_link"],
                "description": contract["description"],
                "commodity_type": contract["commodity_type"],
            })

        browser.close()
        return results


def main():
    parser = argparse.ArgumentParser(description="Scrape Illinois BidBuy construction purchase orders")
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing to Google Sheets")
    args = parser.parse_args()

    results = scrape_all()
    if not results:
        print("\nNo Illinois BidBuy purchase orders matched the filters.")
        sys.exit(0)

    if args.dry_run:
        print(f"\n--- Dry Run: {len(results)} Illinois contracts ---")
        for row in results:
            print(
                f"{row['company_name']} | {row['contract_name']} | {row['award_amount']} | "
                f"{row['begin_date']} | {row['commodity_type']}"
            )
            print(f"  {row['description']}")
            print(f"  {row['award_link']}")
            if row["contact_name"] or row["phone"] or row["address"]:
                print(f"  Contact: {row['contact_name']} | {row['phone']} | {row['address']}")
            print()
    else:
        write_to_google_sheets(results)
        print(f"\nView results at: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
