"""
University of Iowa BuildUI — Awarded Construction Contracts Scraper
Scrapes awarded construction bids from the U of Iowa Facilities Management portal.
Uses Playwright (headless Chrome) because the portal is JS-rendered.

Filters applied:
  - "Awarded To" column is populated (contract has been awarded)
  - Construction estimate > $250,000

Data source: https://buildui.facilities.uiowa.edu/buildui/bids
Detail page: https://buildui.facilities.uiowa.edu/buildui/project-info?projnumber=XXXXXXX

Contact info (Project Manager) comes from the detail page.
No vendor phone/address available from this portal.
"""

import re
import sys
import time

from playwright.sync_api import sync_playwright

from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Config ---
BIDS_URL       = "https://buildui.facilities.uiowa.edu/buildui/bids"
DETAIL_URL     = "https://buildui.facilities.uiowa.edu/buildui/project-info?projnumber={proj}"

MIN_AMOUNT     = 250_000
REQUEST_DELAY  = 1.5  # seconds between detail page fetches

# --- Google Sheets config ---
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID       = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME           = "Webscraper Tool for Procurement Sites Two"

# --- NAICS keyword mapping ---
# Checked against project title + scope description. First match wins.
NAICS_FILTERS = [
    ("238210", "Electrical (NAICS 238210)", [
        "electrical", "electric", "lighting", "wiring", "power",
        "generator", "amag", "access control", "security system",
        "distributed antenna", "antenna",
    ]),
    ("238220", "Plumbing/HVAC (NAICS 238220)", [
        "plumbing", "hvac", "mechanical", "heating", "cooling",
        "air handling", "ahu", "chiller", "boiler", "ventilation",
        "air conditioning", "cleanroom", "clean room", "pharmacy cleanroom",
    ]),
    ("238120", "Structural Steel (NAICS 238120)", [
        "structural steel", "steel erection", "steel fabricat",
        "iron work", "metal building", "guardrail", "railing",
        "staircase", "stair", "tread",
    ]),
    ("237310", "Highway/Street/Bridge (NAICS 237310)", [
        "parking lot", "parking ramp", "surface lot", "ramp maintenance",
        "asphalt", "concrete surface", "pavement", "road", "street",
        "sidewalk", "bridge", "track", "field complex", "courtyard",
        "landscape", "hardscape", "exterior", "plaza",
    ]),
    ("236220", "Commercial Building (NAICS 236220)", [
        "renovate", "renovation", "remodel", "modernize", "modernization",
        "repair", "replace", "addition", "construct", "lab", "laboratory",
        "office", "restroom", "residence", "hall", "pavilion", "building",
        "health care", "healthcare", "clinic", "hospital", "dock",
        "elevator", "floor", "room", "suite", "classroom", "stadium",
        "press box", "canopy",
    ]),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MIN_BID_YEAR = 2026  # only capture bids awarded in this calendar year or later

def parse_amount(amount_str):
    """Convert '$1,234,567.89' → float. Returns 0.0 on failure."""
    if not amount_str:
        return 0.0
    cleaned = amount_str.replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def bid_year(date_str):
    """
    Parse bid date string (M/D/YY or M/D/YYYY) and return the 4-digit year.
    Returns 0 on failure.
    """
    if not date_str:
        return 0
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return time.strptime(date_str.strip(), fmt).tm_year
        except ValueError:
            continue
    return 0


def map_naics(title, scope=""):
    """
    Map project title + scope to (naics_code, naics_label) via keyword matching.
    Falls back to 236220 Commercial Building if no match found.
    """
    combined = (title + " " + scope).lower()
    for naics_code, naics_label, keywords in NAICS_FILTERS:
        for kw in keywords:
            if kw in combined:
                return naics_code, naics_label
    return "236220", "Commercial Building (NAICS 236220)"


def extract_pm_info(detail_text):
    """
    Parse Project Manager name and email from detail page text.
    The format is: "Project Manager:   Robert A Winters robert-winters@uiowa.edu"
    Returns (name, email) or ("", "").
    """
    # Find the Project Manager line
    pm_match = re.search(r'Project Manager:\s*(.+)', detail_text)
    if not pm_match:
        return "", ""

    pm_line = pm_match.group(1).strip()

    # Extract email
    email_match = re.search(r'[\w.\-]+@[\w.\-]+\.[a-z]{2,}', pm_line)
    email = email_match.group(0) if email_match else ""

    # Name is the line minus the email
    name = pm_line.replace(email, "").strip()

    return name, email


def extract_scope(detail_text):
    """Extract the Scope paragraph from detail page text."""
    scope_match = re.search(r'Scope:\s*(.+?)(?:\n[A-Z]|\Z)', detail_text, re.DOTALL)
    if scope_match:
        return scope_match.group(1).strip().replace("\n", " ")
    return ""


def extract_dates(detail_text):
    """Extract Construction Start and Finish from detail page text."""
    start_match  = re.search(r'Construction Start:\s*([\d/]+)', detail_text)
    finish_match = re.search(r'Construction Finish:\s*([\d/]+)', detail_text)
    start  = start_match.group(1)  if start_match  else ""
    finish = finish_match.group(1) if finish_match else ""
    return start, finish


def extract_building(detail_text):
    """Extract the Building field from detail page text."""
    m = re.search(r'Building:\s*(.+)', detail_text)
    return m.group(1).strip() if m else ""


# ---------------------------------------------------------------------------
# Scraping logic
# ---------------------------------------------------------------------------

def scrape_bids_table(page):
    """
    Load the bids page and return all table rows as dicts.
    Each row: projectNum, title, bidDate, awardedTo, estimate, projectUrl
    """
    print(f"Loading bids page: {BIDS_URL}")
    page.goto(BIDS_URL, timeout=60000)
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1500)

    rows = page.evaluate("""
        () => {
            const trs = Array.from(document.querySelectorAll('table tbody tr'));
            return trs.map(tr => {
                const cells = Array.from(tr.querySelectorAll('td'));
                const projLink = tr.querySelector('a[href*="project-info"]');
                return {
                    projectNum:  cells[0]?.innerText?.trim() || '',
                    title:       cells[1]?.innerText?.trim() || '',
                    bidDate:     cells[3]?.innerText?.trim() || '',
                    awardedTo:   cells[4]?.innerText?.trim() || '',
                    estimate:    cells[5]?.innerText?.trim() || '',
                    projectUrl:  projLink?.href || '',
                };
            });
        }
    """)

    print(f"  Found {len(rows)} total rows in table")
    return rows


def fetch_project_detail(page, proj_num):
    """
    Navigate to the project-info page and return relevant fields.
    Returns dict with: scope, building, pm_name, pm_email, start_date, finish_date
    """
    url = DETAIL_URL.format(proj=proj_num)
    try:
        page.goto(url, timeout=30000)
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(800)
        text = page.locator("body").inner_text()
        scope           = extract_scope(text)
        building        = extract_building(text)
        pm_name, pm_email = extract_pm_info(text)
        start, finish   = extract_dates(text)
        return {
            "scope":      scope,
            "building":   building,
            "pm_name":    pm_name,
            "pm_email":   pm_email,
            "start_date": start,
            "finish_date": finish,
        }
    except Exception as e:
        print(f"  [WARN] Could not fetch detail for {proj_num}: {e}")
        return {"scope": "", "building": "", "pm_name": "", "pm_email": "",
                "start_date": "", "finish_date": ""}


def scrape_all():
    """Main scrape flow. Returns list of result dicts ready for Google Sheets."""
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()
        page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
        page.set_viewport_size({"width": 1280, "height": 900})

        try:
            rows = scrape_bids_table(page)

            # Filter: awarded + amount > MIN_AMOUNT + bid year >= MIN_BID_YEAR
            awarded = [
                r for r in rows
                if r["awardedTo"]
                and parse_amount(r["estimate"]) > MIN_AMOUNT
                and bid_year(r["bidDate"]) >= MIN_BID_YEAR
            ]
            print(f"  Awarded + > ${MIN_AMOUNT:,} + year >= {MIN_BID_YEAR}: {len(awarded)} contracts")

            for i, row in enumerate(awarded, 1):
                proj_num  = row["projectNum"]
                title     = row["title"]
                vendor    = row["awardedTo"]
                estimate  = row["estimate"]
                bid_date  = row["bidDate"]
                proj_url  = row["projectUrl"] or DETAIL_URL.format(proj=proj_num)

                print(f"  [{i}/{len(awarded)}] {proj_num} — {vendor} ({estimate})")

                detail = fetch_project_detail(page, proj_num)
                time.sleep(REQUEST_DELAY)

                naics_code, naics_label = map_naics(title, detail["scope"])

                # Build description
                parts = []
                if detail["building"]:
                    parts.append(f"Building: {detail['building']}")
                if detail["scope"]:
                    parts.append(detail["scope"])
                if detail["start_date"] or detail["finish_date"]:
                    parts.append(f"Construction: {detail['start_date']} – {detail['finish_date']}")
                description = " | ".join(parts)

                results.append({
                    "city":            "Iowa City, IA",
                    "company_name":    vendor.strip(),
                    "contact_name":    detail["pm_name"],
                    "phone":           "",
                    "email":           detail["pm_email"],
                    "address":         "",
                    "website":         "",
                    "contract_name":   f"{proj_num} — {title}".strip(" —"),
                    "award_amount":    estimate,
                    "amount_expended": "",
                    "begin_date":      bid_date,
                    "award_link":      proj_url,
                    "description":     description,
                    "commodity_type":  naics_label,
                })

        finally:
            browser.close()

    return results


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------

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


def write_to_google_sheets(results):
    """Append new results to Google Sheets. Never clears existing data."""
    service = get_sheets_service()
    sheet   = service.spreadsheets()

    # Ensure sheet tab exists
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
        print(f"Created new sheet tab: {SHEET_NAME}")

    # Deduplicate by (company_name, contract_name) — indices 1 and 7
    existing  = get_existing_data(service)
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
    skipped = len(results) - len(new_results)
    if skipped:
        print(f"  Skipped {skipped} already-existing entries")
    results = new_results

    # Write headers if sheet is empty
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

    print(f"Wrote {len(results)} new rows to Google Sheet: {SHEET_NAME}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    results = scrape_all()
    if not results:
        print("\nNo awarded contracts matched the filters.")
        sys.exit(0)
    write_to_google_sheets(results)


if __name__ == "__main__":
    main()
