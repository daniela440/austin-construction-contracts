"""
OSHA Enforcement Data Scraper
Pulls inspection and violation data for construction-related NAICS codes
directly from the OSHA IMIS search interface (osha.gov/ords/imis).

No API key needed. Data is public.

Target NAICS codes:
  238210 - Electrical Contractors
  236220 - Commercial/Industrial Building Construction
  237310 - Highway, Street, Bridge Construction
  238220 - Plumbing/HVAC Contractors
  238120 - Structural Steel Erection

Writes to the "OSHA company audits" sheet in the main Google Sheet.
Rows with violations > 0 are included; sorted by violations descending.
"""

import re
import ssl
import sys
import time
import html as html_lib
from datetime import datetime

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Config ---
OSHA_BASE = "https://www.osha.gov/ords/imis"

# Construction NAICS codes to scrape
TARGET_NAICS = [
    ("238210", "Electrical Contractors"),
    ("236220", "Commercial Building Construction"),
    ("237310", "Highway/Street/Bridge Construction"),
    ("238220", "Plumbing/HVAC Contractors"),
    ("238120", "Structural Steel Erection"),
]

# Inspections per page
PAGE_SIZE = 20
REQUEST_DELAY = 1.0  # seconds between pages

# Whether to fetch detail pages for city/address (slower but richer data)
FETCH_DETAILS = True
DETAIL_DELAY = 0.5  # seconds between detail page fetches

# Google Sheets config
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "OSHA company audits"


# --- Scraping ---

HEADERS = {"User-Agent": "OSHA-Research-Scraper/1.0"}


def search_osha_page(naics, start_month, start_day, start_year,
                     end_month, end_day, end_year, page_offset=0):
    """Fetch one page of OSHA industry search results."""
    params = {
        "naics": naics,
        "State": "",
        "officetype": "",
        "Office": "",
        "startmonth": str(start_month).zfill(2),
        "startday": str(start_day).zfill(2),
        "startyear": str(start_year),
        "endmonth": str(end_month).zfill(2),
        "endday": str(end_day).zfill(2),
        "endyear": str(end_year),
        "owner": "A",  # Private employers only
        "scope": "",
        "FedAgnCode": "",
        "p_start": "",
        "p_finish": str(page_offset + PAGE_SIZE),
        "p_sort": "",
        "p_desc": "DESC",
        "p_direction": "Next" if page_offset > 0 else "",
        "p_show": str(PAGE_SIZE),
    }
    resp = requests.get(
        f"{OSHA_BASE}/industry.search",
        params=params,
        headers=HEADERS,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.text


def parse_search_results(html):
    """Parse inspection rows from an OSHA search results page."""
    results = []

    # Find the results table (second table in the page)
    tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)
    if len(tables) < 2:
        return results, False  # (results, has_more)

    table = tables[1]
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table, re.DOTALL | re.IGNORECASE)

    for row in rows[1:]:  # Skip header row
        cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, re.DOTALL | re.IGNORECASE)
        if len(cells) < 12:
            continue

        def cell(i):
            return html_lib.unescape(
                re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', cells[i])).strip()
            )

        num = cell(1)
        activity_nr = cell(2)
        date_opened = cell(3)
        naics_code = cell(9)
        violations_raw = cell(10)
        estab_name = cell(11)
        insp_type = cell(6)
        insp_scope = cell(7)
        state = cell(5)

        # Extract link from 3rd cell (activity number link)
        link_match = re.search(r'href="([^"]*inspection_detail[^"]*)"', cells[2], re.IGNORECASE)
        detail_link = f"{OSHA_BASE}/{link_match.group(1)}" if link_match else ""

        # Parse violations
        try:
            violations = int(violations_raw)
        except (ValueError, TypeError):
            violations = 0

        results.append({
            "activity_nr": activity_nr,
            "estab_name": estab_name,
            "naics_code": naics_code,
            "date_opened": date_opened,
            "insp_type": insp_type,
            "insp_scope": insp_scope,
            "state": state,
            "violations": violations,
            "detail_link": detail_link,
            # To be filled by detail page fetch:
            "city": "",
            "address": "",
            "initial_penalty": 0.0,
            "current_penalty": 0.0,
            "serious": 0,
            "willful": 0,
            "repeat": 0,
        })

    # Check if there's a "Next" page link
    has_more = bool(re.search(r'p_direction=Next', html, re.IGNORECASE))
    return results, has_more


def fetch_inspection_detail(detail_url):
    """Fetch detail page and extract city, address, and penalty data."""
    try:
        resp = requests.get(detail_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        print(f"      Detail fetch error: {e}")
        return {}

    def extract(pattern):
        m = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
        if not m:
            return ""
        return html_lib.unescape(re.sub(r'<[^>]+>', '', m.group(1)).strip())

    # Parse city and street from the Site Address block:
    # <p><strong>Site Address</strong>: <br> Name<br> Street<br>City, ST Zip</p>
    city = ""
    address = ""
    addr_match = re.search(
        r'<strong>Site Address</strong>\s*:.*?<br>(.*?)</p>',
        html, re.DOTALL | re.IGNORECASE
    )
    if addr_match:
        lines = [
            html_lib.unescape(re.sub(r'<[^>]+>', '', part).strip())
            for part in re.split(r'<br\s*/?>', addr_match.group(1), flags=re.IGNORECASE)
        ]
        lines = [l for l in lines if l]
        if lines:
            city_state_zip = lines[-1]  # e.g. "Bloomington, IN 47403"
            city_m = re.match(r'^(.+?),\s*[A-Z]{2}\s+\d{5}', city_state_zip)
            city = city_m.group(1) if city_m else city_state_zip
            if len(lines) >= 2:
                address = lines[-2]  # street line (skip company name at lines[0])

    # Extract th->td pairs for penalty data
    data = {}
    pairs = re.findall(
        r'<th[^>]*>\s*(.*?)\s*</th>\s*<td[^>]*>(.*?)</td>',
        html, re.DOTALL | re.IGNORECASE
    )
    for k, v in pairs:
        key = re.sub(r'<[^>]+>', '', k).strip().lower().replace(' ', '_').rstrip(':')
        val = html_lib.unescape(re.sub(r'<[^>]+>', '', v).strip())
        if key and val and val != '&nbsp;':
            data[key] = val

    def parse_money(s):
        s = re.sub(r'[,$]', '', s or "0")
        try:
            return float(s)
        except ValueError:
            return 0.0

    # Count violation types from citation table
    viol_types = re.findall(r'<td[^>]*>\s*(Serious|Willful|Repeat|Other)\s*</td>', html, re.IGNORECASE)
    serious = sum(1 for v in viol_types if v.lower() == 'serious')
    willful = sum(1 for v in viol_types if v.lower() == 'willful')
    repeat = sum(1 for v in viol_types if v.lower() == 'repeat')

    return {
        "city": city,
        "address": address,
        "initial_penalty": parse_money(data.get("initial_penalty", "0")),
        "current_penalty": parse_money(data.get("current_penalty", "0")),
        "serious": serious,
        "willful": willful,
        "repeat": repeat,
    }


def scrape_naics(naics_code, naics_label, year, min_violations=0):
    """Scrape all inspections for one NAICS code for a single year."""
    all_inspections = []
    print(f"  [{naics_code}] {year}...", end="", flush=True)
    page_offset = 0

    while True:
        # Retry up to 3 times with backoff on network errors
        html = None
        for attempt in range(3):
            try:
                html = search_osha_page(
                    naics=naics_code,
                    start_month=1, start_day=1, start_year=year,
                    end_month=12, end_day=31, end_year=year,
                    page_offset=page_offset,
                )
                break
            except Exception as e:
                if attempt < 2:
                    wait = (attempt + 1) * 5
                    print(f" [retry {attempt+1} in {wait}s]", end="", flush=True)
                    time.sleep(wait)
                else:
                    print(f" ERROR: {e}")
        if html is None:
            break

        rows, has_more = parse_search_results(html)

        # OSHA always returns has_more=True even on empty pages — stop on empty page
        if not rows:
            break

        if min_violations > 0:
            rows = [r for r in rows if r["violations"] >= min_violations]

        all_inspections.extend(rows)
        time.sleep(REQUEST_DELAY)

        if not has_more:
            break
        page_offset += PAGE_SIZE

    label = "all inspections" if min_violations == 0 else "with violations"
    print(f" {len(all_inspections)} {label}.")
    return all_inspections


# --- Detail page enrichment ---

def enrich_with_details(inspections):
    """Fetch inspection detail pages to add city, address, penalty data."""
    total = len(inspections)
    print(f"\nFetching detail pages for {total} inspections...")

    for i, insp in enumerate(inspections, 1):
        if not insp.get("detail_link"):
            continue

        print(f"  [{i}/{total}] {insp['estab_name'][:50]}...", end="", flush=True)
        detail = fetch_inspection_detail(insp["detail_link"])
        insp.update(detail)
        print(f" city={detail.get('city', '?')} penalty=${detail.get('current_penalty', 0):,.0f}")
        time.sleep(DETAIL_DELAY)

    return inspections


# --- Google Sheets ---

SHEET_FIELDS = [
    "estab_name", "city", "state", "address",
    "naics_code", "naics_label",
    "date_opened", "insp_type", "insp_scope",
    "violations", "serious", "willful", "repeat",
    "initial_penalty", "current_penalty",
    "priority", "detail_link",
]

SHEET_HEADERS = [
    "Company", "City", "State", "Address",
    "NAICS Code", "Industry",
    "Inspection Date", "Inspection Type", "Scope",
    "Total Violations", "Serious", "Willful", "Repeat",
    "Initial Penalty ($)", "Current Penalty ($)",
    "Priority", "OSHA Link",
]


def compute_priority(r):
    if r.get("willful", 0) > 0 or r.get("repeat", 0) > 0:
        return "High"
    elif r.get("serious", 0) > 0 or r.get("violations", 0) >= 5:
        return "Medium"
    elif r.get("violations", 0) > 0:
        return "Low"
    return ""


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def ensure_sheet_exists(service):
    spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    for s in spreadsheet.get("sheets", []):
        if s["properties"]["title"] == SHEET_NAME:
            return
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]},
    ).execute()
    print(f"Created sheet tab: {SHEET_NAME}")


LEGEND_ROWS = [
    [],
    ["--- INSPECTION TYPE GUIDE ---"],
    ["Inspection Type", "What it means", "", "Why it matters for safety training"],
    ["Planned",
     "Scheduled inspection from OSHA's annual programmed list. "
     "OSHA targets industries with high injury rates.",
     "",
     "Company was flagged as high-risk in its industry. Good lead — likely ongoing safety gaps."],
    ["Complaint",
     "A worker or union filed a formal complaint about unsafe conditions. "
     "OSHA is required to investigate.",
     "",
     "Active hazard reported by employees. High urgency — workers are at risk right now."],
    ["Referral",
     "Another agency (e.g. fire dept, insurance, police) or a different OSHA office "
     "referred this company for inspection.",
     "",
     "Third-party flagged a safety problem. Strong indicator of systemic issues."],
    ["Fat/Cat",
     "Fatality or Catastrophe — triggered by a worker death or 3+ workers hospitalized. "
     "OSHA must investigate within 8 hours.",
     "",
     "Highest priority lead. A worker died or was seriously injured. Company urgently needs training."],
    ["Accident",
     "Inspection opened after a workplace accident was reported to OSHA.",
     "",
     "Recent incident already happened. Company is reactive — proactive training pitch is timely."],
    ["Prog Related",
     "Programmed Related — offshoot of a planned inspection, e.g. a related worksite nearby.",
     "",
     "Part of a broader enforcement sweep in the area or industry."],
    ["Unprog Rel",
     "Unprogrammed Related — unplanned inspection connected to another open case or event.",
     "",
     "Often triggered by an accident or complaint at a nearby site."],
    ["Referral (Fed)",
     "Referral from a federal agency (e.g. EPA, Army Corps of Engineers).",
     "",
     "Multi-agency concern — usually larger projects with complex safety requirements."],
    ["FollowUp",
     "Return visit to verify the company corrected violations found in a prior inspection.",
     "",
     "Company had prior violations and may not have fully fixed them. Persistent risk."],
    ["Monitoring",
     "Ongoing monitoring inspection, often for companies under a settlement agreement.",
     "",
     "Company is under active OSHA oversight. Very receptive to compliance help."],
    [],
    ["--- PRIORITY GUIDE ---"],
    ["Priority", "Meaning"],
    ["High",
     "Willful or Repeat violations. OSHA found the company knowingly broke safety rules "
     "OR committed the same violation more than once. Highest fines. Best training leads."],
    ["Medium",
     "Serious violations OR 5+ total violations. Worker was exposed to a hazard that could "
     "cause death or serious injury. Company likely needs structured safety training."],
    ["Low",
     "Other violations (e.g. paperwork, minor hazards). Still a valid lead but lower urgency."],
    [],
    ["--- SCOPE GUIDE ---"],
    ["Scope", "Meaning"],
    ["Complete", "OSHA inspected the entire worksite."],
    ["Partial", "Only part of the worksite was inspected — more violations may exist."],
    ["Records", "OSHA only reviewed paperwork/injury logs, did not walk the site."],
    ["No Inspection", "OSHA visited but could not conduct an inspection (e.g. company closed, out of business)."],
]


def write_to_google_sheets(results_2026, results_2025):
    service = get_sheets_service()
    ensure_sheet_exists(service)
    sheet = service.spreadsheets()

    section_2026 = [
        ["=== SECTION 1: 2026 — Fresh Leads (Currently Under Investigation) ==="],
        ["These companies were inspected in 2026. Citations have not been issued yet "
         "(OSHA takes 6-12 weeks to process). High-value outreach window — "
         "contact them before the fine arrives."],
        [],
        SHEET_HEADERS,
    ] + [[r.get(f, "") for f in SHEET_FIELDS] for r in results_2026]

    section_2025 = [
        [],
        ["=== SECTION 2: 2025 — Confirmed Violations (Citations Issued) ==="],
        ["These companies received formal OSHA citations in 2025. "
         "Sorted by number of violations. Priority column shows severity."],
        [],
        SHEET_HEADERS,
    ] + [[r.get(f, "") for f in SHEET_FIELDS] for r in results_2025]

    all_rows = section_2026 + section_2025 + LEGEND_ROWS

    sheet.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A1:Z10000",
        body={},
    ).execute()

    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A1",
        valueInputOption="RAW",
        body={"values": all_rows},
    ).execute()

    print(f"Wrote {len(results_2026)} rows (2026) + {len(results_2025)} rows (2025) + legend to '{SHEET_NAME}'")


def preview_results(results, limit=10):
    print(f"\n--- Top {min(limit, len(results))} results (by violations) ---\n")
    for r in results[:limit]:
        print(f"  {r['estab_name']}")
        loc = ", ".join(filter(None, [r.get("city"), r.get("state")]))
        print(f"  {loc} | NAICS: {r['naics_code']} ({r.get('naics_label', '')})")
        print(f"  Date: {r['date_opened']} | Type: {r['insp_type']}")
        print(f"  Violations: {r['violations']} total "
              f"({r.get('serious', 0)} serious, {r.get('willful', 0)} willful, {r.get('repeat', 0)} repeat)")
        penalty = r.get("current_penalty", 0)
        if penalty:
            print(f"  Penalty: ${penalty:,.2f}")
        print(f"  Priority: {r.get('priority', '?')}")
        print()


def collect_and_process(year, min_violations, fetch_details, label):
    """Scrape all NAICS codes for one year, enrich, deduplicate, sort."""
    print(f"\n{'='*60}")
    print(f"SECTION: {label}")
    print(f"{'='*60}")

    all_inspections = []
    for naics_code, naics_label in TARGET_NAICS:
        rows = scrape_naics(naics_code, naics_label, year, min_violations)
        for r in rows:
            r["naics_label"] = naics_label
        all_inspections.extend(rows)

    print(f"\n  {label} total: {len(all_inspections)} inspections")

    if not all_inspections:
        return []

    if fetch_details:
        all_inspections = enrich_with_details(all_inspections)

    for r in all_inspections:
        r["priority"] = compute_priority(r)

    # Deduplicate
    seen = set()
    unique = []
    for r in all_inspections:
        key = r["activity_nr"]
        if key not in seen:
            seen.add(key)
            unique.append(r)

    # Sort: by date desc for 2026 (newest first), by violations desc for 2025
    if min_violations == 0:
        unique.sort(key=lambda r: r.get("date_opened", ""), reverse=True)
    else:
        unique.sort(
            key=lambda r: (r.get("violations", 0), r.get("current_penalty", 0)),
            reverse=True,
        )

    return unique


def main():
    import argparse
    parser = argparse.ArgumentParser(description="OSHA Construction Inspection Scraper")
    parser.add_argument("--preview", action="store_true",
                        help="Preview results without writing to sheet")
    parser.add_argument("--no-details", action="store_true",
                        help="Skip detail page fetches (faster, no city/address/penalty)")
    args = parser.parse_args()

    fetch_details = FETCH_DETAILS and not args.no_details

    # --- Section A: 2026 — all inspections, no violations filter ---
    results_2026 = collect_and_process(
        year=2026,
        min_violations=0,
        fetch_details=fetch_details,  # fetch address; penalty/violations won't exist yet for 2026
        label="2026 Fresh Leads (all inspections, citations pending)",
    )

    # --- Section C: 2025 — only inspections with confirmed violations ---
    results_2025 = collect_and_process(
        year=2025,
        min_violations=1,
        fetch_details=fetch_details,
        label="2025 Confirmed Violations (citations issued)",
    )

    # Summary
    print(f"\n\n{'='*60}")
    print(f"TOTALS")
    print(f"{'='*60}")
    print(f"  2026 fresh leads:            {len(results_2026)}")
    print(f"  2025 with violations:        {len(results_2025)}")
    print(f"  2025 high priority:          {sum(1 for r in results_2025 if r.get('priority') == 'High')}")
    print(f"  2025 total penalties:        ${sum(r.get('current_penalty', 0) for r in results_2025):,.0f}")

    if results_2026:
        preview_results(results_2026, limit=5)
    if results_2025:
        preview_results(results_2025, limit=5)

    if args.preview:
        print(f"\nPreview mode — run without --preview to write to sheet.")
    else:
        write_to_google_sheets(results_2026, results_2025)
        print(f"\nView: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
