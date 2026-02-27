"""
Florida FDOT Active Construction Contracts Scraper
Pulls awarded contract data from the FDOT ArcGIS FeatureServer.
Source: https://data.fdot.gov/road/projects/
API:    https://gis.fdot.gov/arcgis/rest/services/Active_Construction_Projects/FeatureServer/1

Filters:
  - StartDate >= 2026-01-01
  - Cost > $250,000
  - Description keyword match to target NAICS codes:
      237310 - Highway, Street, and Bridge Construction
      238210 - Electrical Contractors
      236220 - Commercial and Institutional Building Construction
      238220 - Plumbing, Heating, and Air-Conditioning Contractors
      238120 - Structural Steel and Precast Concrete Contractors

No NAICS codes are published natively — inferred from description keywords.
"""

import json
import subprocess
import sys
import urllib.parse
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Config ---
FEATURE_SERVER_URL = (
    "https://gis.fdot.gov/arcgis/rest/services/Active_Construction_Projects/FeatureServer/1"
)
MIN_AMOUNT = 250_000
START_DATE = "2026-01-01"

# Google Sheets config
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

# Source URL for links
SOURCE_URL = "https://data.fdot.gov/road/projects/"

# NAICS keyword mapping
# NOTE: FDOT descriptions are truncated at ~50 chars with spaces inserted mid-word
# (e.g., "RESUR FACING", "SA FETY", "OVERPAS S"). Keywords account for both split
# and unsplit forms.
NAICS_KEYWORDS = {
    "237310": [
        "highway", "road", "bridge", "street", "traffic",
        "pavement", "pave", "surfacing", "resurfac", "resur ",
        "paving", "grading", "excavat", "interchange", "intersection",
        "sidewalk", "guardrail", "median", "widening", "reconstruction",
        "milling", "overlay", "asphalt", "roadway", "xway",
        "expressway", "turnpike", "signing", "rumble", "turn lane",
        "lane(s)", "departure", "fety project", "overpas",
        "pvmnt", "repair", "emergen", "lanes", "ramp", "corridor",
    ],
    "238210": [
        "electric", "electrical", "power", "lighting", "wiring",
        "solar", "generator", "its ", "fiber", "communication", "illuminat",
    ],
    "236220": [
        "building", "facility", "facilities", "terminal", "station",
        "office", "rest area", "welcome center", "maintenance yard",
        "park & ride", "park and ride", "administration", "headquarters",
    ],
    "238220": [
        "plumbing", "hvac", "heating", "cooling", "mechanical",
        "air condition", "ventilation", "stormwater", "sewer", "drainage",
    ],
    "238120": [
        "structural steel", "precast", "retaining wall",
        "sound wall", "bridge deck", "steel erect",
    ],
}

NAICS_LABELS = {
    "237310": "Highway, Street, and Bridge Construction",
    "238210": "Electrical Contractors",
    "236220": "Commercial and Institutional Building Construction",
    "238220": "Plumbing, Heating, and Air-Conditioning Contractors",
    "238120": "Structural Steel and Precast Concrete Contractors",
}


def curl_get(url):
    """Fetch URL via curl subprocess (avoids macOS SSL cert issues)."""
    result = subprocess.run(
        [
            "curl", "-s", "-L",
            "-A", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "-H", "Accept: application/json",
            url,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result.stdout


def fetch_contracts():
    """Query FDOT ArcGIS FeatureServer for 2026+ contracts > $250K."""
    # ArcGIS timestamp filter requires epoch milliseconds or ISO-like format
    params = {
        "where": f"StartDate >= timestamp '{START_DATE} 00:00:00' AND Cost > {MIN_AMOUNT}",
        "outFields": "ContractId,Description,StartDate,EstEndDate,Cost,Vendor,District,County,Website,FinProjNum",
        "returnGeometry": "false",
        "resultRecordCount": 2000,
        "orderByFields": "StartDate DESC",
        "f": "json",
    }
    url = f"{FEATURE_SERVER_URL}/query?{urllib.parse.urlencode(params)}"
    print(f"Querying FDOT FeatureServer...")
    raw = curl_get(url)
    data = json.loads(raw)
    if "error" in data:
        print(f"API error: {data['error']}")
        sys.exit(1)
    return data.get("features", [])


def infer_naics(description):
    """Return the first matching NAICS code based on description keywords."""
    desc_lower = (description or "").lower()
    for naics, keywords in NAICS_KEYWORDS.items():
        if any(kw in desc_lower for kw in keywords):
            return naics
    return None


def parse_timestamp(ts):
    """Convert ArcGIS epoch milliseconds to YYYY-MM-DD string."""
    if ts is None:
        return ""
    return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")



def scrape_all():
    """Main pipeline: fetch -> filter NAICS -> deduplicate -> return results."""
    features = fetch_contracts()
    print(f"API returned {len(features)} features")

    seen = set()
    matched = []
    skipped_naics = 0

    for feat in features:
        a = feat.get("attributes", {})
        contract_id = (a.get("ContractId") or "").strip()
        description = (a.get("Description") or "").strip()
        vendor = (a.get("Vendor") or "").strip()
        cost = a.get("Cost") or 0
        start_date = parse_timestamp(a.get("StartDate"))
        end_date = parse_timestamp(a.get("EstEndDate"))
        district = (a.get("District") or "").strip()
        county = (a.get("County") or "").strip()
        fin_proj = (a.get("FinProjNum") or "").strip()

        # Infer NAICS from description
        naics = infer_naics(description)
        if not naics:
            skipped_naics += 1
            continue

        # Deduplicate by contract_id
        if contract_id in seen:
            continue
        seen.add(contract_id)

        location = f"Florida - {county} County, District {district}".strip(" -,")
        link = SOURCE_URL

        matched.append({
            "city": location,
            "company_name": vendor,
            "contact_name": "",
            "phone": "",
            "email": "",
            "address": "",
            "website": "",
            "contract_name": f"FDOT Contract {contract_id}: {description}",
            "award_amount": cost,
            "amount_expended": 0.0,
            "begin_date": start_date,
            "award_link": link,
            "description": (
                f"NAICS {naics} ({NAICS_LABELS[naics]}) | "
                f"Contract {contract_id} | "
                f"{description} | "
                f"Est End: {end_date} | "
                f"Fin Proj: {fin_proj}"
            ).strip(" |"),
            "commodity_type": NAICS_LABELS[naics],
        })

    print(f"Matched (NAICS keyword filter): {len(matched)}")
    print(f"Skipped (no NAICS match): {skipped_naics}")

    # NAICS breakdown
    naics_counts = {}
    for r in matched:
        ct = r["commodity_type"]
        naics_counts[ct] = naics_counts.get(ct, 0) + 1
    for ct, cnt in sorted(naics_counts.items()):
        print(f"  {ct}: {cnt}")

    return matched


# Sheet field mapping (matches other scrapers)
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
        sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]},
        ).execute()
        print(f"Created new sheet: {SHEET_NAME}")

    # APPEND ONLY — always add to existing data, never clear the sheet.
    # Deduplicates by (company_name, contract_name) to avoid double-writing.
    existing = get_existing_data(service)
    start_row = len(existing) + 1

    existing_fps = {
        (row[1].strip().lower(), row[7].strip().lower())
        for row in (existing[1:] if len(existing) > 1 else [])
        if len(row) > 7
    }
    new_results = [
        r for r in results
        if (
            r.get("company_name", "").strip().lower(),
            r.get("contract_name", "").strip().lower(),
        ) not in existing_fps
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

    print(f"Wrote {len(results)} rows to Google Sheet: {SHEET_NAME}")


def preview_results(results, limit=10):
    print(f"\n--- Preview (first {min(limit, len(results))} results) ---\n")
    for i, r in enumerate(results[:limit], 1):
        print(f"{i}. {r['company_name']}")
        print(f"   Contract: {r['contract_name'][:80]}")
        print(f"   Amount:  ${r['award_amount']:,.0f}")
        print(f"   Date:    {r['begin_date']}")
        print(f"   Type:    {r['commodity_type']}")
        print()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Florida FDOT Contracts Scraper")
    parser.add_argument("--preview", action="store_true", help="Preview without writing to sheet")
    args = parser.parse_args()

    results = scrape_all()

    if not results:
        print("\nNo contracts matched all filters.")
        sys.exit(0)

    preview_results(results)

    if args.preview:
        print("Preview mode — not writing to Google Sheets")
        print(f"Run without --preview to write {len(results)} rows")
    else:
        write_to_google_sheets(results)
        print(f"\nView results at: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
