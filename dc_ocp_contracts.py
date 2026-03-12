"""
DC Office of Contracting and Procurement — Construction Contracts Scraper
Fetches active construction contracts via the OCP JSON API (no browser needed).

Filters applied:
  - Award date >= Jan 1, 2026 (calendar year 2026)
  - Contract amount > $250,000
  - NIGP code prefix in (909, 910, 912, 913, 914) — DC's construction categories

NIGP → NAICS mapping (DC uses NIGP codes, not NAICS):
  909 / 910 / 912  →  236220  Commercial Building Construction
  913              →  237310  Highway / Street / Bridge Construction
  914              →  keyword-matched to 238xxx trade codes
  All codes also checked against title keywords for 238210/238220/238120.

Portal: https://contracts.ocp.dc.gov/contracts/search
API:    POST https://contracts.ocp.dc.gov/api/contracts/search
Detail: GET  https://contracts.ocp.dc.gov/api/contracts/details?id=<base64_id>
"""

import json
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime

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
SEARCH_URL   = "https://contracts.ocp.dc.gov/api/contracts/search"
DETAIL_BASE  = "https://contracts.ocp.dc.gov/api/contracts/details"
PORTAL_BASE  = "https://contracts.ocp.dc.gov/contracts/details"

MIN_AWARD_DATE = datetime(2026, 1, 1)
MIN_AMOUNT     = 250_000

# NIGP class prefixes that represent construction work
CONSTRUCTION_NIGP_PREFIXES = ("909", "910", "912", "913", "914")

REQUEST_DELAY = 1.0  # seconds between detail API calls

# --- Google Sheets config ---
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID       = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME           = "Webscraper Tool for Procurement Sites Two"

# --- NAICS keyword mapping ---
# Checked against contract title + NIGP code description.
# Order matters: first match wins.
NAICS_FILTERS = [
    ("238210", "Electrical (NAICS 238210)", [
        "electrical", "electric", "lighting", "wiring",
        "power distribution", "distributed antenna", "antenna system",
    ]),
    ("238220", "Plumbing/HVAC (NAICS 238220)", [
        "plumbing", "hvac", "mechanical", "heating", "cooling",
        "air conditioning", "chilled water", "boiler",
        "sustainable energy", "energy utility",
    ]),
    ("238120", "Structural Steel (NAICS 238120)", [
        "structural steel", "steel erection", "steel fabricat",
        "iron work", "metal building", "guardrail",
        "impact attenuator", "railing",
    ]),
    ("237310", "Highway/Street/Bridge (NAICS 237310)", [
        "highway", "street", "road", "bridge", "intersection",
        "sidewalk", "pavement", "asphalt", "roundabout", "traffic signal",
        "parking lot", "streetscape", "corridor", "plug",
        "stormwater", "tunnel", "gsi maintenance", "bmp",
        "restoration", "rehabilitation", "reconstruction",
        "avenue", "lane", "curb",
    ]),
    ("236220", "Commercial Building (NAICS 236220)", [
        "building construction", "commercial", "institutional",
        "facility", "facilities", "renovation", "remodel",
        "construction services, general", "nuisance abatement",
        "abatement", "boarded", "housing", "home",
    ]),
]

# Fallback: map NIGP class prefix → NAICS when no keyword matches
NIGP_NAICS_FALLBACK = {
    "909": ("236220", "Commercial Building (NAICS 236220)"),
    "910": ("236220", "Commercial Building (NAICS 236220)"),
    "912": ("236220", "Commercial Building (NAICS 236220)"),
    "913": ("237310", "Highway/Street/Bridge (NAICS 237310)"),
    "914": ("236220", "Commercial Building (NAICS 236220)"),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def api_post(url, payload):
    """POST JSON payload and return parsed response."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; DC-OCP-Scraper/1.0)",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def api_get(url):
    """GET JSON from url and return parsed response, or None on error."""
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; DC-OCP-Scraper/1.0)",
        },
    )
    try:
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  [WARN] GET {url} failed: {e}")
        return None


def parse_amount(amount_str):
    """Convert '$1,234,567.89' → float. Returns 0.0 on failure."""
    if not amount_str:
        return 0.0
    cleaned = amount_str.replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_date(date_str):
    """Convert 'M/D/YYYY' → datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except ValueError:
        return None


def is_construction_nigp(codes):
    """Return True if any NIGP code in the list starts with a construction prefix."""
    for code in (codes or []):
        # Codes may be '9120000' or '912-23-00' or '912-23-00  DESCRIPTION'
        normalized = str(code).replace("-", "").replace(" ", "")
        for prefix in CONSTRUCTION_NIGP_PREFIXES:
            if normalized.startswith(prefix):
                return True
    return False


def map_naics(title, nigp_codes):
    """
    Map a contract title + NIGP codes to (naics_code, naics_label).
    1. Try keyword matching on title.
    2. Fall back to NIGP prefix lookup.
    3. Default to 236220 Commercial Building.
    """
    lower_title = title.lower() if title else ""

    # Keyword match on title
    for naics_code, naics_label, keywords in NAICS_FILTERS:
        for kw in keywords:
            if kw in lower_title:
                return naics_code, naics_label

    # NIGP prefix fallback
    for code in (nigp_codes or []):
        normalized = str(code).replace("-", "").replace(" ", "")
        for prefix, (naics_code, naics_label) in NIGP_NAICS_FALLBACK.items():
            if normalized.startswith(prefix):
                return naics_code, naics_label

    return "236220", "Commercial Building (NAICS 236220)"


def build_detail_url(encoded_id):
    """Build the public-facing contract detail URL."""
    return f"{PORTAL_BASE}?id={urllib.parse.quote(encoded_id)}"


# ---------------------------------------------------------------------------
# Scraping logic
# ---------------------------------------------------------------------------

def fetch_all_contracts():
    """
    Call the OCP search API and return all contracts with Amount.From > $250K.
    DatesOption=2 returns currently-active contracts (all years).
    We post-filter for 2026 award dates client-side.
    """
    payload = {
        "FilterBy": [
            {"id": 0, "name": "IsExpanded",   "value": False},
            {"id": 1, "name": "DatesOption",  "value": 2},
            {"id": 2, "name": "Amount.From",  "value": MIN_AMOUNT},
        ],
        "OrderBy": [],
    }
    print(f"Fetching all active contracts with amount > ${MIN_AMOUNT:,} ...")
    data = api_post(SEARCH_URL, payload)
    results = data.get("results", [])
    print(f"  API returned {len(results)} contracts total")
    return results


def filter_contracts(contracts):
    """
    Keep contracts where:
      - awardDate is in calendar year 2026 or later
      - at least one NIGP code matches a construction prefix
    """
    matched = []
    for c in contracts:
        award_date = parse_date(c.get("awardDate"))
        if not award_date or award_date < MIN_AWARD_DATE:
            continue
        if not is_construction_nigp(c.get("commodityCodes")):
            continue
        matched.append(c)
    return matched


def fetch_detail(encoded_id):
    """Fetch contract detail (vendor address, CO contact). Returns dict or {}."""
    url = f"{DETAIL_BASE}?id={urllib.parse.quote(encoded_id)}"
    result = api_get(url)
    return result or {}


def build_vendor_address(detail):
    """Assemble a single address string from vendor address fields."""
    parts = [
        detail.get("vendorStreet") or "",
        detail.get("vendorCity")   or "",
        detail.get("vendorState")  or "",
        detail.get("vendorZip")    or "",
    ]
    parts = [p.strip() for p in parts if p.strip()]
    return ", ".join(parts)


def scrape_all():
    """Main scrape flow. Returns list of result dicts ready for Google Sheets."""
    contracts = fetch_all_contracts()
    matched   = filter_contracts(contracts)

    print(f"\nFiltered to {len(matched)} construction contracts awarded in 2026:")
    if not matched:
        print("  (none found — this is expected early in the year; re-run weekly)")
        return []

    results = []
    for i, c in enumerate(matched, 1):
        contract_number = c.get("contractNumber", "")
        title           = c.get("title", "")
        vendor          = c.get("vendor", "")
        amount_str      = c.get("contractAmount", "")
        award_date      = c.get("awardDate", "")
        nigp_codes      = c.get("commodityCodes") or []
        encoded_id      = c.get("id", "")

        print(f"  [{i}/{len(matched)}] {contract_number} — {vendor} ({amount_str})")

        # Fetch detail for vendor address + contracting officer contact
        detail = fetch_detail(encoded_id)
        time.sleep(REQUEST_DELAY)

        vendor_address = build_vendor_address(detail)
        co_name  = detail.get("contractingOfficerName")  or \
                   detail.get("contractingSpecialistName") or ""
        co_email = detail.get("contractingOfficerEmail")  or \
                   detail.get("contractingSpecialistEmail") or ""

        # NIGP → NAICS classification
        naics_code, naics_label = map_naics(title, nigp_codes)

        # Build clean NIGP code string for description
        nigp_display = ", ".join(
            str(code).strip() for code in nigp_codes if code
        )

        results.append({
            "city":           "Washington, DC",
            "company_name":   vendor.strip(),
            "contact_name":   co_name.strip(),
            "phone":          "",              # not available in OCP API
            "email":          co_email.strip(),
            "address":        vendor_address,
            "website":        "",              # not available in OCP API
            "contract_name":  f"{contract_number} — {title}".strip(" —"),
            "award_amount":   amount_str,
            "amount_expended": "",
            "begin_date":     award_date,
            "award_link":     build_detail_url(encoded_id),
            "description":    f"Agency: {', '.join(c.get('agencyNames') or [])} | "
                              f"Period: {c.get('startDate','')} – {c.get('endDate','')} | "
                              f"NIGP: {nigp_display}",
            "commodity_type": naics_label,
        })

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
    existing   = get_existing_data(service)
    start_row  = len(existing) + 1
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
        print("\nNothing to write.")
        sys.exit(0)
    write_to_google_sheets(results)


if __name__ == "__main__":
    main()
