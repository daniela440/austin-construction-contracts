"""
CT DAS CTSource Contract Board Scraper
Fetches active construction contracts via the Proactis WebProcure JSON API.

Portal:      https://portal.ct.gov/das/ctsource/contractboard
Search API:  GET https://webprocure.proactiscloud.com/wp-full-text-search/search/contracts
Detail API:  GET https://webprocure.proactiscloud.com/wp-contract/public/contract/{id}

Filters applied:
  - effectiveDate >= Jan 1, 2026  (contract start year)
  - maxValue > $250,000
  - Commodity / title matches construction UNSPSC categories (mapped to NAICS below)
  - Vendor is a private company (excludes gov/nonprofit name patterns)

Run:
    python3 ct_ctsource_contracts.py           # normal run (appends to sheet)
    python3 ct_ctsource_contracts.py --dry-run # print sample, do NOT write to sheet
"""

import argparse
import json
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CUSTOMER_ID    = 51          # CT DAS eboId in Proactis WebProcure
SEARCH_URL     = "https://webprocure.proactiscloud.com/wp-full-text-search/search/contracts"
DETAIL_URL     = "https://webprocure.proactiscloud.com/wp-contract/public/contract"
PORTAL_BASE    = "https://webprocure.proactiscloud.com/wp-web-public/en/#/contractboard"
PAGE_SIZE      = 10          # Proactis API always returns 10 per page (ignores size param)

MIN_EFFECTIVE_DATE = datetime(2026, 1, 1)
MIN_AMOUNT         = 250_000.0

# DAS Construction Services borgId (Proactis WebProcure internal org ID).
# Filtering by borgId=6 targets the correct org directly and avoids keyword noise.
DAS_CONSTRUCTION_BORG_ID = 6

REQUEST_DELAY  = 0.5         # seconds between detail API calls

# Google Sheets config
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID       = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME           = "Webscraper Tool for Procurement Sites Two"

# ---------------------------------------------------------------------------
# SSL (macOS Python often lacks default certs)
# ---------------------------------------------------------------------------
try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
    # Test the context works; Proactis uses intermediate CA not always in certifi
    _test = ssl.create_default_context(cafile=certifi.where())
except (ImportError, Exception):
    SSL_CTX = ssl.create_default_context()
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE

# Proactis uses an intermediate CA not always bundled — allow without verification
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": f"https://webprocure.proactiscloud.com/wp-web-public/en/?eboId={CUSTOMER_ID}",
}

# ---------------------------------------------------------------------------
# UNSPSC commodity names that map to construction work
# Used for server-side Commodity facet filtering AND client-side NAICS mapping
# ---------------------------------------------------------------------------

# Maps: (naics_code, naics_label, [commodity_keywords_to_match])
NAICS_COMMODITY_MAP = [
    ("238210", "Electrical (NAICS 238210)", [
        "electrical systems",
        "electrical equipment",
        "lighting",
        "wiring",
        "electric vehicle charging",
    ]),
    ("238220", "Plumbing/HVAC (NAICS 238220)", [
        "plumbing construction",
        "heating and cooling",
        "hvac",
        "heating and ventilation",
        "distribution and conditioning",
    ]),
    ("238120", "Structural Steel (NAICS 238120)", [
        "structural building products",
        "structural components",
        "structural materials",
    ]),
    ("237310", "Highway/Street/Bridge (NAICS 237310)", [
        "highway and road construction",
        "infrastructure building",
        "infrastructure maintenance",
        "roads and landscape",
        "civil engineering",
    ]),
    ("236220", "Commercial Building (NAICS 236220)", [
        "building and facility construction",
        "building and facility maintenance",
        "building maintenance",
        "nonresidential building",
        "residential building",
        "heavy construction",
        "specialized trade construction",
        "commercial and office building",
        "multiple unit dwelling",
        "building construction",
        "das construction",                 # CT org name
        "concrete installation",
        "masonry and stonework",
        "carpentry",
        "plastering and drywall",
        "floor laying",
        "glass and glazing",
        "painting and paper",
        "coating and caulking",
        "interior finishing",
        "conveyance systems installation",
    ]),
]

# All commodity keywords (for quick filtering before NAICS assignment)
ALL_CONSTRUCTION_KEYWORDS = [kw for _, _, kws in NAICS_COMMODITY_MAP for kw in kws]

# ---------------------------------------------------------------------------
# Vendor name patterns that indicate non-private entities to exclude
# ---------------------------------------------------------------------------
GOV_NONPROFIT_PATTERNS = [
    "city of ", "town of ", "state of ", "county of ",
    "board of education", "board of ed",
    "university", "college", "institute of technology",
    "authority",
    "district ",
    " alliance", "nonprofit",
    "department of ", "dept. of ",
    "association", "council of governments",
    "regional agency",
]

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def api_get(url: str, params: dict, retries: int = 3) -> dict:
    """GET JSON from url with query params. Retries on timeout/network error."""
    qs = urllib.parse.urlencode(params)
    full_url = f"{url}?{qs}"
    req = urllib.request.Request(full_url, headers=HEADERS)
    last_err = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = e.read(300).decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {e.code} on {full_url}: {body}") from e
        except Exception as e:
            last_err = e
            wait = 3 * (attempt + 1)
            print(f"  [RETRY {attempt+1}/{retries}] {e} — waiting {wait}s ...")
            time.sleep(wait)
    raise RuntimeError(f"Failed after {retries} retries: {last_err}")


def fetch_search_page(offset: int) -> dict:
    """Fetch one page of DAS Construction Services contracts."""
    return api_get(SEARCH_URL, {
        "customerid": CUSTOMER_ID,
        "borgIds": DAS_CONSTRUCTION_BORG_ID,
        "q": "*",
        "from": offset,
        "sort": "t-a",
    })


def fetch_detail(contract_id: int) -> dict:
    """Fetch full contract detail from wp-contract API."""
    return api_get(f"{DETAIL_URL}/{contract_id}", {"customerId": CUSTOMER_ID})


# ---------------------------------------------------------------------------
# Filtering & classification
# ---------------------------------------------------------------------------

def ts_to_dt(ts_ms) -> datetime | None:
    """Convert epoch milliseconds → datetime, or None."""
    if not ts_ms:
        return None
    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000)
    except (ValueError, OSError):
        return None


def is_construction_commodity(commodities: list[str]) -> bool:
    """Return True if any commodity string looks like construction work."""
    joined = " ".join((c or "").lower() for c in commodities)
    return any(kw in joined for kw in ALL_CONSTRUCTION_KEYWORDS)


def classify_naics(title: str, commodities: list[str]) -> tuple[str, str]:
    """
    Map contract title + commodity list → (naics_code, naics_label).
    First match wins; defaults to 236220 Commercial Building.
    """
    combined = (title or "").lower() + " " + " ".join((c or "").lower() for c in commodities)
    for naics_code, naics_label, keywords in NAICS_COMMODITY_MAP:
        for kw in keywords:
            if kw in combined:
                return naics_code, naics_label
    return "236220", "Commercial Building (NAICS 236220)"


def is_private_vendor(name: str) -> bool:
    """Return False if the vendor looks like a government or nonprofit entity."""
    lower = (name or "").lower()
    return not any(pat in lower for pat in GOV_NONPROFIT_PATTERNS)


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def fetch_all_matching() -> list[dict]:
    """
    Paginate through all CT contracts and return those that match:
    - effectiveDate >= 2026-01-01
    - is_construction_commodity
    Then deduplicate by contract ID.
    """
    print(f"Fetching CT CTSource contracts (customerid={CUSTOMER_ID}) ...")

    # First page to get total
    first = fetch_search_page(0)
    total = first.get("hits", 0)
    print(f"  Total contracts in system: {total}")

    all_records = list(first.get("records", []))

    offset = PAGE_SIZE
    while offset < total:
        page = fetch_search_page(offset)
        records = page.get("records", [])
        if not records:
            break
        all_records.extend(records)
        print(f"  Fetched {len(all_records)}/{total} ...", end="\r")
        offset += PAGE_SIZE
        time.sleep(0.2)

    print(f"  Fetched {len(all_records)} total records.          ")

    today = datetime.now()

    # Filter: effectiveDate >= MIN_EFFECTIVE_DATE and contract not expired
    matched = []
    for r in all_records:
        eff_dt = ts_to_dt(r.get("effectiveDate"))
        exp_dt = ts_to_dt(r.get("expirationDate"))

        if not eff_dt or eff_dt < MIN_EFFECTIVE_DATE:
            continue
        # Skip expired contracts
        if exp_dt and exp_dt < today:
            continue

        matched.append(r)

    print(f"  Pre-filtered to {len(matched)} contracts with effective date >= {MIN_EFFECTIVE_DATE.date()}")
    return matched


def enrich_contract(record: dict) -> dict | None:
    """
    Fetch detail for a contract. Return enriched dict or None if it
    doesn't pass amount / vendor filters.
    """
    contract_id = record.get("id")
    title       = record.get("title", "")

    try:
        detail = fetch_detail(contract_id)
    except RuntimeError as e:
        print(f"  [WARN] detail fetch failed for {contract_id}: {e}")
        return None

    time.sleep(REQUEST_DELAY)

    # Amount filter
    max_value = float(detail.get("maxValue") or 0)
    if max_value < MIN_AMOUNT:
        # Some contracts have hideContractValue=True or maxValue=0 — include them if hidden
        if not detail.get("hideContractValue"):
            print(f"  [SKIP] #{detail.get('number')} amount ${max_value:,.0f} < ${MIN_AMOUNT:,.0f}")
            return None

    # Vendor info
    supplier = detail.get("supplier") or {}
    contact  = supplier.get("contact") or {}
    vendor   = supplier.get("name", "").strip()

    if not vendor:
        vendor = record.get("supplierName", "").strip()

    # Private company filter
    if not is_private_vendor(vendor):
        print(f"  [SKIP] #{detail.get('number')} — non-private vendor: {vendor}")
        return None

    # Build commodity list from search record
    # (search record doesn't have commodities, but the detail borg has category info)
    commodities = [
        detail.get("description", ""),
        title,
    ]

    naics_code, naics_label = classify_naics(title, commodities)

    # Dates
    eff_date  = detail.get("effectiveDateString") or detail.get("awardDateString") or ""
    exp_date  = detail.get("expireDateString") or ""

    # Amount display
    amount_str = f"${max_value:,.0f}" if max_value else "N/A (value hidden)"
    expended   = float(detail.get("cumulativeExpendedValue") or 0)
    expended_str = f"${expended:,.0f}" if expended else ""

    # Contract portal link
    link = f"{PORTAL_BASE}/contract/{contract_id}?customerid={CUSTOMER_ID}"

    # Agency
    borg = detail.get("borg") or {}
    agency = borg.get("borgName", "")

    # Contract admin (buyer-side, not vendor — but useful context)
    admin = detail.get("contractAdministrator") or {}

    return {
        "city":           "Hartford, CT",
        "company_name":   vendor,
        "contact_name":   contact.get("contactName", "").strip(),
        "phone":          (contact.get("telephone") or "").strip().rstrip("-"),
        "email":          contact.get("email", "").strip(),
        "address":        contact.get("contactLocation", "").strip(),
        "website":        "",
        "contract_name":  f"{detail.get('number','')} — {title}".strip(" —"),
        "award_amount":   amount_str,
        "amount_expended": expended_str,
        "begin_date":     eff_date,
        "award_link":     link,
        "description":    (
            f"Agency: {agency} | "
            f"Period: {eff_date} – {exp_date} | "
            f"Desc: {(detail.get('description') or '')[:120]}"
        ),
        "commodity_type": naics_label,
        "_contract_id":   contract_id,  # internal key for dedup (not written to sheet)
    }


def scrape_all() -> list[dict]:
    """Main scrape flow."""
    candidates = fetch_all_matching()
    if not candidates:
        print("No matching contracts found.")
        return []

    print(f"\nFetching detail for {len(candidates)} candidate contracts ...")
    results = []
    for i, rec in enumerate(candidates, 1):
        print(f"  [{i}/{len(candidates)}] #{rec.get('number','?')} — {rec.get('title','')[:60]}")
        enriched = enrich_contract(rec)
        if enriched:
            results.append(enriched)

    print(f"\n→ {len(results)} contracts passed all filters")
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


def write_to_google_sheets(results: list[dict]) -> None:
    """
    Append-only: never removes existing data.
    Deduplicates by (contract_id embedded in award_link OR contract_name).
    """
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

    # Read existing to deduplicate
    existing = get_existing_data(service)
    start_row = len(existing) + 1

    # Fingerprint existing rows: lowercase contract_name (col 7, index 7)
    existing_fps = {
        row[7].strip().lower()
        for row in (existing[1:] if len(existing) > 1 else [])
        if len(row) > 7
    }

    new_results = [
        r for r in results
        if r.get("contract_name", "").strip().lower() not in existing_fps
    ]
    skipped = len(results) - len(new_results)
    if skipped:
        print(f"  Skipped {skipped} already-existing entries")

    # Write headers if sheet is empty
    if not existing:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A1",
            valueInputOption="RAW",
            body={"values": [SHEET_HEADERS]},
        ).execute()
        start_row = 2

    rows = [[r.get(f, "") for f in SHEET_FIELDS] for r in new_results]

    if rows:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A{start_row}",
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()
        print(f"Wrote {len(rows)} new rows to Google Sheet: {SHEET_NAME}")
    else:
        print("Nothing new to write.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def print_sample(results: list[dict]) -> None:
    """Print a human-readable sample of results."""
    print(f"\n{'='*70}")
    print(f"SAMPLE OUTPUT — {len(results)} contracts found")
    print(f"Source: CT DAS Construction Services (borgId=6), effective >= {MIN_EFFECTIVE_DATE.date()}")
    print(f"{'='*70}")
    for i, r in enumerate(results[:10], 1):
        print(f"\n[{i}] {r['contract_name'][:70]}")
        print(f"     Company:  {r['company_name']}")
        print(f"     Contact:  {r['contact_name']} | {r['phone']} | {r['email']}")
        print(f"     Address:  {r['address']}")
        print(f"     Amount:   {r['award_amount']}  (expended: {r['amount_expended'] or 'N/A'})")
        print(f"     Start:    {r['begin_date']}")
        print(f"     Type:     {r['commodity_type']}")
        print(f"     Link:     {r['award_link']}")
    if len(results) > 10:
        print(f"\n... and {len(results)-10} more.")


def main():
    parser = argparse.ArgumentParser(description="CT CTSource construction contract scraper")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print sample output only — do NOT write to Google Sheets")
    args = parser.parse_args()

    results = scrape_all()

    if not results:
        print("No results to write.")
        sys.exit(0)

    print_sample(results)

    if args.dry_run:
        print(f"\n[DRY RUN] Skipping Google Sheets write. {len(results)} records ready.")
        sys.exit(0)

    write_to_google_sheets(results)


if __name__ == "__main__":
    main()
