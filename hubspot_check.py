"""
HubSpot Company Checker (read-only)

Reads company names from Sheet A (contracts) and the OSHA sheet and checks
if each exists in HubSpot. Writes "Yes" or "No" to the "In HubSpot?" column.

Skips rows that already have a value so reruns are safe.
"""

import os
import time

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Config ---
SERVICE_ACCOUNT_FILE = "service-account-key.json"
SCRAPER_SHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SCRAPER_SHEET_NAME = "Webscraper Tool for Procurement Sites Two"
OSHA_SHEET_NAME = "OSHA company audits"

COL_COMPANY  = 1   # B: Company Name (contracts sheet)
COL_HUBSPOT  = 16  # Q: In HubSpot? (contracts sheet)
COL_ENGAGED  = 17  # R: Last Engaged
COL_VISITS   = 18  # S: Site Visits
COL_SEQUENCE = 19  # T: In Sequence?
COL_REPLIES  = 20  # U: Replies

OSHA_COL_COMPANY  = 0   # A: Company Name (OSHA sheet)
OSHA_COL_HUBSPOT  = 23  # X: In HubSpot? (OSHA sheet)
OSHA_COL_ENGAGED  = 24  # Y: Last Engaged
OSHA_COL_VISITS   = 25  # Z: Site Visits
OSHA_COL_SEQUENCE = 26  # AA: In Sequence?
OSHA_COL_REPLIES  = 27  # AB: Replies

HUBSPOT_SEARCH_URL  = "https://api.hubapi.com/crm/v3/objects/companies/search"
HUBSPOT_COMPANY_URL = "https://api.hubapi.com/crm/v3/objects/companies/{}"
HUBSPOT_ASSOC_URL   = "https://api.hubapi.com/crm/v3/objects/companies/{}/associations/contacts"
HUBSPOT_BATCH_URL   = "https://api.hubapi.com/crm/v3/objects/contacts/batch/read"
API_DELAY = 0.3   # seconds between HubSpot calls


# --- Env ---

def _load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())


_load_env()
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")


# --- Sheets ---

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def read_sheet_data(service):
    """Read all rows A:U from the contracts sheet."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SCRAPER_SHEET_ID,
        range=f"'{SCRAPER_SHEET_NAME}'!A:U",
    ).execute()
    return result.get("values", [])


def read_osha_data(service):
    """Read data rows A5:AB from the OSHA sheet (row 5 = first data row)."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SCRAPER_SHEET_ID,
        range=f"'{OSHA_SHEET_NAME}'!A5:AB",
    ).execute()
    return result.get("values", [])


def write_batch(service, updates):
    """Write a batch of (range, [values]) pairs to the sheet.
    Each update is (range_str, list_of_values_for_one_row).
    """
    if not updates:
        return
    batch_data = [{"range": r, "values": [v]} for r, v in updates]
    for start in range(0, len(batch_data), 100):
        chunk = batch_data[start:start + 100]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SCRAPER_SHEET_ID,
            body={"valueInputOption": "RAW", "data": chunk},
        ).execute()
        print(f"  Wrote {len(chunk)} rows to sheet")


# --- HubSpot ---

def _name_similarity(a, b):
    """Jaccard similarity on words, ignoring common legal suffixes."""
    stops = {"inc", "llc", "corp", "co", "ltd", "the", "and", "&", "of", "company", "contractors", "construction"}
    words_a = set(a.lower().split()) - stops
    words_b = set(b.lower().split()) - stops
    if not words_a or not words_b:
        return 0.0
    return len(words_a & words_b) / len(words_a | words_b)


_EMPTY_ENGAGEMENT = ["—", "—", "—", "—"]


def _get_engagement(company_id, headers):
    """Fetch engagement data for a matched HubSpot company. Returns list of 4 values:
    [last_engaged, site_visits, in_sequence, replies]
    """
    try:
        # Company-level: last contacted date + website visits
        resp = requests.get(
            HUBSPOT_COMPANY_URL.format(company_id),
            params={"properties": "notes_last_contacted,hs_analytics_num_visits"},
            headers=headers, timeout=10,
        )
        resp.raise_for_status()
        props = resp.json().get("properties", {})
        raw_date = props.get("notes_last_contacted") or ""
        last_engaged = raw_date[:10] if raw_date else "—"  # trim to YYYY-MM-DD
        site_visits = str(int(props.get("hs_analytics_num_visits") or 0))
        time.sleep(API_DELAY)

        # Contact associations
        resp2 = requests.get(
            HUBSPOT_ASSOC_URL.format(company_id),
            headers=headers, timeout=10,
        )
        resp2.raise_for_status()
        contact_ids = [r["id"] for r in resp2.json().get("results", [])]
        time.sleep(API_DELAY)

        in_sequence = "No"
        replies = "0"
        if contact_ids:
            batch_payload = {
                "inputs": [{"id": cid} for cid in contact_ids[:50]],
                "properties": ["hs_sequences_actively_enrolled_count", "hs_email_replied"],
            }
            resp3 = requests.post(HUBSPOT_BATCH_URL, json=batch_payload, headers=headers, timeout=15)
            resp3.raise_for_status()
            total_seq = 0
            total_replies = 0
            for contact in resp3.json().get("results", []):
                cp = contact.get("properties", {})
                total_seq += int(cp.get("hs_sequences_actively_enrolled_count") or 0)
                total_replies += int(cp.get("hs_email_replied") or 0)
            in_sequence = "Yes" if total_seq > 0 else "No"
            replies = str(total_replies)
            time.sleep(API_DELAY)

        return [last_engaged, site_visits, in_sequence, replies]

    except Exception as e:
        print(f"    Engagement fetch error: {e}")
        return _EMPTY_ENGAGEMENT


def check_hubspot(company_name):
    """
    Search HubSpot for a company by name (read-only).
    Returns a list of 5 values: [status, last_engaged, site_visits, in_sequence, replies]
    status is "Yes", "No", or "Error".
    """
    name = company_name.strip()
    if not name:
        return ["", "—", "—", "—", "—"]

    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "query": name,
        "limit": 5,
        "properties": ["name"],
    }

    try:
        resp = requests.post(HUBSPOT_SEARCH_URL, json=payload, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            return ["No"] + _EMPTY_ENGAGEMENT

        name_lower = name.lower()
        for result in results:
            hs_name = (result.get("properties") or {}).get("name") or ""
            hs_lower = hs_name.lower()
            if (name_lower in hs_lower or hs_lower in name_lower
                    or _name_similarity(name_lower, hs_lower) >= 0.5):
                company_id = result["id"]
                engagement = _get_engagement(company_id, headers)
                return ["Yes"] + engagement

        return ["No"] + _EMPTY_ENGAGEMENT

    except requests.HTTPError as e:
        print(f"    HTTP error for '{name}': {e.response.status_code} {e.response.text[:120]}")
        return ["Error"] + _EMPTY_ENGAGEMENT
    except Exception as e:
        print(f"    Error for '{name}': {e}")
        return ["Error"] + _EMPTY_ENGAGEMENT


# --- Main ---

def col_letter(n):
    """Convert 0-based column index to spreadsheet letter (0=A, 16=Q)."""
    result = ""
    n += 1
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def check_contracts_sheet(service):
    """Check contracts sheet companies against HubSpot and write results to columns Q:U."""
    hs_col      = col_letter(COL_HUBSPOT)   # Q
    engaged_col = col_letter(COL_ENGAGED)   # R
    last_col    = col_letter(COL_REPLIES)   # U

    print(f"--- Contracts sheet ({SCRAPER_SHEET_NAME}) ---")
    rows = read_sheet_data(service)

    if not rows:
        print("No data found in contracts sheet.")
        return

    # Write headers if missing
    header = rows[0]
    if (len(header) <= COL_HUBSPOT or header[COL_HUBSPOT] != "In HubSpot?"
            or len(header) <= COL_REPLIES or header[COL_REPLIES] != "Replies"):
        print(f"Writing headers to {hs_col}1:{last_col}1...")
        service.spreadsheets().values().update(
            spreadsheetId=SCRAPER_SHEET_ID,
            range=f"'{SCRAPER_SHEET_NAME}'!{hs_col}1:{last_col}1",
            valueInputOption="RAW",
            body={"values": [["In HubSpot?", "Last Engaged", "Site Visits", "In Sequence?", "Replies"]]},
        ).execute()

    data_rows = rows[1:]
    total = sum(1 for r in data_rows if len(r) > COL_COMPANY and r[COL_COMPANY].strip())
    print(f"Found {total} rows with company names. Checking HubSpot...")

    updates = []
    checked = skipped = 0

    for i, row in enumerate(data_rows, start=2):
        company = row[COL_COMPANY].strip() if len(row) > COL_COMPANY else ""
        if not company:
            continue

        # Skip if all 5 columns already populated
        engaged_val = row[COL_ENGAGED].strip() if len(row) > COL_ENGAGED else ""
        hs_val = row[COL_HUBSPOT].strip() if len(row) > COL_HUBSPOT else ""
        if hs_val and engaged_val:
            skipped += 1
            continue

        print(f"  Row {i}: '{company}'...", end=" ", flush=True)
        result = check_hubspot(company)
        print(result[0])
        checked += 1

        updates.append((f"'{SCRAPER_SHEET_NAME}'!{hs_col}{i}:{last_col}{i}", result))
        time.sleep(API_DELAY)

        if len(updates) >= 50:
            write_batch(service, updates)
            updates = []

    if updates:
        write_batch(service, updates)

    print(f"Contracts done. Checked: {checked}, Skipped (already done): {skipped}")


def check_osha_sheet(service):
    """Check OSHA sheet companies against HubSpot and write results to columns X:AB."""
    hs_col      = col_letter(OSHA_COL_HUBSPOT)   # X
    engaged_col = col_letter(OSHA_COL_ENGAGED)   # Y
    last_col    = col_letter(OSHA_COL_REPLIES)   # AB

    print(f"\n--- OSHA sheet ({OSHA_SHEET_NAME}) ---")
    rows = read_osha_data(service)
    if not rows:
        print("No data found in OSHA sheet.")
        return

    total = sum(1 for r in rows if len(r) > OSHA_COL_COMPANY and r[OSHA_COL_COMPANY].strip())
    print(f"Found {total} rows with company names. Checking HubSpot...")

    updates = []
    checked = skipped = 0

    # enumerate with start=5 so i == actual sheet row number (data starts at row 5)
    for i, row in enumerate(rows, start=5):
        company = row[OSHA_COL_COMPANY].strip() if len(row) > OSHA_COL_COMPANY else ""
        if not company or company.startswith("===") or company.startswith("These"):
            continue

        # Skip if all 5 columns already populated
        engaged_val = row[OSHA_COL_ENGAGED].strip() if len(row) > OSHA_COL_ENGAGED else ""
        hs_val = row[OSHA_COL_HUBSPOT].strip() if len(row) > OSHA_COL_HUBSPOT else ""
        if hs_val and engaged_val:
            skipped += 1
            continue

        print(f"  Row {i}: '{company}'...", end=" ", flush=True)
        result = check_hubspot(company)
        print(result[0])
        checked += 1

        updates.append((f"'{OSHA_SHEET_NAME}'!{hs_col}{i}:{last_col}{i}", result))
        time.sleep(API_DELAY)

        if len(updates) >= 50:
            write_batch(service, updates)
            updates = []

    if updates:
        write_batch(service, updates)

    print(f"OSHA done. Checked: {checked}, Skipped (already done): {skipped}")


if __name__ == "__main__":
    if not HUBSPOT_TOKEN:
        print("ERROR: HUBSPOT_TOKEN not set in .env")
    else:
        svc = get_sheets_service()
        check_contracts_sheet(svc)
        check_osha_sheet(svc)
