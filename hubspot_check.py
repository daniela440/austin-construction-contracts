"""
HubSpot Company Checker (read-only)

Reads company names from Sheet A and checks if each exists in HubSpot.
Writes "Yes" or "No" to column Q ("In HubSpot?").

Skips rows that already have a value in column Q so reruns are safe.
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

COL_COMPANY = 1   # B: Company Name
COL_HUBSPOT = 16  # Q: In HubSpot?

HUBSPOT_SEARCH_URL = "https://api.hubapi.com/crm/v3/objects/companies/search"
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
    """Read all rows A:Q from the sheet."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SCRAPER_SHEET_ID,
        range=f"'{SCRAPER_SHEET_NAME}'!A:Q",
    ).execute()
    return result.get("values", [])


def write_batch(service, updates):
    """Write a batch of (range, value) pairs to the sheet."""
    if not updates:
        return
    batch_data = [{"range": r, "values": [[v]]} for r, v in updates]
    for start in range(0, len(batch_data), 100):
        chunk = batch_data[start:start + 100]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SCRAPER_SHEET_ID,
            body={"valueInputOption": "RAW", "data": chunk},
        ).execute()
        print(f"  Wrote {len(chunk)} cells to sheet")


# --- HubSpot ---

def _name_similarity(a, b):
    """Jaccard similarity on words, ignoring common legal suffixes."""
    stops = {"inc", "llc", "corp", "co", "ltd", "the", "and", "&", "of", "company", "contractors", "construction"}
    words_a = set(a.lower().split()) - stops
    words_b = set(b.lower().split()) - stops
    if not words_a or not words_b:
        return 0.0
    return len(words_a & words_b) / len(words_a | words_b)


def check_hubspot(company_name):
    """
    Search HubSpot for a company by name (read-only).
    Returns "Yes" if a match is found, "No" otherwise.
    """
    name = company_name.strip()
    if not name:
        return ""

    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }

    # Freetext search across all company properties
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
            return "No"

        name_lower = name.lower()
        for result in results:
            hs_name = (result.get("properties") or {}).get("name") or ""
            hs_lower = hs_name.lower()
            # Accept: one name contains the other, or word-overlap Jaccard >= 0.5
            if (name_lower in hs_lower or hs_lower in name_lower
                    or _name_similarity(name_lower, hs_lower) >= 0.5):
                return "Yes"

        return "No"

    except requests.HTTPError as e:
        print(f"    HTTP error for '{name}': {e.response.status_code} {e.response.text[:120]}")
        return "Error"
    except Exception as e:
        print(f"    Error for '{name}': {e}")
        return "Error"


# --- Main ---

def col_letter(n):
    """Convert 0-based column index to spreadsheet letter (0=A, 16=Q)."""
    result = ""
    n += 1
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def main():
    if not HUBSPOT_TOKEN:
        print("ERROR: HUBSPOT_TOKEN not set in .env")
        return

    hs_col = col_letter(COL_HUBSPOT)  # "Q"

    print("Reading sheet data...")
    service = get_sheets_service()
    rows = read_sheet_data(service)

    if not rows:
        print("No data found in sheet.")
        return

    # Write header if missing
    header = rows[0]
    current_header = header[COL_HUBSPOT] if len(header) > COL_HUBSPOT else ""
    if current_header != "In HubSpot?":
        print(f"Writing header 'In HubSpot?' to {hs_col}1...")
        service.spreadsheets().values().update(
            spreadsheetId=SCRAPER_SHEET_ID,
            range=f"'{SCRAPER_SHEET_NAME}'!{hs_col}1",
            valueInputOption="RAW",
            body={"values": [["In HubSpot?"]]},
        ).execute()

    data_rows = rows[1:]  # skip header row
    total = sum(1 for r in data_rows if len(r) > COL_COMPANY and r[COL_COMPANY].strip())
    print(f"Found {total} rows with company names. Checking HubSpot...")

    updates = []
    checked = skipped = 0

    for i, row in enumerate(data_rows, start=2):
        company = row[COL_COMPANY].strip() if len(row) > COL_COMPANY else ""
        if not company:
            continue

        existing = row[COL_HUBSPOT].strip() if len(row) > COL_HUBSPOT else ""
        if existing:
            skipped += 1
            continue  # already checked; skip to allow safe reruns

        print(f"  Row {i}: '{company}'...", end=" ", flush=True)
        result = check_hubspot(company)
        print(result)
        checked += 1

        updates.append((f"'{SCRAPER_SHEET_NAME}'!{hs_col}{i}", result))
        time.sleep(API_DELAY)

        # Flush to sheet every 50 rows
        if len(updates) >= 50:
            write_batch(service, updates)
            updates = []

    if updates:
        write_batch(service, updates)

    print(f"\nDone. Checked: {checked}, Skipped (already done): {skipped}")


if __name__ == "__main__":
    main()
