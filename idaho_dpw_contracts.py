"""
Idaho Division of Public Works construction bid results scraper.

Pulls awarded 2026 construction bids from the public "Recent Construction Bid Results"
section on https://dpw.idaho.gov/construction/ and maps projects into the shared sheet format.

Publicly available on this source:
  - project number
  - project title
  - bidder names
  - base bid amounts
  - submission date
  - notice of intent issued date

Not publicly available on this source:
  - vendor website
  - vendor phone
  - vendor email
  - vendor address
  - rich project scope beyond the project title
"""

from __future__ import annotations

import re
import sys
import time
from datetime import datetime

import requests
import urllib3
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SOURCE_URL = "https://dpw.idaho.gov/construction/"
TARGET_YEAR = 2026
MIN_AMOUNT = 250_000.0

SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
})

FETCH_RETRIES = 4
FETCH_TIMEOUT = 45

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

NAICS_FILTERS = [
    ("238210", "Electrical (NAICS 238210)", [
        "electrical", "fire alarm", "access control", "surveillance system",
        "surveillance", "security system", "cabling", "electronic",
        "lighting", "power", "generator",
    ]),
    ("238220", "Plumbing/HVAC (NAICS 238220)", [
        "hvac", "mechanical", "plumbing", "boiler", "chiller",
        "heating", "cooling", "air handler", "mep",
    ]),
    ("238120", "Structural Steel (NAICS 238120)", [
        "structural steel", "steel", "metal building",
    ]),
    ("237310", "Highway/Street/Bridge (NAICS 237310)", [
        "highway", "frontage", "road", "parking", "parking lot",
        "resurfacing", "rebuild", "reconfiguration", "sidewalk",
        "infrastructure improvements", "street", "bridge",
        "entrance", "pavement", "sitework",
    ]),
    ("236220", "Commercial Building (NAICS 236220)", [
        "remodel", "renovation", "renovations", "building", "hall",
        "dorm", "conference room", "maintenance building", "educational space",
        "lecture hall", "operations center", "house", "facility",
        "replace", "replacement", "updates",
    ]),
]


def parse_date(text: str) -> datetime | None:
    text = (text or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%m-%d-%y")
    except ValueError:
        return None


def parse_amount(text: str) -> float:
    cleaned = re.sub(r"[^0-9.]", "", text or "")
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def classify_naics(title: str) -> tuple[str, str] | None:
    lower = title.lower()
    for code, label, keywords in NAICS_FILTERS:
        if any(keyword in lower for keyword in keywords):
            return code, label
    return None


def fetch_page_text() -> list[str]:
    last_err = None
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            print(f"  Fetch attempt {attempt}/{FETCH_RETRIES}: {SOURCE_URL}")
            resp = SESSION.get(SOURCE_URL, timeout=FETCH_TIMEOUT, verify=False)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text("\n")
            lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
            return [line for line in lines if line]
        except Exception as exc:
            last_err = exc
            if attempt == FETCH_RETRIES:
                break
            wait = attempt * 10
            print(f"  WARN fetch failed on attempt {attempt}: {exc}")
            print(f"  Waiting {wait}s before retry...")
            time.sleep(wait)

    raise RuntimeError(f"Unable to fetch Idaho DPW construction page after {FETCH_RETRIES} attempts: {last_err}")


def extract_results_lines(lines: list[str]) -> list[str]:
    try:
        start = lines.index("Recent Construction Bid Results")
    except ValueError:
        raise RuntimeError("Could not locate 'Recent Construction Bid Results' section")

    results = []
    for line in lines[start + 1:]:
        if line.startswith("Archived Bid Results:"):
            continue
        if line.startswith("Submission Date Project Number Project Name Contractor Base Bid Alternates Notice of Intent Issued"):
            continue
        if line.startswith("* * *"):
            break
        results.append(line)
    return results


def split_project_blocks(lines: list[str]) -> list[list[str]]:
    blocks = []
    current = []
    for line in lines:
        if re.match(r"^\d{2}-\d{2}-\d{2}\s+\d{5}\s+", line):
            if current:
                blocks.append(current)
            current = [line]
        elif current:
            current.append(line)
    if current:
        blocks.append(current)
    return blocks


def parse_project_header(line: str) -> tuple[str, str, str, str]:
    m = re.match(r"^(\d{2}-\d{2}-\d{2})\s+(\d{5})\s+(.*)$", line)
    if not m:
        raise ValueError(f"Unparseable project header: {line}")

    submission_date, project_number, tail = m.groups()
    loc_match = re.search(r", Idaho\b", tail)
    if not loc_match:
        raise ValueError(f"Could not find Idaho location boundary: {line}")

    title = tail[: loc_match.end()].strip()
    remainder = tail[loc_match.end():].strip()
    return submission_date, project_number, title, remainder


def parse_bid_block(block: list[str]) -> dict | None:
    submission_date, project_number, title, remainder = parse_project_header(block[0])
    lines = [remainder] if remainder else []
    lines.extend(block[1:])

    bids: list[dict] = []
    pending_idx: int | None = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line == "No Bidders":
            return None

        if re.match(r"^\d+\.\s*(\$|N/A|NA)", line, re.IGNORECASE):
            continue

        if re.match(r"^(NON-RESPONSIVE|BID-RELIEF GRANTED)$", line, re.IGNORECASE):
            if pending_idx is not None:
                bids[pending_idx]["status"] = line.upper()
                pending_idx = None
            elif bids:
                bids[-1]["status"] = line.upper()
            continue

        bid_match = re.match(r"^(.*?)\$\s*([\d,]+(?:\.\d+)?)", line)
        if bid_match:
            vendor, amount = bid_match.groups()
            bids.append({
                "vendor": vendor.strip(),
                "amount": parse_amount(amount),
                "status": "",
            })
            pending_idx = None
            continue

        if re.match(r"^\d{2}-\d{2}-\d{2}$", line):
            continue

        bids.append({
            "vendor": line,
            "amount": None,
            "status": "",
        })
        pending_idx = len(bids) - 1

    notice_dates = re.findall(r"\b\d{2}-\d{2}-\d{2}\b", " ".join(block[1:]))
    notice_date = notice_dates[-1] if notice_dates else ""

    valid_bids = [
        bid for bid in bids
        if bid.get("amount") is not None
        and bid.get("status") not in {"NON-RESPONSIVE", "BID-RELIEF GRANTED"}
    ]
    if not valid_bids:
        return None

    winner = min(valid_bids, key=lambda b: b["amount"])
    return {
        "submission_date": submission_date,
        "notice_date": notice_date,
        "project_number": project_number,
        "title": title,
        "winner": winner["vendor"],
        "base_bid": winner["amount"],
    }


def scrape_all() -> list[dict]:
    print("Fetching Idaho DPW construction page...")
    lines = fetch_page_text()
    results_lines = extract_results_lines(lines)
    blocks = split_project_blocks(results_lines)
    print(f"Found {len(blocks)} bid-result project blocks")

    matched = []
    skipped_year = skipped_notice = skipped_amount = skipped_naics = 0

    for block in blocks:
        try:
            row = parse_bid_block(block)
        except Exception as exc:
            print(f"  WARN parse failure: {exc}")
            continue
        if not row:
            continue

        notice_dt = parse_date(row["notice_date"])
        if not notice_dt:
            skipped_notice += 1
            continue
        if notice_dt.year != TARGET_YEAR:
            skipped_year += 1
            continue
        if row["base_bid"] < MIN_AMOUNT:
            skipped_amount += 1
            continue

        naics = classify_naics(row["title"])
        if not naics:
            skipped_naics += 1
            continue

        submission_dt = parse_date(row["submission_date"])
        matched.append({
            "city": "Idaho",
            "company_name": row["winner"],
            "contact_name": "",
            "phone": "",
            "email": "",
            "address": "",
            "website": "",
            "contract_name": f"{row['project_number']} — {row['title']}",
            "award_amount": f"${row['base_bid']:,.2f}",
            "amount_expended": "",
            "begin_date": notice_dt.strftime("%Y-%m-%d"),
            "award_link": SOURCE_URL,
            "description": (
                f"Submission Date: {submission_dt.strftime('%Y-%m-%d') if submission_dt else row['submission_date']} | "
                f"Notice of Intent Issued: {notice_dt.strftime('%Y-%m-%d')} | "
                f"Idaho DPW Recent Construction Bid Results"
            ),
            "commodity_type": naics[1],
        })

    print("\n--- Summary ---")
    print(f"Parsed bid-result blocks:   {len(blocks)}")
    print(f"Skipped (no notice date):  {skipped_notice}")
    print(f"Skipped (not 2026):        {skipped_year}")
    print(f"Skipped (< $250K):         {skipped_amount}")
    print(f"Skipped (NAICS filter):    {skipped_naics}")
    print(f"Matched:                   {len(matched)}")
    return matched


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def get_existing_data(service):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A:N",
        ).execute()
        return result.get("values", [])
    except Exception:
        return []


def write_to_google_sheets(results: list[dict]) -> None:
    service = get_sheets_service()
    sheet = service.spreadsheets()

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

    existing = get_existing_data(service)
    start_row = len(existing) + 1
    existing_fps = {
        (row[1].strip().lower(), row[7].strip().lower())
        for row in (existing[1:] if len(existing) > 1 else [])
        if len(row) > 7
    }
    new_results = [
        r for r in results
        if (r.get("company_name", "").strip().lower(), r.get("contract_name", "").strip().lower())
        not in existing_fps
    ]
    if len(new_results) < len(results):
        print(f"  Skipped {len(results) - len(new_results)} already-existing entries")

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
    print(f"Wrote {len(rows)} rows to Google Sheet: {SHEET_NAME}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Idaho DPW awarded construction bids")
    parser.add_argument("--preview", action="store_true", help="Preview results without writing to Google Sheets")
    args = parser.parse_args()

    results = scrape_all()
    if not results:
        print("Nothing to write.")
        return

    if args.preview:
        for row in results[:10]:
            print(f"{row['company_name']} | {row['contract_name']} | {row['award_amount']} | {row['commodity_type']}")
        return

    write_to_google_sheets(results)


if __name__ == "__main__":
    main()
