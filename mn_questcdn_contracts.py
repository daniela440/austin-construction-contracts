"""
Minnesota QuestCDN final-award scraper.

Targets public QuestCDN bid results for the Minnesota Department of Administration
provider where:
  - Bid Award Type is Final
  - Posting Type is Construction Project
  - closing / award timing is in 2026
  - the project title maps to one of the tracked NAICS buckets
  - awarded amount is >= $250,000

Publicly available from QuestCDN:
  - structured results list via /cdn/results_data/
  - detail page with owner / solicitor / award metadata
  - award PDF containing awarded contractor and bid amounts

Not consistently available from the tested HTML:
  - awarded contractor contact name / phone / email on the detail page
"""

from __future__ import annotations

import argparse
import io
import re
import time
from datetime import datetime
from html import unescape
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from google.oauth2 import service_account
from googleapiclient.discovery import build

BASE_URL = "https://qcpi.questcdn.com"
RESULTS_URL = f"{BASE_URL}/cdn/results/?group=6506969&provider=6506969&projType=all"
RESULTS_API_URL = f"{BASE_URL}/cdn/results_data/"
DETAIL_QUERY_SUFFIX = "group=6506969&provider=6506969&projType=all"

TARGET_YEAR = 2026
MIN_AMOUNT = 250_000.0
REQUEST_DELAY = 0.3

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

NAICS_FILTERS = [
    ("238210", "Electrical (NAICS 238210)", [
        "electrical", "lighting", "signal", "generator", "wiring",
        "control room", "controls", "security systems",
    ]),
    ("238220", "Plumbing/HVAC (NAICS 238220)", [
        "hvac", "plumbing", "heating", "cooling", "boiler",
        "chiller", "mechanical", "finned-tube",
    ]),
    ("238120", "Structural Steel (NAICS 238120)", [
        "steel", "structural", "metal", "iron", "fence",
    ]),
    ("237310", "Highway/Street/Bridge (NAICS 237310)", [
        "road", "bridge", "paving", "asphalt", "street",
        "storm sewer", "water control structure", "sitework",
        "causeway", "trail", "slope repair",
    ]),
    ("236220", "Commercial Building (NAICS 236220)", [
        "construction", "renovation", "building", "facility",
        "addition", "abatement", "redevelopment", "repair",
        "rehabilitation", "remodel",
    ]),
]

EXCLUDE_KEYWORDS = [
    "tree removal",
    "prairie enhancement",
    "wetland restoration",
    "land management",
    "bat survey",
]


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def strip_tags(html: str) -> str:
    return normalize_text(re.sub(r"<[^>]+>", " ", html or ""))


def title_from_html(html: str) -> str:
    match = re.search(r'title="([^"]+)"', html or "", re.I)
    if match:
        return normalize_text(unescape(match.group(1)))
    return normalize_text(unescape(strip_tags(html)))


def href_from_html(html: str) -> str:
    match = re.search(r'href="([^"]+)"', html or "", re.I)
    return urljoin(BASE_URL, match.group(1)) if match else ""


def canonical_detail_url(url: str) -> str:
    if not url:
        return ""
    if "/cdn/results/view/" not in url:
        return url
    if "group=6506969" in url:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}{DETAIL_QUERY_SUFFIX}"


def parse_amount(text: str) -> float:
    cleaned = re.sub(r"[^0-9.]", "", text or "")
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def normalize_company(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def classify_naics(title: str) -> tuple[str, str] | None:
    lower = (title or "").lower()
    if any(keyword in lower for keyword in EXCLUDE_KEYWORDS):
        return None

    for code, label, keywords in NAICS_FILTERS:
        if any(keyword in lower for keyword in keywords):
            return code, label
    return None


def build_results_params(start: int, length: int) -> list[tuple[str, str]]:
    params: list[tuple[str, str]] = [("draw", "1")]
    for idx in range(10):
        params.extend(
            [
                (f"columns[{idx}][data]", str(idx)),
                (f"columns[{idx}][name]", ""),
                (f"columns[{idx}][searchable]", "true"),
                (f"columns[{idx}][orderable]", "true"),
                (f"columns[{idx}][search][value]", ""),
                (f"columns[{idx}][search][regex]", "false"),
            ]
        )
    params.append(("columns[2][searchable]", "false"))
    params.append(("columns[9][search][value]", "Final"))
    params.extend(
        [
            ("start", str(start)),
            ("length", str(length)),
            ("search[value]", ""),
            ("search[regex]", "false"),
            ("group", "6506969"),
            ("provider", "6506969"),
            ("projType", "all"),
        ]
    )
    return params


def extract_detail_metadata(soup: BeautifulSoup) -> tuple[dict[str, str], str]:
    metadata: dict[str, str] = {}
    info_link = ""

    for row in soup.select("table tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) != 2:
            continue
        key = normalize_text(cells[0].get_text(" ", strip=True))
        value = normalize_text(cells[1].get_text(" ", strip=True))
        if key.endswith(":"):
            metadata[key[:-1]] = value
            if key.startswith("Bid Result/Award Information"):
                link = cells[1].find("a", href=True)
                if link:
                    info_link = urljoin(BASE_URL, link["href"])

    return metadata, info_link


def extract_pdf_award(pdf_bytes: bytes) -> tuple[str, str, float]:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    text = "\n".join((page.extract_text() or "") for page in reader.pages)
    text = normalize_text(text)

    awarded_company = ""
    awarded_date = ""
    awarded_amount_text = ""

    match = re.search(
        r"AWARDED ON\s+(\d{2}/\d{2}/\d{4})\s+TO\s+(.+?)\s+COMPANY:",
        text,
        re.I,
    )
    if match:
        awarded_date = match.group(1)
        awarded_company = normalize_text(match.group(2))
    else:
        match = re.search(r"AWARDED TO\s+(.+?)\s+COMPANY:", text, re.I)
        if match:
            awarded_company = normalize_text(match.group(1))

    company_block_match = re.search(r"COMPANY:?\s*(.+?)\s+CITY:", text, re.I)
    price_block_match = re.search(
        r"BASE PRICE:\s*(.+?)(?:12% PREFERENCE ADDED:|ADJUSTED BASE PRICE:|ADD ALTERNATE|TOTAL BID|BID BOND)",
        text,
        re.I,
    )

    companies: list[str] = []
    if company_block_match:
        raw_block = company_block_match.group(1)
        pieces = [normalize_text(piece) for piece in re.split(r"\*\*+", raw_block) if normalize_text(piece)]
        companies = [re.sub(r"^[*]+", "", piece).strip() for piece in pieces]

    prices = []
    if price_block_match:
        prices = re.findall(r"\$[\d,]+\.\d{2}", price_block_match.group(1))

    if awarded_company and companies and prices:
        award_idx = None
        target = normalize_company(awarded_company)
        for idx, company in enumerate(companies):
            if normalize_company(company) == target:
                award_idx = idx
                break
        if award_idx is None:
            for idx, company in enumerate(companies):
                if target and (target in normalize_company(company) or normalize_company(company) in target):
                    award_idx = idx
                    break
        if award_idx is not None and award_idx < len(prices):
            awarded_amount_text = prices[award_idx]

    if not awarded_amount_text and prices:
        awarded_amount_text = prices[0]

    return awarded_company, awarded_date, parse_amount(awarded_amount_text)


def fetch_final_results(session: requests.Session) -> list[dict]:
    session.get(RESULTS_URL, timeout=120)
    time.sleep(REQUEST_DELAY)

    start = 0
    length = 100
    seen_2026 = False
    results: list[dict] = []

    while True:
        response = session.get(
            RESULTS_API_URL,
            params=build_results_params(start, length),
            timeout=120,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data", [])
        if not rows:
            break

        current_batch = []
        for item in rows:
            title_html = item[1] or ""
            current_batch.append(
                {
                    "quest_number": strip_tags(item[0] or ""),
                    "title": title_from_html(title_html),
                    "detail_url": canonical_detail_url(href_from_html(title_html)),
                    "closing_date": strip_tags(item[2] or ""),
                    "city": strip_tags(item[3] or ""),
                    "county": strip_tags(item[4] or ""),
                    "state": strip_tags(item[5] or ""),
                    "owner": title_from_html(item[6] or ""),
                    "solicitor": title_from_html(item[7] or ""),
                    "posting_type": strip_tags(item[8] or ""),
                    "award_type": strip_tags(item[9] or ""),
                }
            )

        results.extend(current_batch)
        if any(str(TARGET_YEAR) in row["closing_date"] for row in current_batch):
            seen_2026 = True
        if seen_2026 and all(str(TARGET_YEAR) not in row["closing_date"] for row in current_batch):
            break

        start += length
        if start >= payload.get("recordsTotal", 0):
            break
        time.sleep(REQUEST_DELAY)

    return results


def build_result_row(session: requests.Session, item: dict) -> dict | None:
    response = session.get(item["detail_url"], timeout=120)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    metadata, info_link = extract_detail_metadata(soup)
    if not info_link:
        return None

    pdf_response = session.get(info_link, timeout=120)
    pdf_response.raise_for_status()
    awarded_company, pdf_award_date, awarded_amount = extract_pdf_award(pdf_response.content)

    award_date = metadata.get("Award Date", pdf_award_date)
    begin_date = ""
    if award_date:
        try:
            begin_date = datetime.strptime(award_date, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            begin_date = award_date

    if not awarded_company or awarded_amount < MIN_AMOUNT:
        return None

    naics = classify_naics(item["title"])
    if not naics:
        return None

    code, label = naics
    description = (
        f"{item['title']} | Quest {item['quest_number']} | "
        f"Owner: {item['owner']} | Award status: {metadata.get('Award Status', '')}"
    )

    return {
        "city": f"Minnesota - {item['city']}" if item["city"] else "Minnesota",
        "company_name": awarded_company,
        "contact_name": "",
        "phone": "",
        "email": "",
        "address": f"{item['city']}, {item['state']}".strip(", "),
        "website": "",
        "contract_name": f"Quest {item['quest_number']} — {item['title']}",
        "award_amount": f"{awarded_amount:,.2f}",
        "amount_expended": "",
        "begin_date": begin_date,
        "award_link": item["detail_url"],
        "description": description,
        "commodity_type": label,
        "_naics_code": code,
        "_owner_contact": metadata.get("Owner Contact", ""),
        "_owner_phone": metadata.get("Owner Phone", ""),
        "_solicitor_contact": metadata.get("Contact", ""),
        "_solicitor_email": metadata.get("Email", ""),
        "_info_link": info_link,
    }


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

    print(f"Wrote {len(new_results)} Minnesota QuestCDN rows to Google Sheet: {SHEET_NAME}")


def main(preview: bool = False):
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0 Safari/537.36"
            )
        }
    )

    rows = fetch_final_results(session)
    print(f"Fetched {len(rows)} QuestCDN final result rows")

    candidates = []
    for item in rows:
        if item["award_type"] != "Final":
            continue
        if item["posting_type"] != "Construction Project":
            continue
        if str(TARGET_YEAR) not in item["closing_date"]:
            continue
        if not classify_naics(item["title"]):
            continue
        candidates.append(item)

    print(f"{len(candidates)} 2026 construction-like QuestCDN candidates")

    results = []
    for idx, item in enumerate(candidates, start=1):
        print(f"[{idx}/{len(candidates)}] {item['quest_number']} | {item['title']}")
        try:
            row = build_result_row(session, item)
        except Exception as exc:
            print(f"  Failed to process {item['quest_number']}: {exc}")
            continue
        if row:
            results.append(row)
            print(
                f"  -> {row['company_name']} | ${float(str(row['award_amount']).replace(',', '')):,.2f} | "
                f"{row['commodity_type']}"
            )
        time.sleep(REQUEST_DELAY)

    print(f"After filtering: {len(results)} Minnesota QuestCDN award rows")

    if preview:
        for row in results[:20]:
            print(
                f"{row['company_name']} | {row['contract_name']} | {row['award_amount']} | "
                f"{row['begin_date']} | {row['commodity_type']}"
            )
        return

    write_to_google_sheets(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--preview", action="store_true", help="Run without writing to Google Sheets.")
    args = parser.parse_args()
    main(preview=args.preview)
