"""
Mississippi MDOT bid-award scraper.

Targets public 2026 MDOT letings where:
  - a Bid Awards PDF exists
  - the award falls into the tracked construction NAICS set
  - the awarded amount is >= $250,000

Data sources:
  - Letting calendar: public HTML with letting dates and Bid Awards PDF links
  - Letting detail page: public HTML with project descriptions keyed by project number
  - Bid Awards PDF: public PDF with awarded contractors and amounts
  - Vendor list: public searchable grid with address / city / state / zip / phone / fax
"""

from __future__ import annotations

import argparse
import io
import re
import time
import warnings
from dataclasses import dataclass
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from google.oauth2 import service_account
from googleapiclient.discovery import build
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from urllib3.exceptions import InsecureRequestWarning

BASE_URL = "https://mdot.ms.gov/applications/bidsystem/"
HOME_URL = f"{BASE_URL}home.aspx"
VENDOR_URL = "https://mdot.ms.gov/applications/BidSystem/Vendors.aspx"

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
    ("237310", "Highway/Street/Bridge (NAICS 237310)", [
        "overlay", "mill", "bridge", "road", "highway", "stripe",
        "striping", "pavement", "paving", "asphalt", "audible stripe",
        "raised pavement markers", "seal", "widen",
    ]),
    ("238210", "Electrical (NAICS 238210)", [
        "signal", "lighting", "electrical", "its", "traffic signal",
    ]),
    ("238220", "Plumbing/HVAC (NAICS 238220)", [
        "mechanical", "hvac", "plumbing", "boiler", "chiller",
    ]),
    ("238120", "Structural Steel (NAICS 238120)", [
        "structural steel", "steel", "metal", "girder",
    ]),
    ("236220", "Commercial Building (NAICS 236220)", [
        "building", "facility", "construction", "renovation",
    ]),
]

EXCLUDE_KEYWORDS = [
    "consultant",
    "supplier only",
    "professional services",
]


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def parse_amount(text: str) -> float:
    cleaned = re.sub(r"[^0-9.]", "", text or "")
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def normalize_company(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def clean_company_candidate(candidate: str, project_label: str = "") -> str:
    candidate = normalize_text(candidate)
    if not candidate:
        return ""

    digit_match = re.search(r"\d", candidate)
    if digit_match:
        candidate = candidate[: digit_match.start()].strip()

    suffix_match = re.search(
        r"([A-Z0-9&.,'()\- ]{2,}?(?:LLC|L\.L\.C\.|INC\.?|COMPANY|CO\.?|GROUP|CORP\.?|CORPORATION))$",
        candidate,
        re.I,
    )
    if suffix_match:
        candidate = suffix_match.group(1).strip()

    lower = candidate.lower()
    project_lower = project_label.lower()
    bad_fragments = [
        " county -",
        " approximately ",
        " project no",
        " known as ",
        " bridge repair",
        " overlay ",
        " widen ",
        " seal ",
        " from ",
        " to ",
        " project completion",
    ]
    if any(fragment in lower for fragment in bad_fragments):
        return ""
    if project_lower and lower and lower in project_lower:
        return ""
    if re.match(r"^[A-Z][a-z]+ [A-Z]{2}$", candidate):
        return ""
    return candidate


def looks_like_company_name(text: str) -> bool:
    lower = (text or "").lower()
    markers = [
        " llc", " inc", " company", " co.", " corp", " corporation",
        " group", " paving", " asphalt", " construction", " contracting",
        " systems", " products", " strategies", " traffic", " key",
    ]
    return any(marker in lower for marker in markers)


def classify_naics(description: str) -> tuple[str, str] | None:
    lower = (description or "").lower()
    if any(keyword in lower for keyword in EXCLUDE_KEYWORDS):
        return None
    for code, label, keywords in NAICS_FILTERS:
        if any(keyword in lower for keyword in keywords):
            return code, label
    return None


@dataclass
class Letting:
    letting_date: str
    letting_type: str
    detail_url: str
    awards_pdf_url: str


@dataclass
class AwardEntry:
    project_ids: list[str]
    project_label: str
    company_name: str
    award_amount: float


def get_session() -> requests.Session:
    warnings.simplefilter("ignore", InsecureRequestWarning)
    session = requests.Session()
    session.verify = False
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0 Safari/537.36"
            )
        }
    )
    return session


def fetch_lettings(session: requests.Session) -> list[Letting]:
    response = session.get(HOME_URL, timeout=120)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    lettings: list[Letting] = []
    table = soup.select_one("#ctl00_content_GridView1")
    if not table:
        return lettings

    for tr in table.select("tr")[1:]:
        cells = tr.find_all("td")
        if len(cells) < 9:
            continue

        letting_date = normalize_text(cells[0].get_text(" ", strip=True))
        if str(TARGET_YEAR) not in letting_date:
            continue

        detail_link = cells[0].find("a", href=True)
        awards_link = cells[8].find("a", href=True)
        if not detail_link or not awards_link:
            continue

        lettings.append(
            Letting(
                letting_date=letting_date,
                letting_type=normalize_text(cells[1].get_text(" ", strip=True)),
                detail_url=requests.compat.urljoin(HOME_URL, detail_link["href"]),
                awards_pdf_url=requests.compat.urljoin(HOME_URL, awards_link["href"]),
            )
        )
    return lettings


def fetch_project_descriptions(session: requests.Session, detail_url: str) -> dict[str, str]:
    response = session.get(detail_url, timeout=120)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    descriptions: dict[str, str] = {}
    table = soup.select_one("#ctl00_content_GridView1")
    if not table:
        return descriptions

    for tr in table.select("tr")[1:]:
        cells = tr.find_all("td")
        if len(cells) < 2:
            continue
        description = normalize_text(cells[1].get_text(" ", strip=True))
        if not description:
            continue
        for project_id in re.findall(r"/\s*(\d{9})", description):
            descriptions[project_id] = description
    return descriptions


def parse_awards_pdf(pdf_bytes: bytes, project_descriptions: dict[str, str]) -> tuple[str, list[AwardEntry]]:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    pages = [page.extract_text() or "" for page in reader.pages]
    full_text = "\n".join(pages)

    award_date = ""
    award_date_match = re.search(
        r"ON\s+([A-Z][a-z]+ \d{1,2}, \d{4})\s*,\s*THE COMMISSION TOOK ACTION",
        full_text,
        re.I,
    )
    if award_date_match:
        try:
            award_date = datetime.strptime(award_date_match.group(1), "%B %d, %Y").strftime("%Y-%m-%d")
        except ValueError:
            award_date = award_date_match.group(1)

    entries: list[AwardEntry] = []
    current_project_line = ""
    current_company = ""
    current_project_label = ""

    def flush_entry(amount_line: str):
        nonlocal current_project_line, current_company, current_project_label
        if not current_project_line:
            return

        amount_match = re.search(r"\$[\d,]+\.\d{2}", amount_line)
        if not amount_match:
            return

        amount = parse_amount(amount_match.group(0))
        pre_amount = normalize_text(amount_line[: amount_match.start()])
        company = clean_company_candidate(current_company, current_project_label)
        if not company:
            company = clean_company_candidate(pre_amount, current_project_label)
        company = re.sub(r"\s*MISSISSIPPI DEPARTMENT OF TRANSPORTATION.*$", "", company, flags=re.I).strip()
        if not company:
            return

        project_ids = re.findall(r"/\s*(\d{9})", current_project_line)
        if not project_ids:
            return

        descriptions = [project_descriptions.get(pid, "") for pid in project_ids if project_descriptions.get(pid)]
        project_label = " | ".join(dict.fromkeys(descriptions)) if descriptions else current_project_line
        entries.append(
            AwardEntry(
                project_ids=project_ids,
                project_label=project_label,
                company_name=company,
                award_amount=amount,
            )
        )
        current_project_line = ""
        current_company = ""
        current_project_label = ""

    for page_text in pages:
        lines = [line.strip() for line in page_text.splitlines() if line.strip()]
        for line in lines:
            if line.startswith("POSTPONED, WITHDRAWN, REJECTED"):
                return award_date, entries
            if line in {"AWARDED"} or line.startswith("TOTAL AWARDED"):
                continue
            if line.startswith("$") or line.startswith("MISSISSIPPI DEPARTMENT OF TRANSPORTATION"):
                continue
            if re.search(r"/\s*\d{9}", line):
                current_project_line = normalize_text(line)
                project_ids = re.findall(r"/\s*(\d{9})", current_project_line)
                current_project_label = " | ".join(
                    dict.fromkeys(project_descriptions.get(pid, "") for pid in project_ids if project_descriptions.get(pid))
                )
                current_company = ""
                continue
            if not current_project_line:
                continue
            if "$" in line:
                flush_entry(line)
                continue
            line_norm = normalize_text(line)
            if current_project_label and line_norm.lower() in current_project_label.lower():
                continue
            desc_match = any(pid in line for pid in project_descriptions.keys())
            if desc_match or re.match(r"^[A-Z][A-Z\s&,\-/.()0-9]+COUNTY", line):
                continue
            if not current_company:
                current_company = line_norm
            else:
                current_company = normalize_text(f"{current_company} {line_norm}")

    return award_date, entries


def lookup_vendors(company_names: list[str]) -> dict[str, dict[str, str]]:
    results: dict[str, dict[str, str]] = {}
    unique_names = list(dict.fromkeys(company_names))
    if not unique_names:
        return results

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1200})
        page.goto(VENDOR_URL, wait_until="networkidle", timeout=120000)
        page.wait_for_timeout(1500)

        name_input = page.locator("#ctl00_content_ASPxGridView1_DXFREditorcol1_I")
        for name in unique_names:
            try:
                name_input.fill("")
                page.wait_for_timeout(300)
                name_input.fill(name)
                name_input.press("Enter")
                page.wait_for_timeout(3500)
            except PlaywrightTimeoutError:
                continue

            rows = page.locator("tr[id^='ctl00_content_ASPxGridView1_DXDataRow']")
            row_count = min(rows.count(), 5)
            target = normalize_company(name)

            for idx in range(row_count):
                cells = rows.nth(idx).locator("td")
                values = [normalize_text(cells.nth(i).inner_text()) for i in range(cells.count())]
                if len(values) < 7:
                    continue
                row_name = values[1]
                row_norm = normalize_company(row_name)
                if row_norm == target or target in row_norm or row_norm in target:
                    results[name] = {
                        "company_name": row_name,
                        "address": values[2],
                        "city": values[3],
                        "state": values[4],
                        "zip": values[5],
                        "phone": values[6],
                        "fax": values[7] if len(values) > 7 else "",
                    }
                    break
        browser.close()

    return results


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

    print(f"Wrote {len(new_results)} Mississippi MDOT rows to Google Sheet: {SHEET_NAME}")


def main(preview: bool = False):
    session = get_session()
    lettings = fetch_lettings(session)
    print(f"Found {len(lettings)} Mississippi 2026 lettings with Bid Awards PDFs")

    award_rows = []
    for idx, letting in enumerate(lettings, start=1):
        print(f"[{idx}/{len(lettings)}] {letting.letting_date}")
        try:
            descriptions = fetch_project_descriptions(session, letting.detail_url)
            pdf_response = session.get(letting.awards_pdf_url, timeout=120)
            pdf_response.raise_for_status()
            award_date, entries = parse_awards_pdf(pdf_response.content, descriptions)
        except Exception as exc:
            print(f"  Failed letting {letting.letting_date}: {exc}")
            continue

        for entry in entries:
            if entry.award_amount < MIN_AMOUNT:
                continue
            naics = classify_naics(entry.project_label)
            if not naics:
                continue
            code, label = naics
            award_rows.append(
                {
                    "letting_date": letting.letting_date,
                    "award_date": award_date,
                    "company_name": entry.company_name,
                    "project_ids": entry.project_ids,
                    "project_label": entry.project_label,
                    "award_amount": entry.award_amount,
                    "commodity_type": label,
                    "_naics_code": code,
                    "_award_pdf": letting.awards_pdf_url,
                }
            )
        time.sleep(REQUEST_DELAY)

    print(f"After award parsing/filtering: {len(award_rows)} Mississippi rows")

    vendor_map = lookup_vendors([row["company_name"] for row in award_rows])
    print(f"Vendor matches found: {len(vendor_map)}")

    results = []
    for row in award_rows:
        vendor = vendor_map.get(row["company_name"], {})
        if not vendor and not looks_like_company_name(row["company_name"]):
            continue
        city = vendor.get("city", "")
        state = vendor.get("state", "MS" if city else "")
        city_label = f"Mississippi - {city}" if city and state == "MS" else (f"{state} - {city}" if city and state else city)
        contract_id = " & ".join(row["project_ids"])
        begin_date = row["award_date"] or ""
        description = f"{row['project_label']} | Letting: {row['letting_date']}"

        results.append(
            {
                "city": city_label,
                "company_name": vendor.get("company_name", row["company_name"]),
                "contact_name": "",
                "phone": vendor.get("phone", ""),
                "email": "",
                "address": " ".join(
                    part for part in [
                        vendor.get("address", ""),
                        ", ".join(part for part in [vendor.get("city", ""), vendor.get("state", "")] if part),
                        vendor.get("zip", ""),
                    ] if part
                ).strip(),
                "website": "",
                "contract_name": f"MDOT {contract_id} — {row['project_label']}",
                "award_amount": f"{row['award_amount']:,.2f}",
                "amount_expended": "",
                "begin_date": begin_date,
                "award_link": row["_award_pdf"],
                "description": description,
                "commodity_type": row["commodity_type"],
            }
        )

    if preview:
        for row in results[:25]:
            print(
                f"{row['company_name']} | {row['contract_name']} | {row['award_amount']} | "
                f"{row['begin_date']} | {row['city']} | {row['phone']}"
            )
        return

    write_to_google_sheets(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--preview", action="store_true", help="Run without writing to Google Sheets.")
    args = parser.parse_args()
    main(preview=args.preview)
