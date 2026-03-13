"""
Montana MDT award-sheet scraper.

Scrapes the 2026 awards PDF and appends awarded contractors to the shared
Google Sheet using the same 14-column schema as the other procurement scrapers.

This scraper is intentionally PDF-side only. Company contact enrichment is left
to the existing downstream enrichment workflow.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

import fitz  # PyMuPDF
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

CONTRACTING_URL = "https://mdt.mt.gov/business/contracting/"
FTP_CONTRACTING_URL = "https://ftp.mdt.mt.gov/business/contracting/default.aspx"
PDF_URL = (
    "https://mdt.mt.gov/other/webdata/external/contractplans/contract/archives/"
    "AWARD_SHEETS/awardsheets-2026.pdf"
)

SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

SHEET_FIELDS = [
    "city",
    "company_name",
    "contact_name",
    "phone",
    "email",
    "address",
    "website",
    "contract_name",
    "award_amount",
    "amount_expended",
    "begin_date",
    "award_link",
    "description",
    "commodity_type",
]
SHEET_HEADERS = [
    "City",
    "Company Name",
    "Contact Name",
    "Phone",
    "Email",
    "Address",
    "Website",
    "Contract Name",
    "Award Amount",
    "Amount Expended",
    "Begin Date",
    "Award Link",
    "Project Description",
    "Commodity Type",
]

STATE_ABBRS = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI",
    "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN",
    "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH",
    "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA",
    "WI", "WV", "WY",
}
COMPANY_END_MARKERS = {
    "INC", "INC.", "LLC", "L.L.C.", "CO", "CO.", "COMPANY", "CORP", "CORP.",
    "CORPORATION", "LTD", "LTD.", "LP", "L.P.", "LLP", "L.L.P.", "PLC", "PC",
    "P.C.", "JV", "JV.", "PARTNERS", "PARTNERSHIP", "CONSTRUCTION",
    "CONTRACTING", "PAVING", "EXCAVATING", "ASPHALT", "BUILDERS", "CONCRETE",
    "ELECTRIC", "ELECTRICAL", "ENTERPRISES", "ENTERPRISE", "SERVICES",
    "SYSTEMS", "INDUSTRIES", "GROUP",
}
IGNORE_LINE_PATTERNS = (
    "engineer's estimate",
    "engineers estimate",
    "apparent low bidder",
    "page ",
    "bidder",
    "awarded company",
)

NAICS_FILTERS = [
    ("238210", "Electrical (NAICS 238210)", [
        "signal", "lighting", "electrical", "illuminat", "traffic control",
        "fiber", "its ", "intelligent transportation",
    ]),
    ("238220", "Plumbing/HVAC (NAICS 238220)", [
        "plumbing", "hvac", "mechanical", "water line", "sewer",
    ]),
    ("238120", "Structural Steel (NAICS 238120)", [
        "steel", "girder", "metal", "truss", "structural",
    ]),
    ("236220", "Commercial Building (NAICS 236220)", [
        "building", "facility", "shop", "office", "renovation", "maintenance",
    ]),
    ("237310", "Highway/Street/Bridge (NAICS 237310)", [
        "bridge", "road", "highway", "paving", "resurfacing", "overlay",
        "intersection", "guardrail", "striping", "asphalt", "concrete",
        "culvert", "grading", "drainage",
    ]),
]


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def parse_amount(text: str) -> float:
    cleaned = re.sub(r"[^0-9.]", "", text or "")
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def title_case_company(name: str) -> str:
    if not name:
        return ""
    parts = name.title().split()
    keep_upper = {"LLC", "L.L.C.", "LP", "L.P.", "LLP", "L.L.P.", "JV", "PC", "DBA"}
    return " ".join(
        part.upper() if part.upper().rstrip(".,") in keep_upper else part
        for part in parts
    )


def match_naics(text: str) -> str:
    lower = (text or "").lower()
    for _code, label, keywords in NAICS_FILTERS:
        if any(keyword in lower for keyword in keywords):
            return label
    return "Highway/Street/Bridge (NAICS 237310)"


def parse_date(text: str) -> str:
    if not text:
        return ""
    cleaned = normalize_text(text.replace(".", ""))
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
            ),
            "Accept": "application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://mdt.mt.gov/business/contracting/awarded-bid-packages.aspx",
        }
    )
    return session


def download_pdf(url: str) -> bytes:
    response = get_session().get(url, timeout=120)
    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "").lower()
    if "pdf" not in content_type and response.content[:5] != b"%PDF-":
        raise RuntimeError("MDT did not return a PDF document.")
    return response.content


def download_pdf_via_playwright(
    headed: bool = False,
    chrome_user_data_dir: str | None = None,
    chrome_profile_dir: str = "Default",
) -> bytes:
    intercepted: list[bytes] = []

    with sync_playwright() as p:
        if chrome_user_data_dir:
            context = p.chromium.launch_persistent_context(
                user_data_dir=chrome_user_data_dir,
                channel="chrome",
                headless=not headed,
                args=[f"--profile-directory={chrome_profile_dir}"],
                viewport={"width": 1440, "height": 1200},
            )
            browser = None
        else:
            browser = p.chromium.launch(headless=not headed)
            context = browser.new_context(viewport={"width": 1440, "height": 1200})

        try:
            page = context.pages[0] if context.pages else context.new_page()

            def handle_response(resp):
                url_lower = resp.url.lower()
                if "awardsheets-2026.pdf" not in url_lower and not url_lower.endswith(".pdf"):
                    return
                try:
                    body = resp.body()
                except Exception:
                    return
                if body[:5] == b"%PDF-":
                    intercepted.append(body)

            page.on("response", handle_response)

            for start_url in (CONTRACTING_URL, FTP_CONTRACTING_URL):
                try:
                    page.goto(start_url, wait_until="domcontentloaded", timeout=120000)
                    page.wait_for_timeout(3000)
                except PlaywrightTimeoutError:
                    continue

                for selector in (
                    "a:has-text('Award Sheets 2026')",
                    "a[href*='awardsheets-2026.pdf']",
                    "a:has-text('Awarded Contracts')",
                ):
                    try:
                        locator = page.locator(selector).first
                        if locator.count() == 0:
                            continue
                        locator.click()
                        page.wait_for_timeout(5000)
                        if intercepted:
                            return intercepted[0]
                    except Exception:
                        continue

                try:
                    response = context.request.get(PDF_URL, timeout=60000)
                    body = response.body()
                    if body[:5] == b"%PDF-":
                        return body
                except Exception:
                    pass

                if intercepted:
                    return intercepted[0]
        finally:
            context.close()
            if browser:
                browser.close()

    raise RuntimeError("Playwright browser session did not receive the Montana awards PDF.")


def extract_pages(pdf_bytes: bytes) -> list[str]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        return [page.get_text("text", sort=True) for page in doc]
    finally:
        doc.close()


def extract_inline_field(text: str, label: str, stop_labels: list[str] | None = None) -> str:
    stop_labels = stop_labels or []
    pattern = re.escape(label) + r"\s*(.+)"
    match = re.search(pattern, text, re.I)
    if not match:
        return ""

    value = match.group(1)
    for stop in stop_labels:
        value = re.split(re.escape(stop), value, maxsplit=1, flags=re.I)[0]
    return normalize_text(value)


def extract_block_value(lines: list[str], label: str, stop_prefixes: tuple[str, ...]) -> str:
    label_lower = label.lower()
    for idx, line in enumerate(lines):
        normalized = normalize_text(line)
        lower = normalized.lower()
        if lower == label_lower:
            collected: list[str] = []
            for follow in lines[idx + 1:]:
                follow_norm = normalize_text(follow)
                if not follow_norm:
                    if collected:
                        break
                    continue
                if follow_norm.lower().startswith(stop_prefixes):
                    break
                collected.append(follow_norm)
            return normalize_text(" ".join(collected))

        if lower.startswith(f"{label_lower} "):
            remainder = normalized[len(label):].strip(" :")
            return normalize_text(remainder)
    return ""


def trim_trailing_location(text: str) -> str:
    tokens = text.split()
    if len(tokens) < 3 or tokens[-1].strip(".,") not in STATE_ABBRS:
        return text.strip(" ,")

    company_cutoff = None
    for idx, token in enumerate(tokens[:-1]):
        cleaned = token.strip(",.()").upper()
        if cleaned in COMPANY_END_MARKERS:
            company_cutoff = idx

    if company_cutoff is not None:
        tokens = tokens[: company_cutoff + 1]
    else:
        tokens = tokens[:-2]
    return " ".join(tokens).strip(" ,")


def parse_winner_line(line: str) -> tuple[str, float] | None:
    line_norm = normalize_text(line)
    lower = line_norm.lower()
    if not line_norm or any(fragment in lower for fragment in IGNORE_LINE_PATTERNS):
        return None

    amount_match = re.search(r"\$[\d,]+\.\d{2}", line_norm)
    if not amount_match or "*" not in line_norm:
        return None

    amount = parse_amount(amount_match.group(0))
    if amount <= 0:
        return None

    remainder = normalize_text(line_norm[amount_match.end():].replace("*", " "))
    if not remainder:
        return None

    remainder = trim_trailing_location(remainder)
    if not remainder:
        return None

    return title_case_company(remainder), amount


@dataclass
class AwardRow:
    contract_id: str
    call_no: str
    letting_date: str
    award_date: str
    company_name: str
    award_amount: float
    description: str
    county: str


def parse_awards(pdf_bytes: bytes) -> list[AwardRow]:
    pages = extract_pages(pdf_bytes)
    rows: list[AwardRow] = []

    for page_text in pages:
        if not page_text.strip():
            continue

        lines = [line.strip() for line in page_text.splitlines() if line.strip()]
        joined = "\n".join(lines)

        contract_id = extract_inline_field(joined, "Contract:", ["Call No:", "For Letting of", "Award Date:"])
        call_no = extract_inline_field(joined, "Call No:", ["For Letting of", "Award Date:", "Contract:"])
        letting_date = extract_inline_field(joined, "For Letting of", ["Award Date:", "Contract:", "Call No:"])
        award_date = extract_inline_field(joined, "Award Date:", ["Contract:", "Call No:", "For Letting of"])

        description = extract_block_value(
            lines,
            "Project:",
            (
                "county:",
                "district:",
                "engineer's estimate",
                "engineers estimate",
                "bidder",
                "award date:",
                "contract:",
                "call no:",
                "for letting of",
            ),
        )
        county = extract_block_value(
            lines,
            "County:",
            (
                "district:",
                "project:",
                "engineer's estimate",
                "engineers estimate",
                "bidder",
                "award date:",
                "contract:",
                "call no:",
                "for letting of",
            ),
        )

        for line in lines:
            winner = parse_winner_line(line)
            if not winner:
                continue

            company_name, award_amount = winner
            rows.append(
                AwardRow(
                    contract_id=contract_id or "Unknown Contract",
                    call_no=call_no,
                    letting_date=parse_date(letting_date),
                    award_date=parse_date(award_date),
                    company_name=company_name,
                    award_amount=award_amount,
                    description=description,
                    county=county,
                )
            )

    return rows


def transform_results(raw_rows: list[AwardRow]) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for row in raw_rows:
        contract_name = (
            f"MDT {row.contract_id} (Call {row.call_no})"
            if row.call_no
            else f"MDT {row.contract_id}"
        )
        dedup_key = (row.company_name.lower(), contract_name.lower())
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        description_parts = [part for part in [row.description, f"County: {row.county}" if row.county else "", f"Letting: {row.letting_date}" if row.letting_date else ""] if part]
        full_description = " | ".join(description_parts) if description_parts else contract_name

        county_label = row.county.title() if row.county else ""
        city_label = f"Montana ({county_label})" if county_label else "Montana"

        results.append(
            {
                "city": city_label,
                "company_name": row.company_name,
                "contact_name": "",
                "phone": "",
                "email": "",
                "address": "",
                "website": "",
                "contract_name": contract_name,
                "award_amount": row.award_amount,
                "amount_expended": "",
                "begin_date": row.award_date or row.letting_date,
                "award_link": PDF_URL,
                "description": full_description,
                "commodity_type": match_naics(full_description),
            }
        )

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


def write_to_google_sheets(results: list[dict[str, str]]) -> None:
    service = get_sheets_service()
    sheet = service.spreadsheets()

    spreadsheet = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id = None
    for current_sheet in spreadsheet.get("sheets", []):
        if current_sheet["properties"]["title"] == SHEET_NAME:
            sheet_id = current_sheet["properties"]["sheetId"]
            break

    if sheet_id is None:
        sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]},
        ).execute()
        print(f"Created new sheet: {SHEET_NAME}")

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

    if len(new_results) < len(results):
        print(f"Skipped {len(results) - len(new_results)} already-existing entries")

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

    print(f"Wrote {len(new_results)} Montana MDT rows to Google Sheet: {SHEET_NAME}")


def preview_results(results: list[dict[str, str]], limit: int = 10) -> None:
    print(f"\n--- Preview (first {min(limit, len(results))} results) ---\n")
    for idx, row in enumerate(results[:limit], start=1):
        print(f"{idx}. {row['company_name']}")
        print(f"   {row['contract_name']} | ${float(row['award_amount']):,.2f}")
        print(f"   {row['city']} | {row['begin_date']}")
        print(f"   {row['description'][:120]}")
        print()


def main(
    preview: bool = False,
    headed: bool = False,
    chrome_user_data_dir: str | None = None,
    chrome_profile_dir: str = "Default",
) -> None:
    try:
        pdf_bytes = download_pdf(PDF_URL)
        print("Fetched Montana awards PDF via direct HTTP")
    except Exception as direct_exc:
        print(f"Direct PDF fetch failed: {direct_exc}")
        print("Falling back to Playwright browser session...")
        pdf_bytes = download_pdf_via_playwright(
            headed=headed,
            chrome_user_data_dir=chrome_user_data_dir,
            chrome_profile_dir=chrome_profile_dir,
        )
        print("Fetched Montana awards PDF via Playwright")

    raw_rows = parse_awards(pdf_bytes)
    print(f"Parsed {len(raw_rows)} awarded Montana rows from the 2026 award sheet")

    results = transform_results(raw_rows)
    print(f"After dedup/transform: {len(results)} rows")

    if preview:
        preview_results(results)
        print("Preview mode — not writing to sheet.")
        return

    write_to_google_sheets(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Montana MDT award-sheet scraper")
    parser.add_argument("--preview", action="store_true", help="Preview results without writing to Google Sheets")
    parser.add_argument("--headed", action="store_true", help="Use a visible browser for the Playwright fallback")
    parser.add_argument(
        "--chrome-user-data-dir",
        default=None,
        help="Optional Chrome user data dir for a persistent real-browser session",
    )
    parser.add_argument(
        "--chrome-profile-dir",
        default="Default",
        help="Chrome profile directory name when using --chrome-user-data-dir",
    )
    args = parser.parse_args()
    chrome_user_data_dir = args.chrome_user_data_dir
    if chrome_user_data_dir == "auto":
        chrome_user_data_dir = str(Path.home() / "Library/Application Support/Google/Chrome")
    main(
        preview=args.preview,
        headed=args.headed,
        chrome_user_data_dir=chrome_user_data_dir,
        chrome_profile_dir=args.chrome_profile_dir,
    )
