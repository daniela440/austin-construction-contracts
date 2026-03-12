"""
Delaware MyMarketplace awarded contracts scraper.

Targets awarded Delaware contracts that:
  - have an effective date in 2026
  - look like construction/public-works work matching the tracked NAICS buckets
  - expose vendor contact information through the public vendor endpoints

Notes:
  - The Delaware portal does not expose a reliable contract dollar amount in its
    contracts grid or detail HTML, so Award Amount is left blank.
  - To avoid small maintenance work, this scraper uses a narrow title/UNSPSC/PDF
    summary filter biased toward heavy civil and larger building projects.
"""

from __future__ import annotations

import html
import io
import json
import re
import sys
import time
from datetime import datetime
from typing import Any

import fitz  # PyMuPDF
import requests
import urllib3
from google.oauth2 import service_account
from googleapiclient.discovery import build

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


GRID_URL = "https://mmp.delaware.gov/Contracts/GetContracts"
DETAIL_MODAL_URL = "https://mmp.delaware.gov/Contracts/GetContractDetail"
DETAIL_PAGE_URL = "https://mmp.delaware.gov/Contracts/Details/{}"
VENDORS_LIST_URL = "https://mmp.delaware.gov/Contracts/GetContractVendorsList"
AGENCY_AWARD_URL = "https://mmp.delaware.gov/Contracts/GetContractAgencyAwardDocumentList"
VENDOR_DETAIL_URL = "https://mmp.delaware.gov/Vendor/GetVendorDetail"
VENDOR_CONTACT_URL = "https://mmp.delaware.gov/Vendor/GetContractVendorContact"

REQUEST_DELAY = 0.4
PDF_DELAY = 0.3
GRID_PAGE_SIZE = 250
TARGET_YEAR = 2026

SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME = "Webscraper Tool for Procurement Sites Two"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept": "application/json, text/html, */*",
})

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

KEEP_UPPER = {"LLC", "LP", "LLP", "LTD", "JV", "II", "III", "IV", "INC", "CO", "DBA"}

EXCLUDE_KEYWORDS = [
    "roof", "roofing", "window replacement", "water infiltration",
    "tank maintenance", "aboveground storage tank", "janitorial",
    "cleaning", "pest", "landscaping maintenance", "grounds maintenance",
    "snow removal", "painting", "striping", "fencing repair",
    "consultant", "consulting", "design services", "architectural and engineering services",
    "engineering services", "a/e design", "analysis", "planning services",
    "inspection services", "testing services", "study", "survey services",
    "classified advertisements", "classified ads", "advertisements",
    "bulk liquified petroleum gas", "bulk lpg", "propane",
    "waste pick-up", "waste pickup", "waste services", "disposal",
    "supply", "materials supply", "used oil", "lubricants", "antifreeze",
    "equipment", "machinery", "collection services", "support services",
    "management services", "advisory services", "platform", "software",
    "audio visual equipment", "web conferencing", "emergency management contractors",
    "environmental investigation", "remediation services", "environmental remediation",
    "remediation", "investigation services",
]

HEAVY_CIVIL_KEYWORDS = [
    "highway", "street", "bridge", "sidewalk", "shared use path", "path",
    "trail", "intersection", "traffic calming", "curb ramp", "drainage",
    "culvert", "roadway", "roundabout", "pavement", "asphalt", "resurfacing",
    "reconstruction", "rehabilitation", "pedestrian", "corridor", "median",
    "signal", "storm sewer", "stormwater", "water main", "sanitary sewer",
    "utility relocation", "multi-use path",
]

COMMERCIAL_BUILDING_KEYWORDS = [
    "construction", "renovation", "rehabilitation", "facility improvements",
    "building improvements", "building renovation", "addition", "community center",
    "school", "library", "fire station", "ems station", "public works facility",
    "maintenance building", "administration building", "center", "campus",
]

ELECTRICAL_KEYWORDS = [
    "electrical", "lighting", "wiring", "generator", "ev charger", "ev charging",
    "traffic signal", "signalization",
]

PLUMBING_HVAC_KEYWORDS = [
    "hvac", "mechanical", "plumbing", "boiler", "chiller", "air conditioning",
    "heating", "cooling", "ventilation", "piping",
]

STRUCTURAL_STEEL_KEYWORDS = [
    "structural steel", "steel erection", "bridge steel", "fabricated steel",
]

PROJECT_SIGNAL_KEYWORDS = [
    "construction", "renovation", "rehabilitation", "replacement", "improvements",
    "upgrade", "upgrades", "installation", "install", "abatement", "cabling",
    "bridge", "road", "street", "sidewalk", "path", "garage", "ramp",
    "signal", "electrical", "hvac", "mechanical", "plumbing", "steel",
]

WORK_DONE_KEYWORDS = [
    "construction", "renovation", "rehabilitation", "replacement", "replace",
    "improvements", "improvement", "upgrade", "upgrades", "installation",
    "install", "abatement", "repair/upgrades", "clearing", "grubbing",
    "relocation", "rehab", "reconstruction", "deck sealing", "mep upgrades",
]

SUPPLY_ONLY_KEYWORDS = [
    "supply", "supplies", "equipment", "machinery", "materials",
    "materials supply", "maintenance equipment", "signal materials",
    "its materials", "petroleum gas", "propane", "oil", "lubricants",
]


def request_json(method: str, url: str, *, params: dict[str, Any] | None = None,
                 json_body: dict[str, Any] | None = None, retries: int = 3) -> dict[str, Any]:
    last_err = None
    for attempt in range(retries):
        try:
            resp = SESSION.request(
                method,
                url,
                params=params,
                json=json_body,
                timeout=45,
                verify=False,
                headers={"Content-Type": "application/json; charset=utf-8"} if json_body is not None else None,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_err = exc
            if attempt < retries - 1:
                time.sleep((attempt + 1) * 2)
    raise RuntimeError(f"{method} {url} failed: {last_err}")


def request_text(url: str, *, params: dict[str, Any] | None = None, retries: int = 3) -> str:
    last_err = None
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=45, verify=False)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_err = exc
            if attempt < retries - 1:
                time.sleep((attempt + 1) * 2)
    raise RuntimeError(f"GET {url} failed: {last_err}")


def request_bytes(url: str, *, retries: int = 3) -> bytes:
    last_err = None
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=60, verify=False)
            resp.raise_for_status()
            return resp.content
        except Exception as exc:
            last_err = exc
            if attempt < retries - 1:
                time.sleep((attempt + 1) * 2)
    raise RuntimeError(f"GET {url} failed: {last_err}")


def clean_text(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def title_case(name: str) -> str:
    parts = []
    for word in re.split(r"(\s+)", name.strip()):
        if not word or word.isspace():
            parts.append(word)
        elif word.upper() in KEEP_UPPER:
            parts.append(word.upper())
        else:
            parts.append(word[:1].upper() + word[1:].lower())
    return "".join(parts)


def parse_date(date_str: str) -> datetime | None:
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def fetch_contract_page(page_num: int, rows: int = GRID_PAGE_SIZE) -> dict[str, Any]:
    return request_json(
        "POST",
        GRID_URL,
        params={"status": "Current"},
        json_body={"page": page_num, "rows": rows, "sidx": "Title", "sord": "asc"},
    )


def fetch_all_contracts() -> list[dict[str, Any]]:
    print("Fetching Delaware awarded contracts grid...")
    first = fetch_contract_page(1)
    total_pages = int(first.get("total") or 1)
    all_rows = list(first.get("rows", []))
    print(f"  Page 1/{total_pages}: {len(all_rows)} rows")

    for page_num in range(2, total_pages + 1):
        page = fetch_contract_page(page_num)
        rows = page.get("rows", [])
        if not rows:
            break
        all_rows.extend(rows)
        print(f"  Page {page_num}/{total_pages}: {len(rows)} rows")
        time.sleep(REQUEST_DELAY)

    deduped = []
    seen_ids = set()
    for row in all_rows:
        rid = row.get("Id")
        if rid and rid not in seen_ids:
            seen_ids.add(rid)
            deduped.append(row)

    print(f"  Retrieved {len(deduped)} unique current contracts")
    return deduped


def extract_vendor_ids(vendors_html: str) -> list[tuple[int, str]]:
    matches = re.findall(
        r"getVendorDetail\((\d+),\s*\d+\).*?>([^<]+)</a>",
        vendors_html,
        re.IGNORECASE | re.DOTALL,
    )
    return [(int(v_id), clean_text(name)) for v_id, name in matches]


def parse_vendor_detail(detail_html: str) -> dict[str, str]:
    def field(label: str) -> str:
        pattern = rf"<strong>{re.escape(label)}</strong>\s*<div>\s*<p>(.*?)</p>"
        m = re.search(pattern, detail_html, re.IGNORECASE | re.DOTALL)
        return clean_text(m.group(1)) if m else ""

    address1 = field("Address")
    address2 = field("Address2")
    city_state_zip = field("City, State, Zip")
    address = ", ".join(part for part in [address1, address2, city_state_zip] if part)

    return {
        "company_name": field("Company Name"),
        "website": field("Website"),
        "address": address,
    }


def parse_contact_card(contact_html: str) -> dict[str, str]:
    def field(label: str) -> str:
        pattern = rf"<strong>{re.escape(label)}:</strong>\s*<p>(.*?)</p>"
        m = re.search(pattern, contact_html, re.IGNORECASE | re.DOTALL)
        return clean_text(m.group(1)) if m else ""

    email_match = re.search(r"mailto:([^\"'>\s]+)", contact_html, re.IGNORECASE)
    return {
        "name": field("Name"),
        "phone": field("Phone"),
        "alt_phone": field("Alternate Phone"),
        "email": email_match.group(1).strip() if email_match else field("Email"),
        "cell": field("Cell"),
        "fax": field("Fax"),
    }


def fetch_vendor_bundle(contract_id: int, vendor_id: int) -> dict[str, str]:
    vendor_detail_html = request_text(
        VENDOR_DETAIL_URL,
        params={"id": vendor_id, "currentContractId": contract_id},
    )
    time.sleep(REQUEST_DELAY)
    primary_html = request_text(
        VENDOR_CONTACT_URL,
        params={"vendorId": vendor_id, "contractId": contract_id, "contactType": "primary"},
    )
    time.sleep(REQUEST_DELAY)
    secondary_html = request_text(
        VENDOR_CONTACT_URL,
        params={"vendorId": vendor_id, "contractId": contract_id, "contactType": "secondary"},
    )
    time.sleep(REQUEST_DELAY)

    detail = parse_vendor_detail(vendor_detail_html)
    primary = parse_contact_card(primary_html)
    secondary = parse_contact_card(secondary_html)

    chosen = primary if any(primary.values()) else secondary
    if not chosen.get("email") and secondary.get("email"):
        chosen["email"] = secondary["email"]
    if not chosen.get("phone"):
        chosen["phone"] = secondary.get("phone") or secondary.get("cell") or ""

    return {
        "company_name": detail.get("company_name", ""),
        "website": detail.get("website", ""),
        "address": detail.get("address", ""),
        "contact_name": chosen.get("name", ""),
        "phone": chosen.get("phone") or chosen.get("cell") or "",
        "email": chosen.get("email", ""),
    }


def extract_award_notice_url(award_html: str) -> str:
    m = re.search(r'href="([^"]+\.pdf)"', award_html, re.IGNORECASE)
    return html.unescape(m.group(1)) if m else ""


def extract_summary_from_pdf(pdf_bytes: bytes) -> str:
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        return ""

    text = "\n".join(page.get_text() for page in doc[: min(len(doc), 4)])
    if not text.strip():
        return ""

    m = re.search(
        r"1\.\s*CONTRACT SUMMARY.*?(?:The .*?)(?=\n\s*2\.\s*CONTRACT PERIOD)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        snippet = m.group(0)
        snippet = re.sub(r"^1\.\s*CONTRACT SUMMARY.*?\)\s*", "", snippet, flags=re.IGNORECASE | re.DOTALL)
        return clean_text(snippet)
    return ""


def extract_contract_contact_email(detail_html: str) -> str:
    m = re.search(r'mailto:([^"\'>\s]+)', detail_html, re.IGNORECASE)
    return m.group(1).strip() if m else ""


def looks_small_or_nonconstruction(text: str) -> bool:
    lower = text.lower()
    if any(word in lower for word in EXCLUDE_KEYWORDS):
        heavy_override = any(word in lower for word in HEAVY_CIVIL_KEYWORDS)
        electrical_override = any(word in lower for word in ELECTRICAL_KEYWORDS)
        plumbing_override = any(word in lower for word in PLUMBING_HVAC_KEYWORDS)
        steel_override = any(word in lower for word in STRUCTURAL_STEEL_KEYWORDS)
        building_override = any(word in lower for word in COMMERCIAL_BUILDING_KEYWORDS)
        if not any([heavy_override, electrical_override, plumbing_override, steel_override, building_override]):
            return True
    return False


def likely_candidate(title: str, unspsc_codes: str, descriptions: list[str]) -> bool:
    text = " ".join([title, unspsc_codes, *descriptions]).lower()
    unspsc_set = {code.strip() for code in unspsc_codes.split(",") if code.strip()}

    if looks_small_or_nonconstruction(text):
        return False

    if any(kw in text for kw in SUPPLY_ONLY_KEYWORDS):
        if not any(kw in text for kw in WORK_DONE_KEYWORDS):
            return False

    # Keep only the construction UNSPSC families that match the tracked NAICS buckets.
    if not unspsc_set.intersection({"7200", "7212", "7214", "7215"}):
        if not any(kw in text for kw in PROJECT_SIGNAL_KEYWORDS):
            return False

    # It must read like actual work being performed, not a one-time purchase or generic service.
    if not any(kw in text for kw in WORK_DONE_KEYWORDS + HEAVY_CIVIL_KEYWORDS):
        if not (
            ("7212" in unspsc_set or "7200" in unspsc_set)
            and any(kw in text for kw in ["school", "building", "garage", "museum", "hall", "facility", "center"])
            and any(kw in text for kw in ["abatement", "renovation", "rehabilitation", "upgrades"])
        ):
            return False

    return any(kw in text for kw in PROJECT_SIGNAL_KEYWORDS + HEAVY_CIVIL_KEYWORDS + COMMERCIAL_BUILDING_KEYWORDS)


def classify_naics(title: str, unspsc_codes: str, descriptions: list[str], summary: str) -> tuple[str, str] | None:
    text = " ".join([title, summary, unspsc_codes, *descriptions]).lower()
    unspsc_set = {code.strip() for code in unspsc_codes.split(",") if code.strip()}

    if looks_small_or_nonconstruction(text):
        return None

    if any(kw in text for kw in SUPPLY_ONLY_KEYWORDS) and not any(kw in text for kw in WORK_DONE_KEYWORDS):
        return None

    if any(kw in text for kw in STRUCTURAL_STEEL_KEYWORDS):
        return "238120", "Structural Steel (NAICS 238120)"

    if any(kw in text for kw in ELECTRICAL_KEYWORDS):
        return "238210", "Electrical (NAICS 238210)"

    if any(kw in text for kw in PLUMBING_HVAC_KEYWORDS):
        return "238220", "Plumbing/HVAC (NAICS 238220)"

    if (
        "7212" in unspsc_set
        or "7200" in unspsc_set
        or re.search(r"\b(building|school|center|facility|garage|museum|hall|library|station)\b", text)
    ):
        if re.search(r"\b(repair|replacement|maintenance)\b", text) and not re.search(
            r"\b(construction|renovation|rehabilitation|addition|improvements|abatement|upgrades|mep)\b", text
        ):
            return None
        return "236220", "Commercial Building (NAICS 236220)"

    if "7214" in unspsc_set or any(kw in text for kw in HEAVY_CIVIL_KEYWORDS):
        return "237310", "Highway/Street/Bridge (NAICS 237310)"

    if "7215" in unspsc_set:
        # Specialized trade only if the title clearly points to one of the tracked NAICS.
        if any(kw in text for kw in ELECTRICAL_KEYWORDS):
            return "238210", "Electrical (NAICS 238210)"
        if any(kw in text for kw in PLUMBING_HVAC_KEYWORDS):
            return "238220", "Plumbing/HVAC (NAICS 238220)"
        if any(kw in text for kw in STRUCTURAL_STEEL_KEYWORDS):
            return "238120", "Structural Steel (NAICS 238120)"
        return None

    return None


def build_description(title: str, agency: str, unspsc_descs: list[str], summary: str, award_notice_url: str) -> str:
    parts = []
    if summary:
        parts.append(summary)
    if agency:
        parts.append(f"Agency: {agency}")
    if unspsc_descs:
        parts.append("UNSPSC: " + ", ".join(d.strip() for d in unspsc_descs if d.strip()))
    if award_notice_url:
        parts.append("Award Notice PDF available")
    return " | ".join(parts)


def scrape_all() -> list[dict[str, str]]:
    contracts = fetch_all_contracts()
    if not contracts:
        return []

    results = []
    seen_pairs = set()
    skipped_year = skipped_scope = skipped_vendors = errors = 0
    pdf_cache: dict[str, str] = {}

    for idx, contract in enumerate(contracts, 1):
        effective = parse_date(contract.get("EffectiveDate", ""))
        if not effective or effective.year != TARGET_YEAR:
            skipped_year += 1
            continue

        contract_id = contract.get("Id")
        title = clean_text(contract.get("Title", ""))
        contract_number = clean_text(contract.get("ContractNumber", ""))
        agency = clean_text(contract.get("AgencyCode", ""))
        unspsc_codes = clean_text(contract.get("BidUnspscCodesString", ""))
        unspsc_descs = [clean_text(x) for x in contract.get("bidUnspscDescripsString") or [] if clean_text(x)]

        if not likely_candidate(title, unspsc_codes, unspsc_descs):
            skipped_scope += 1
            continue

        print(f"  [{idx}/{len(contracts)}] {contract_number} — {title}")

        try:
            detail_html = request_text(DETAIL_MODAL_URL, params={"id": contract_id})
            time.sleep(REQUEST_DELAY)
            vendors_html = request_text(VENDORS_LIST_URL, params={"id": contract_id, "currentCount": 0})
            time.sleep(REQUEST_DELAY)
            award_html = request_text(AGENCY_AWARD_URL, params={"id": contract_id, "currentCount": 0})
            time.sleep(REQUEST_DELAY)
        except Exception as exc:
            print(f"    ERROR fetching contract detail bundle: {exc}")
            errors += 1
            continue

        award_notice_url = extract_award_notice_url(award_html)
        summary = ""
        if award_notice_url:
            if award_notice_url not in pdf_cache:
                try:
                    pdf_cache[award_notice_url] = extract_summary_from_pdf(request_bytes(award_notice_url))
                    time.sleep(PDF_DELAY)
                except Exception as exc:
                    print(f"    WARN award PDF parse failed: {exc}")
                    pdf_cache[award_notice_url] = ""
            summary = pdf_cache[award_notice_url]

        naics = classify_naics(title, unspsc_codes, unspsc_descs, summary)
        if not naics:
            continue

        vendor_ids = extract_vendor_ids(vendors_html)
        if not vendor_ids:
            skipped_vendors += 1
            continue

        contract_contact_email = extract_contract_contact_email(detail_html)
        description = build_description(title, agency, unspsc_descs, summary, award_notice_url)
        award_link = DETAIL_PAGE_URL.format(contract_id)
        contract_name = f"{contract_number} — {title}".strip(" —")

        for vendor_id, fallback_vendor_name in vendor_ids:
            try:
                vendor = fetch_vendor_bundle(contract_id, vendor_id)
            except Exception as exc:
                print(f"    WARN vendor fetch failed for {vendor_id}: {exc}")
                vendor = {
                    "company_name": fallback_vendor_name,
                    "website": "",
                    "address": "",
                    "contact_name": "",
                    "phone": "",
                    "email": "",
                }

            company_name = title_case(vendor.get("company_name") or fallback_vendor_name)
            dedup_key = (contract_id, vendor_id)
            if dedup_key in seen_pairs:
                continue
            seen_pairs.add(dedup_key)

            results.append({
                "city": "Delaware",
                "company_name": company_name,
                "contact_name": vendor.get("contact_name", ""),
                "phone": vendor.get("phone", ""),
                "email": vendor.get("email", "") or contract_contact_email,
                "address": vendor.get("address", ""),
                "website": vendor.get("website", ""),
                "contract_name": contract_name,
                "award_amount": "",
                "amount_expended": "",
                "begin_date": effective.strftime("%Y-%m-%d"),
                "award_link": award_link,
                "description": description,
                "commodity_type": naics[1],
            })

    print("\n--- Summary ---")
    print(f"Total current contracts: {len(contracts)}")
    print(f"Skipped (not 2026):      {skipped_year}")
    print(f"Skipped (scope filter):  {skipped_scope}")
    print(f"Skipped (no vendors):    {skipped_vendors}")
    print(f"Errors:                  {errors}")
    print(f"Matched vendor rows:     {len(results)}")
    return results


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


def write_to_google_sheets(results: list[dict[str, str]]) -> None:
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


def preview_results(results: list[dict[str, str]], limit: int = 10) -> None:
    print(f"\n--- Top {min(limit, len(results))} results ---\n")
    for row in results[:limit]:
        print(f"{row['company_name']} | {row['contract_name']} | {row['begin_date']} | {row['commodity_type']}")
        if row["contact_name"] or row["email"]:
            print(f"  Contact: {row['contact_name']} | {row['phone']} | {row['email']}")
        print(f"  Website: {row['website']}")
        print(f"  Address: {row['address']}")
        print(f"  Description: {row['description'][:220]}")
        print()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Delaware awarded construction contracts")
    parser.add_argument("--preview", action="store_true", help="Preview results without writing to Google Sheets")
    args = parser.parse_args()

    results = scrape_all()
    if not results:
        print("No Delaware contracts matched the filter.")
        sys.exit(0)

    preview_results(results, limit=8)
    if args.preview:
        print("Preview mode only; not writing to Google Sheets.")
        return

    write_to_google_sheets(results)
    print(f"\nView results at: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
