"""
Colorado CDOT Bid Tab Archives — Construction Contracts Scraper
Scrapes awarded 2026 construction contracts from CDOT's public bid tab archive page.
Uses Playwright (headless Chrome) because the page requires JS rendering.

Source: https://www.codot.gov/business/bidding/bid-tab-archives
Data per award:
  - Letting date (YYYY-MM-DD)
  - Project code + full description + county
  - Low bidder (company name)
  - Bid tabulation PDF link (Hyland Cloud OnBase)
  - Corrected Bid amount (extracted via Playwright session + PyMuPDF render + macOS Vision OCR)

All CDOT contracts are highway/infrastructure → NAICS 237310.
No contact info is available from this source; Apollo.io handles enrichment downstream.

PDF Amount Extraction:
  The bid tab PDFs use Type3 custom fonts with no ToUnicode mapping, so standard PDF
  text extraction fails. Instead we:
    1. Navigate to the docpop viewer URL in Playwright to establish a session
    2. Intercept the PdfHandler.ashx iframe URL (which contains session tokens)
    3. Download the raw PDF using the same browser context (session cookies carried)
    4. Render page 1 as a PNG using PyMuPDF
    5. OCR with macOS Vision framework (swift subprocess) to extract "Corrected Bid"
  Falls back gracefully (empty amount) if any step fails or swift is unavailable.
"""

import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime

from playwright.sync_api import sync_playwright

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CDOT_BID_TAB_URL = "https://www.codot.gov/business/bidding/bid-tab-archives"

NAICS_LABEL = "Highway/Street/Bridge (NAICS 237310)"

SERVICE_ACCOUNT_FILE = "service-account-key.json"
SPREADSHEET_ID       = "1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ"
SHEET_NAME           = "Webscraper Tool for Procurement Sites Two"

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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_letting_date(date_text):
    """
    Convert 'March 5' or 'February 19' (month + day, no year) → '2026-MM-DD'.
    Year is always 2026 (current scraping target). Injects the year before
    parsing to avoid the Python 3.15 ambiguity deprecation warning.
    """
    date_text = date_text.strip()
    if not date_text:
        return ""
    for fmt in ("%B %d %Y", "%b %d %Y", "%B %d, %Y", "%b %d, %Y"):
        candidate = date_text
        if "%Y" in fmt and not any(str(y) in date_text for y in range(2000, 2100)):
            candidate = f"{date_text} 2026"
        try:
            dt = datetime.strptime(candidate, fmt)
            return f"{dt.year}-{dt.month:02d}-{dt.day:02d}"
        except ValueError:
            continue
    return date_text


def extract_project_code(description):
    """
    Extract the CDOT project number from the description string.
    e.g. 'NHPP 0403-072 (26449) - Region 3...'       → '26449'
         'IM 025A-056 (25466R) - Region 2...'          → '25466R'
         'STM 006A-076 (26267Combo) - Region 4...'     → '26267Combo'
    Falls back to the route/solicitation code before the first ' - '.
    """
    m = re.search(r'\((\d{4,6}[A-Za-z]*)\)', description)
    if m:
        return m.group(1)
    parts = description.split(" - ")
    return parts[0].strip() if parts else description[:30]


# ---------------------------------------------------------------------------
# PDF Amount Extraction (Playwright + PyMuPDF + macOS Vision OCR)
# ---------------------------------------------------------------------------

_SWIFT_OCR_SCRIPT = """\
import Vision
import AppKit

let imgURL = URL(fileURLWithPath: "IMG_PATH")
guard let img = NSImage(contentsOf: imgURL) else { exit(1) }
var imgRect = NSRect(origin: .zero, size: img.size)
guard let cgImg = img.cgImage(forProposedRect: &imgRect, context: nil, hints: nil) else { exit(1) }

var output = ""
let sema = DispatchSemaphore(value: 0)
let req = VNRecognizeTextRequest { req, _ in
    if let results = req.results as? [VNRecognizedTextObservation] {
        for obs in results {
            if let top = obs.topCandidates(1).first { output += top.string + "\\n" }
        }
    }
    sema.signal()
}
req.recognitionLevel = .accurate
let handler = VNImageRequestHandler(cgImage: cgImg)
try? handler.perform([req])
sema.wait()
print(output)
"""


def ocr_amount_from_png(png_path):
    """
    Run macOS Vision OCR on the rendered PDF page PNG and return the
    'Corrected Bid' dollar amount. Returns '' if swift is unavailable or OCR fails.
    """
    if not os.path.exists(png_path):
        return ""

    swift_code = _SWIFT_OCR_SCRIPT.replace("IMG_PATH", png_path)
    try:
        result = subprocess.run(
            ["swift", "/dev/stdin"],
            input=swift_code,
            capture_output=True,
            text=True,
            timeout=45,
        )
        text = result.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return ""

    # The "Low Bid Summary" page has a table with columns:
    #   Estimated Cost | Corrected Bid | Percent of Estimate | Overrun/Underrun
    # The OCR reads the Letting Totals summary row in that column order.
    # So the Corrected Bid is the SECOND dollar amount after "Letting Totals".
    letting_m = re.search(r'Letting\s+Totals([\s\S]+)', text, re.IGNORECASE)
    if letting_m:
        amounts = re.findall(r'\$([\d,]+\.\d{2})', letting_m.group(1))
        if len(amounts) >= 2:
            raw = amounts[1].replace(",", "")
            try:
                amount = float(raw)
                if amount > 10_000:
                    return f"${amount:,.2f}"
            except ValueError:
                pass
        # Fallback: single amount (some PDFs may only show one total)
        elif len(amounts) == 1:
            raw = amounts[0].replace(",", "")
            try:
                amount = float(raw)
                if amount > 10_000:
                    return f"${amount:,.2f}"
            except ValueError:
                pass
    return ""


def fetch_pdf_amount(playwright_ctx, docpop_url):
    """
    Fetch the corrected bid amount from a Hyland Cloud OnBase docpop URL.
    Flow:
      1. Open a new page and navigate to the docpop viewer URL.
      2. Intercept the PdfHandler.ashx request URL (contains session tokens).
      3. Download the raw PDF using the same browser context.
      4. Render page 1 as a PNG using PyMuPDF.
      5. OCR with macOS Vision to extract the "Corrected Bid" amount.
    Returns '$X,XXX,XXX.XX' or '' on any failure.
    """
    if not docpop_url or "hylandcloud" not in docpop_url:
        return ""

    try:
        import fitz  # PyMuPDF
    except ImportError:
        return ""

    pdf_handler_url = []

    page = playwright_ctx.new_page()
    try:
        page.on("request", lambda req: (
            pdf_handler_url.append(req.url)
            if "PdfHandler" in req.url else None
        ))

        page.goto(docpop_url, timeout=30000)
        page.wait_for_timeout(10000)

        if not pdf_handler_url:
            return ""

        # Download PDF using the same context (session cookies are carried over)
        resp = playwright_ctx.request.get(pdf_handler_url[0], timeout=30000)
        pdf_bytes = resp.body()

        if not pdf_bytes[:4] == b"%PDF":
            return ""

    except Exception as e:
        print(f"      [warn] PDF fetch failed: {e}")
        return ""
    finally:
        page.close()

    # Render page 1 to PNG and OCR
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(pdf_bytes)
            pdf_tmp = f.name

        png_tmp = pdf_tmp.replace(".pdf", ".png")
        doc = fitz.open(pdf_tmp)
        pix = doc[0].get_pixmap(matrix=fitz.Matrix(2, 2))
        pix.save(png_tmp)
        doc.close()
        os.unlink(pdf_tmp)

        amount = ocr_amount_from_png(png_tmp)
        os.unlink(png_tmp)
        return amount

    except Exception as e:
        print(f"      [warn] PDF render/OCR failed: {e}")
        return ""


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def scrape_cdot_bid_tabs():
    """
    Load the CDOT bid tab archives page with Playwright, extract 2026 awards,
    then fetch bid amounts from each Hyland Cloud PDF (where available).
    Returns a list of result dicts ready for Google Sheets.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        print(f"Loading {CDOT_BID_TAB_URL} ...")
        page.goto(CDOT_BID_TAB_URL, wait_until="networkidle", timeout=60000)

        # The page is the current year's bid tab page (one table only).
        # Find the first table whose header row contains "Letting Date".
        raw = page.evaluate("""() => {
            const tables = document.querySelectorAll('table');
            let table = null;

            for (const t of tables) {
                const firstRow = t.querySelector('tr');
                if (!firstRow) continue;
                const headerText = firstRow.innerText.toLowerCase();
                if (headerText.includes('letting') || headerText.includes('low bidder')) {
                    table = t;
                    break;
                }
            }

            if (!table && tables.length > 0) table = tables[0];
            if (!table) return { error: 'No bid tab table found on page', rows: [] };

            const rows = [];
            table.querySelectorAll('tr').forEach(tr => {
                const cells = [];
                tr.querySelectorAll('td,th').forEach(cell => {
                    const a = cell.querySelector('a');
                    cells.push({ text: cell.innerText.trim(), href: a ? a.href : '' });
                });
                if (cells.length) rows.push(cells);
            });

            return { error: null, rows: rows };
        }""")

        page.close()

        if raw.get("error"):
            print(f"JS extraction error: {raw['error']}")
            browser.close()
            return []

        rows = raw.get("rows", [])
        print(f"  {len(rows)} rows in 2026 table (including header)")

        if len(rows) < 2:
            print("No data rows found.")
            browser.close()
            return []

        # Parse table rows into contract entries
        entries = []
        for row in rows[1:]:
            if len(row) < 3:
                continue

            letting_date = row[0]["text"]
            project_desc = row[1]["text"]
            low_bidder   = row[2]["text"]
            bid_tab_href = row[3]["href"] if len(row) > 3 else ""

            if not letting_date and not project_desc:
                continue

            if any(x in low_bidder.lower() for x in
                   ["all bids rejected", "rejected", "no award", "no bid"]):
                print(f"  Skipping (rejected): {project_desc[:70]}")
                continue

            if not low_bidder or low_bidder.lower() in ("", "pending", "tbd", "n/a", "-"):
                print(f"  Skipping (pending bidder): {project_desc[:70]}")
                continue

            entries.append({
                "letting_date": letting_date,
                "project_desc": project_desc,
                "low_bidder":   low_bidder,
                "bid_tab_href": bid_tab_href,
            })

        print(f"  {len(entries)} valid awarded contracts")

        # Fetch bid amounts from PDFs
        results = []
        for i, entry in enumerate(entries, 1):
            project_code = extract_project_code(entry["project_desc"])
            begin_date   = parse_letting_date(entry["letting_date"])
            bid_tab_href = entry["bid_tab_href"]

            print(f"  [{i}/{len(entries)}] {project_code:12s} — {entry['low_bidder']}")

            award_amount = ""
            if bid_tab_href:
                award_amount = fetch_pdf_amount(ctx, bid_tab_href)
                if award_amount:
                    print(f"    Amount: {award_amount}")
                else:
                    print(f"    Amount: (unavailable)")

            results.append({
                "city":            "Colorado",
                "company_name":    entry["low_bidder"],
                "contact_name":    "",
                "phone":           "",
                "email":           "",
                "address":         "",
                "website":         "",
                "contract_name":   f"CDOT {project_code}",
                "award_amount":    award_amount,
                "amount_expended": "",
                "begin_date":      begin_date,
                "award_link":      bid_tab_href if bid_tab_href else CDOT_BID_TAB_URL,
                "description":     entry["project_desc"],
                "commodity_type":  NAICS_LABEL,
            })

        browser.close()

    return results


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------

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


def write_to_google_sheets(results):
    """
    Append new results to Google Sheets. Never clears existing data.
    Also back-fills award_amount (col I) for existing rows that had it empty.
    """
    service = get_sheets_service()
    sheet   = service.spreadsheets()

    spreadsheet = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    tab_exists = any(
        s["properties"]["title"] == SHEET_NAME
        for s in spreadsheet.get("sheets", [])
    )
    if not tab_exists:
        sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]},
        ).execute()
        print(f"Created new sheet tab: {SHEET_NAME}")

    existing  = get_existing_data(service)
    start_row = len(existing) + 1

    # Build lookup: (company_name, contract_name) → (row_index, current_amount)
    # row_index is 1-based sheet row number (existing[0] is header = row 1)
    existing_map = {}
    for i, row in enumerate(existing[1:] if len(existing) > 1 else [], start=2):
        if len(row) > 7:
            key = (row[1].strip().lower(), row[7].strip().lower())
            current_amount = row[8].strip() if len(row) > 8 else ""
            existing_map[key] = (i, current_amount)

    existing_fps = set(existing_map.keys())

    new_results = [
        r for r in results
        if (r["company_name"].strip().lower(),
            r["contract_name"].strip().lower()) not in existing_fps
    ]

    # Back-fill amounts for rows that exist but had no amount
    amount_updates = []
    for r in results:
        key = (r["company_name"].strip().lower(), r["contract_name"].strip().lower())
        if key in existing_map:
            row_idx, current_amount = existing_map[key]
            if not current_amount and r.get("award_amount"):
                amount_updates.append((row_idx, r["award_amount"]))

    if amount_updates:
        for row_idx, amount in amount_updates:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{SHEET_NAME}'!I{row_idx}",
                valueInputOption="RAW",
                body={"values": [[amount]]},
            ).execute()
        print(f"  Back-filled amounts for {len(amount_updates)} existing rows")

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

    print(f"Wrote {len(new_results)} new rows to Google Sheet: {SHEET_NAME}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Scrape Colorado CDOT 2026 construction contract awards"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print results without writing to Google Sheets",
    )
    args = parser.parse_args()

    results = scrape_cdot_bid_tabs()

    if not results:
        print("\nNo awarded contracts found.")
        sys.exit(0)

    print(f"\n{len(results)} contracts scraped")

    if args.dry_run:
        print("\n--- Dry Run ---")
        for r in results:
            print(f"  {r['contract_name']:17s} | {r['company_name']:45s} | "
                  f"{r['award_amount'] or '(no amount)':>18s} | {r['begin_date']}")
        return

    write_to_google_sheets(results)
    print(f"\nView: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
