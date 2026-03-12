# PeopleSoft ERP Supplier Portal Scraping

Use when a state procurement portal runs on Oracle PeopleSoft (common for Kansas, Iowa, and many other state supplier/contract search pages).

## How to identify a PeopleSoft portal
- URL contains `/psp/` or `/psc/` path segments (e.g. `/psp/sokfsprdsup/SUPPLIER/ERP/`)
- Page title or body includes "PeopleSoft" or "Oracle"
- Content loads inside a named iframe called `TargetContent`
- Pagination is done via a `<select>` dropdown (e.g. "1-50 of 250", "51-100 of 250") rather than next/prev buttons

---

## Key pattern: content lives inside an iframe

All interactive content renders inside `iframe[name="TargetContent"]`. You must work through the frame, not the top-level page.

```python
from playwright.sync_api import sync_playwright

PORTAL_URL = "https://supplier.sok.ks.gov/psp/sokfsprdsup/SUPPLIER/ERP/c/KS_SUPPLIER_MENU.KS_PROCR_CNTRCT.GBL"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 1200})
    page.goto(PORTAL_URL, wait_until="networkidle", timeout=120000)
    page.wait_for_timeout(2500)

    # All PeopleSoft content renders here
    frame = page.frame(name="TargetContent")

    # Fill a date filter and submit
    frame.fill("#MY_DATE_FIELD", "01/01/2026")
    frame.click("#SEARCH_BUTTON_ID")
    frame.wait_for_timeout(4000)  # PeopleSoft is slow — wait generously
```

---

## Select-based pagination

PeopleSoft uses a `<select>` dropdown to jump between pages of results. There is no standard "next page" link.

```python
def go_to_next_page(frame, current_start: int, page_size: int = 50) -> bool:
    """
    Advance to the next page of PeopleSoft results using the select dropdown.
    Returns True if navigation succeeded, False if no more pages.
    """
    next_start = current_start + page_size

    # Find the select that has options like "1-50 of 250", "51-100 of 250"
    selects = frame.locator("select").evaluate_all("""nodes =>
        nodes.map(node => ({
            id: node.id,
            name: node.name,
            options: Array.from(node.options).map(opt => ({
                value: opt.value,
                text: opt.textContent.trim()
            }))
        }))
    """)

    for sel in selects:
        # Identify the paginator select by its option text format
        if not any(re.search(r"^\d+-\d+ of \d+", opt["text"]) for opt in sel["options"]):
            continue
        # Find the option for the next page
        target = next(
            (opt for opt in sel["options"] if opt["text"].startswith(f"{next_start}-")),
            None
        )
        if target:
            escaped_id = sel["id"].replace("$", "\\$")
            frame.locator(f"#{escaped_id}").select_option(target["value"])
            frame.wait_for_timeout(3500)  # Wait for page to reload content
            return True

    return False  # No more pages
```

Full pagination loop:
```python
import re

all_rows = []
current_start = 1
page_size = 50

for _ in range(20):  # safety cap
    rows = extract_current_page(frame)
    all_rows.extend(rows)
    moved = go_to_next_page(frame, current_start, page_size)
    if not moved:
        break
    current_start += page_size
```

---

## Text-based row extraction (PeopleSoft grid)

PeopleSoft grids often render as plain text in the DOM, not as clean `<table>` rows. Use `body.innerText` and parse the tab-separated block.

```python
def extract_page_text(frame) -> list[dict]:
    """
    Extract rows from a PeopleSoft results grid using innerText parsing.
    Finds the column header row and reads fixed-width tab-separated data after it.
    """
    body_text = frame.locator("body").inner_text()

    # Locate the header line — adjust to match your portal's column names
    HEADER_MARKER = "Contract Number\tContract Title\tSupplier\tExpire Date\tAgency"
    start = body_text.find(HEADER_MARKER)
    if start == -1:
        return []

    # Find end of data block (PeopleSoft often ends with "info\t" paginator text)
    end = body_text.find("info\t", start)
    block = body_text[start:end] if end > start else body_text[start:]

    lines = [line.replace("\t", " ").strip() for line in block.split("\n") if line.strip()]

    # Skip the header line itself
    header_idx = next((i for i, l in enumerate(lines) if l.startswith("Contract Number")), -1)
    data_lines = lines[header_idx + 1:] if header_idx >= 0 else lines

    rows = []
    i = 0
    while i < len(data_lines):
        # PeopleSoft row numbers are bare integers — use as row anchors
        if not re.match(r"^\d+$", data_lines[i]):
            i += 1
            continue
        chunk = data_lines[i : i + 9]          # adjust column count as needed
        if len(chunk) >= 9:
            rows.append({
                "row_num":        chunk[0],
                "contract_num":   chunk[1],
                "title":          chunk[2],
                "supplier":       chunk[3],
                "expire_date":    chunk[4],
                "agency":         chunk[5],
                "contract_type":  chunk[6],
                "subdivision":    chunk[7],
            })
        i += 9
    return rows
```

---

## Tips
- Always wait generously after clicks (`wait_for_timeout(3500)` minimum — PeopleSoft does full server round-trips)
- Use `frame.wait_for_load_state("networkidle")` after navigation if the portal supports it
- PeopleSoft IDs often contain `$` characters — escape them as `\\$` in CSS selectors
- If the portal has SSO, you may need Tier 4 (Bright Data) to handle the login wall
- Date inputs use `MM/DD/YYYY` format

---

## Real examples in this project
- `ks_contracts_probe.js` — Kansas Supplier Portal (PeopleSoft ERP), reconnaissance probe that mapped the iframe + select-pagination pattern
