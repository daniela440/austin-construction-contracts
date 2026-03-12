# QuestCDN DataTables Server-Side API

Use when scraping a QuestCDN bid results portal (qcpi.questcdn.com).
QuestCDN uses a jQuery DataTables server-side API that is directly callable — no HTML parsing needed.

## When to use
- Target URL contains `qcpi.questcdn.com/cdn/results/`
- You need awarded/final bid results for a specific provider group
- You want to avoid paginating through the DataTables UI manually

---

## Key discovery: bootstrap the session first

QuestCDN's `/cdn/results_data/` API requires a valid session cookie set by the results page.
Always load the results page in Playwright first, then call the API in the same browser context.

```python
from playwright.sync_api import sync_playwright
import json

GROUP_ID = "6506969"       # Provider/group ID — find in the results page URL
PROVIDER_ID = "6506969"    # Same as group for most public portals
RESULTS_URL = f"https://qcpi.questcdn.com/cdn/results/?group={GROUP_ID}&provider={PROVIDER_ID}&projType=all"
RESULTS_API  = "https://qcpi.questcdn.com/cdn/results_data/"

def build_api_url(start: int, length: int, status_filter: str = "Final") -> str:
    """Build a DataTables server-side API URL for QuestCDN results."""
    from urllib.parse import urlencode
    params = {
        "draw": "1",
        "start": str(start),
        "length": str(length),
        "search[value]": "",
        "search[regex]": "false",
        "group": GROUP_ID,
        "provider": PROVIDER_ID,
        "projType": "all",
    }
    # DataTables column descriptors (10 columns)
    for i in range(10):
        params[f"columns[{i}][data]"] = str(i)
        params[f"columns[{i}][name]"] = ""
        params[f"columns[{i}][searchable]"] = "true"
        params[f"columns[{i}][orderable]"] = "true"
        params[f"columns[{i}][search][value]"] = ""
        params[f"columns[{i}][search][regex]"] = "false"
    params["columns[2][searchable]"] = "false"        # close-date column is not searchable
    params["columns[9][search][value]"] = status_filter  # "Final" = awarded; "" = all
    return f"{RESULTS_API}?{urlencode(params)}"
```

## Pattern: fetch all pages

```python
def fetch_all_results(context, page_size=50, max_records=500):
    """Fetch all QuestCDN results via the DataTables API using an existing Playwright context."""
    all_rows = []

    # First call — get total record count
    page = context.new_page()
    resp = page.goto(build_api_url(0, page_size), wait_until="domcontentloaded", timeout=60000)
    data = json.loads(resp.text())
    page.close()

    total = data.get("recordsFiltered") or data.get("recordsTotal") or 0
    all_rows.extend(data.get("data") or [])

    for start in range(page_size, min(total, max_records), page_size):
        page = context.new_page()
        resp = page.goto(build_api_url(start, page_size), wait_until="domcontentloaded", timeout=60000)
        chunk = json.loads(resp.text())
        page.close()
        all_rows.extend(chunk.get("data") or [])

    return all_rows

# --- Full usage example ---
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        viewport={"width": 1440, "height": 1200},
        accept_downloads=True,
    )

    # REQUIRED: seed a session cookie by visiting the results page first
    seed = context.new_page()
    seed.goto(RESULTS_URL, wait_until="networkidle", timeout=120000)
    seed.wait_for_timeout(1500)
    seed.close()

    rows = fetch_all_results(context, page_size=25, max_records=200)
    browser.close()
```

---

## Response data structure (column index → field)

| Index | Field |
|-------|-------|
| `item[0]` | Quest number (HTML — strip tags) |
| `item[1]` | Title (HTML `<a>` — extract `href` for detail URL, `title` attr for text) |
| `item[2]` | Close/bid date (HTML — strip tags) |
| `item[3]` | City |
| `item[4]` | County |
| `item[5]` | State |
| `item[6]` | Owner (HTML `<a>`) |
| `item[7]` | Solicitor (HTML `<a>`) |
| `item[8]` | Posting type (e.g. "Construction Project") |
| `item[9]` | Award type (e.g. "Final", "Partial") |

Helper to parse HTML columns:
```python
import re
from html import unescape

def strip_tags(html: str) -> str:
    return unescape(re.sub(r"<[^>]+>", " ", html or "")).strip()

def extract_link(html: str, base: str = "https://qcpi.questcdn.com") -> str:
    m = re.search(r'href="([^"]+)"', html or "")
    return (base + m.group(1)) if m and not m.group(1).startswith("http") else (m.group(1) if m else "")

def extract_title(html: str) -> str:
    m = re.search(r'title="([^"]+)"', html or "")
    return unescape(m.group(1).strip()) if m else strip_tags(html)
```

---

## Useful filters (apply client-side after fetching)
```python
TARGET_YEAR = "2026"
MIN_AMOUNT  = 250_000

filtered = [
    row for row in rows
    if strip_tags(row[9]) == "Final"                         # awarded only
    and TARGET_YEAR in strip_tags(row[2])                    # close date in target year
    and strip_tags(row[8]) == "Construction Project"         # construction postings only
]
```

---

## Detail page: award table extraction

```python
def scrape_detail(context, detail_url: str) -> dict:
    page = context.new_page()
    page.goto(detail_url, wait_until="networkidle", timeout=120000)
    page.wait_for_timeout(1500)

    tables = page.locator("table").evaluate_all("""nodes =>
        nodes.map(t =>
            Array.from(t.querySelectorAll("tr")).map(tr =>
                Array.from(tr.querySelectorAll("th,td")).map(td => td.textContent.replace(/\\s+/g," ").trim())
            )
        )
    """)
    page.close()

    for table in tables:
        if not table:
            continue
        header = " | ".join(table[0]).lower()
        if "company" in header and "amount" in header:
            for row in table[1:]:
                if len(row) >= 5:
                    return {
                        "company":  row[0],
                        "contact":  row[1],
                        "phone":    row[2],
                        "email":    row[3],
                        "amount":   row[4],
                        "awarded":  any(re.search(r"✓|check|awarded", c, re.I) for c in row),
                    }
    return {}
```

---

## Finding your GROUP_ID

Look at the results page URL: `?group=XXXXXXX&provider=XXXXXXX`
Each state/agency portal has a different group ID. Examples:
- Minnesota DOA: `6506969`
- Other portals: inspect the results page URL for their group

---

## Real examples in this project
- `mn_questcdn_contracts.py` — Minnesota DOA QuestCDN awards, full pipeline (results API + detail page + PDF extraction)
