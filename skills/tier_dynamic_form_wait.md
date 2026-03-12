# Dynamic Form Wait & Fallback Extraction

Use when a government portal's search form uses JavaScript to render results, and standard
`wait_for_load_state("networkidle")` is not reliable (results appear after a JS callback
rather than a network request, or the page never truly reaches "networkidle").

---

## Pattern A: Poll body text for a known result marker

When a form submission updates the DOM with a recognizable string (e.g. "Contracts Found (42)"),
poll `body.innerText` in a loop instead of relying on network idle.

```python
from playwright.sync_api import sync_playwright

RESULT_MARKER = "Contracts Found ("
NOT_YET_MARKER = "Select your criteria and search."  # text shown before results load

def wait_for_results(page, max_attempts: int = 30, interval_ms: int = 1000) -> str:
    """
    Poll page body text until result marker appears or timeout.
    Returns the full body text when results are ready.
    """
    for _ in range(max_attempts):
        text = page.locator("body").inner_text()
        if RESULT_MARKER in text and NOT_YET_MARKER not in text:
            return text
        page.wait_for_timeout(interval_ms)
    return page.locator("body").inner_text()  # return whatever we have

# Usage
page.locator('input[name="amountFrom"]').fill("250000")
page.locator('textarea[name="comments"]').fill("electrical")
page.get_by_role("button", name="Search").click()

body_text = wait_for_results(page)
found_line = next((line for line in body_text.split("\n") if "Contracts Found" in line), "")
print(found_line)  # e.g. "Contracts Found (17)"
```

Adapt `RESULT_MARKER` and `NOT_YET_MARKER` to the specific portal.
Common patterns to look for:
- `"X records found"`, `"Showing X results"`, `"X Contracts Found"`
- Absence of a loading spinner: `"loading"`, `"Please wait"`

---

## Pattern B: Table-first extraction with innerText fallback

Some government CMS sites render results as `<table>` rows, but the table structure may be
missing or inconsistent. This two-pass approach tries `<table>` rows first, then falls back
to parsing `body.innerText` line-by-line using date/status patterns as anchors.

```python
import re

def extract_table_rows(page) -> list[dict]:
    """
    Two-pass extraction: structured <table> first, raw innerText fallback.
    Tuned for list pages that show: date | winner | municipality | summary | status | posted_date
    """

    # --- Pass 1: standard table rows ---
    rows = page.evaluate("""() => {
        function norm(t) { return (t || "").replace(/\\s+/g, " ").trim(); }
        function abs(href) {
            try { return new URL(href, window.location.href).href; } catch { return href || ""; }
        }
        const out = [];
        for (const tr of document.querySelectorAll("table tbody tr")) {
            const cells = Array.from(tr.querySelectorAll("td")).map(td => norm(td.textContent));
            if (cells.length < 5) continue;
            const link = tr.querySelector("a");
            out.push({
                col0: cells[0], col1: cells[1], col2: cells[2],
                col3: cells[3], col4: cells[4], col5: cells[5] || "",
                detailUrl: link ? abs(link.getAttribute("href")) : ""
            });
        }
        return out;
    }""")

    if rows:
        return rows

    # --- Pass 2: innerText line scanning ---
    body_text = page.locator("body").inner_text()
    lines = [line.strip() for line in body_text.split("\n") if line.strip()]

    # Collect all links for later matching
    anchors = page.evaluate("""() =>
        Array.from(document.querySelectorAll("a"))
            .map(a => ({
                text: (a.textContent || "").replace(/\\s+/g, " ").trim(),
                href: a.href
            }))
            .filter(a => a.href && a.text)
    """)

    DATE_PATTERN   = re.compile(r"\d{2}/\d{2}/\d{4}")
    STATUS_PATTERN = re.compile(r"Opened|Awarded|Pending|Final", re.I)

    out = []
    for i in range(len(lines) - 5):
        if not DATE_PATTERN.search(lines[i]):
            continue
        if not STATUS_PATTERN.search(lines[i + 4]):
            continue

        summary = lines[i + 3]
        link = next(
            (a for a in anchors if a["text"] in summary or summary in a["text"]),
            None
        )
        out.append({
            "col0": lines[i],       # date
            "col1": lines[i + 1],   # winner / bidder
            "col2": lines[i + 2],   # municipality
            "col3": summary,        # description
            "col4": lines[i + 4],   # status
            "col5": lines[i + 5] if i + 5 < len(lines) else "",
            "detailUrl": link["href"] if link else "",
        })
    return out
```

---

## Pattern C: Seen-page deduplication for paginated results

When clicking through pages with `next` links, page markers can help prevent infinite loops
if the paginator becomes disabled mid-scrape.

```python
def scrape_all_pages(page, max_pages: int = 10) -> list[dict]:
    all_rows = []
    seen_markers = set()

    for _ in range(max_pages):
        # Use the paginator's current-page text as a unique marker
        marker = page.locator(".ui-paginator-current").first.inner_text(timeout=2000) or ""
        if marker in seen_markers:
            break
        seen_markers.add(marker)

        all_rows.extend(extract_table_rows(page))

        next_btn = page.locator(
            "a[rel='next'], .pager__item--next a, .ui-paginator-next:not(.ui-state-disabled)"
        ).first
        if next_btn.count() == 0:
            break
        next_btn.click()
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1200)

    return all_rows
```

---

## Tips
- For Pattern A: set `max_attempts` generously (30 × 1s = 30 seconds); some state portals are slow
- For Pattern B: tune `DATE_PATTERN` and `STATUS_PATTERN` to the portal's specific text format
- Combine Pattern A (wait for load) + Pattern B (extract) when the page is dynamic AND the DOM is inconsistent
- Always add deduplication (`seen` set on URL or a composite key) when paginating — some portals repeat rows

---

## Real examples in this project
- `in_contractsearch_probe.js` — Indiana IDOA contract search; uses body-text polling to detect when results load
- `me_awarded_bids_probe.js` — Maine DOT awarded bids; uses table-first + innerText fallback with date/status anchors
- `illinois_bidbuy_probe.js` — Illinois BidBuy (JSF/PrimeFaces); uses paginator-marker deduplication
