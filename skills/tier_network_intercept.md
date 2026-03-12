# Playwright Network Interception

Use when a page makes authenticated background requests (e.g., to fetch a PDF, API data,
or session-protected resource) that you need to capture or replay using the browser's session.

## When to use
- The URL you need requires session cookies or tokens that are only set after page interaction
- A PDF or file is loaded inside an iframe/viewer (Hyland Cloud, OnBase, DocuWare, etc.)
- The actual data URL is dynamically generated and not visible in the page HTML
- Standard `fetch()` or `urllib` calls fail with 401/403 even with copied headers

---

## Pattern: Intercept a request URL, then download with the same session

```python
from playwright.sync_api import sync_playwright

def fetch_with_intercept(page_url, intercept_pattern):
    """
    Navigate to page_url, intercept the first request URL matching intercept_pattern,
    then download that URL using the same browser context (session cookies carried over).
    Returns raw bytes or None on failure.
    """
    intercepted_urls = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        # Register listener BEFORE navigating
        page.on("request", lambda req: (
            intercepted_urls.append(req.url)
            if intercept_pattern in req.url else None
        ))

        page.goto(page_url, timeout=30000)
        page.wait_for_timeout(10000)  # Wait for background requests to fire

        if not intercepted_urls:
            browser.close()
            return None

        # Download using the browser context — session cookies are automatically included
        resp = ctx.request.get(intercepted_urls[0], timeout=30000)
        data = resp.body()
        browser.close()
        return data
```

## Listening to responses instead of requests

Use `page.on("response", ...)` when you need the response body directly,
or when request URLs are generated dynamically after the initial navigation:

```python
intercepted = []

page.on("response", lambda resp: (
    intercepted.append(resp)
    if "PdfHandler" in resp.url else None
))

page.goto(viewer_url)
page.wait_for_timeout(8000)

if intercepted:
    body = intercepted[0].body()  # raw bytes
```

---

## Common intercept patterns

| Site type | Pattern to match |
|-----------|-----------------|
| Hyland Cloud / OnBase | `"PdfHandler"` or `"DocPop"` |
| ArcGIS map services | `"/MapServer/"` or `"/FeatureServer/"` |
| Government API behind login | `"/api/"` after a login flow |
| Dynamically signed S3 URLs | `"amazonaws.com"` |

---

## Tips
- Always register the listener **before** `page.goto()` — events fire during navigation
- Use `page.wait_for_timeout(ms)` to give background requests time to fire; tune as needed
- `ctx.request.get(url)` carries the browser context's cookies — this is the key advantage
- If the intercepted URL expires quickly, keep the browser open and act immediately
- Use `--headed` (set `headless=False`) to debug what the browser is actually loading

---

## Real example in this project
`co_vss_contracts.py` — intercepts `PdfHandler.ashx` requests from a Hyland Cloud OnBase
viewer to download session-authenticated PDF bid tabs, then extracts amounts via Vision OCR:

```python
pdf_handler_url = []
page.on("request", lambda req: (
    pdf_handler_url.append(req.url)
    if "PdfHandler" in req.url else None
))
page.goto(docpop_url, timeout=30000)
page.wait_for_timeout(10000)

resp = playwright_ctx.request.get(pdf_handler_url[0], timeout=30000)
pdf_bytes = resp.body()
```
