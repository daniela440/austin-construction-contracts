# JSON/REST API Scraping

Use when a government portal exposes a direct JSON API — no HTML parsing or browser needed.
This is the most reliable and fastest scraping approach when available.

## How to identify a JSON API
- Open DevTools → Network tab → filter by `Fetch/XHR`
- Reload the page or trigger a search
- Look for requests returning `Content-Type: application/json`
- Copy the request URL and payload — this is your API

Common patterns in government portals:
- Socrata Open Data: `resource/<id>.json?$where=...&$limit=...`
- ArcGIS FeatureServer: `/query?where=...&outFields=*&f=json`
- Custom REST APIs: `POST /api/contracts/search` with JSON body

---

## Pattern A: Simple GET with query parameters

```python
import json
import ssl
import urllib.request
import urllib.parse

# SSL setup (macOS Python often lacks default certs)
try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE

def api_get(url, params=None):
    """GET JSON from a URL with optional query parameters."""
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; Scraper/1.0)",
    })
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

# Example: Socrata API
results = api_get(
    "https://data.sfgov.org/resource/theoreticalid.json",
    params={"$where": "award_amount > 1000000", "$limit": 5000, "$offset": 0}
)
```

## Pattern B: POST with JSON body

```python
def api_post(url, payload):
    """POST JSON payload and return parsed response."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; Scraper/1.0)",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

# Example: DC OCP search
data = api_post("https://contracts.ocp.dc.gov/api/contracts/search", {
    "FilterBy": [
        {"id": 1, "name": "DatesOption", "value": 2},
        {"id": 2, "name": "Amount.From", "value": 250000},
    ],
    "OrderBy": [],
})
results = data.get("results", [])
```

---

## Pagination patterns

### Socrata (`$limit` / `$offset`)
```python
PAGE_SIZE = 1000
offset = 0
all_results = []
while True:
    page = api_get(base_url, params={"$limit": PAGE_SIZE, "$offset": offset, ...})
    if not page:
        break
    all_results.extend(page)
    if len(page) < PAGE_SIZE:
        break
    offset += PAGE_SIZE
```

### ArcGIS FeatureServer (`resultOffset` / `resultRecordCount`)
```python
PAGE_SIZE = 1000
offset = 0
all_features = []
while True:
    data = api_get(query_url, params={
        "where": "1=1", "outFields": "*", "f": "json",
        "resultOffset": offset, "resultRecordCount": PAGE_SIZE
    })
    features = data.get("features", [])
    all_features.extend(features)
    if not data.get("exceededTransferLimit"):
        break
    offset += PAGE_SIZE
```

### No pagination (all results in one call)
Some APIs return everything at once (e.g., DC OCP). Just call once and filter client-side.

---

## Rate limiting
Add a delay between detail-fetch calls when hitting per-record endpoints:

```python
import time
for record in records:
    detail = api_get(f"{DETAIL_BASE}?id={record['id']}")
    time.sleep(1.0)  # 1 second between calls
```

---

## Real examples in this project
- `sf_contracts.py` / `txdot_contracts.py` — Socrata GET with `$where` filters + `$limit`/`$offset`
- `fl_fdot_contracts.py` — ArcGIS FeatureServer with `resultOffset` pagination
- `alaska_dot_contracts.py` — Simple JSON GET, no pagination
- `dc_ocp_contracts.py` — POST search API + GET detail API per record
