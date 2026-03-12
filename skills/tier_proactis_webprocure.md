# Proactis WebProcure API Scraping

Use when a government procurement portal is powered by Proactis WebProcure
(common in Connecticut, and other states/municipalities that use Proactis procurement software).

## How to identify a Proactis portal
- The portal page embeds or redirects to `webprocure.proactiscloud.com`
- DevTools shows requests to `/wp-full-text-search/` or `/wp-contract/`
- The portal URL often goes through a state/agency site (e.g. `portal.ct.gov/das/ctsource/contractboard`)

---

## Key discovery: the config bootstrap endpoint

Every Proactis portal loads its API base URLs from a public config endpoint:

```
GET https://webprocure.proactiscloud.com/wp-web-public/en/resource?eboId=undefined
```

Response:
```json
{
  "apiBaseURL":     "https://webprocure.proactiscloud.com/wp-full-text-search",
  "loginURL":       "https://webprocure.proactiscloud.com/login.do",
  "wpBaseURL":      "https://webprocure.proactiscloud.com/",
  "contractApiUrl": "https://webprocure.proactiscloud.com/wp-contract"
}
```

Use these base URLs for all subsequent API calls.

---

## Pattern A: Search contracts (GET)

```python
import json, ssl, urllib.request, urllib.parse

try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode   = ssl.CERT_NONE

SEARCH_BASE = "https://webprocure.proactiscloud.com/wp-full-text-search"
CONTRACT_BASE = "https://webprocure.proactiscloud.com/wp-contract"

def proactis_search(query: str = "", page: int = 0, page_size: int = 20,
                    effective_from: str = "2026-01-01") -> dict:
    """
    Search Proactis contracts. Returns the raw JSON response.
    Key response fields: totalResults, contracts[].id, .title, .maxValue, .effectiveDate, .vendorName
    """
    params = urllib.parse.urlencode({
        "query":         query,
        "page":          page,
        "pageSize":      page_size,
        "effectiveFrom": effective_from,
        "status":        "ACTIVE",
    })
    url = f"{SEARCH_BASE}/search/contracts?{params}"
    req = urllib.request.Request(url, headers={
        "Accept":     "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; Scraper/1.0)",
        "Referer":    "https://webprocure.proactiscloud.com/wp-web-public/en/",
    })
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))
```

## Pattern B: Get contract detail by ID

```python
def proactis_detail(contract_id: str) -> dict:
    """
    Fetch full contract detail including vendor, amounts, contact, commodity codes.
    """
    url = f"{CONTRACT_BASE}/public/contract/{contract_id}"
    req = urllib.request.Request(url, headers={
        "Accept":     "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; Scraper/1.0)",
        "Referer":    "https://webprocure.proactiscloud.com/wp-web-public/en/",
    })
    with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))
```

---

## Full pagination loop

```python
import time

PAGE_SIZE    = 20
MIN_VALUE    = 250_000
SINCE_DATE   = "2026-01-01"

all_contracts = []
page = 0

while True:
    data = proactis_search(page=page, page_size=PAGE_SIZE, effective_from=SINCE_DATE)
    contracts = data.get("contracts") or []
    all_contracts.extend(contracts)

    total = data.get("totalResults", 0)
    fetched = (page + 1) * PAGE_SIZE
    if fetched >= total or not contracts:
        break

    page += 1
    time.sleep(0.5)

# Filter client-side
filtered = [c for c in all_contracts if (c.get("maxValue") or 0) >= MIN_VALUE]
```

---

## Response field reference

### Search result item (`contracts[]`)
| Field | Description |
|-------|-------------|
| `id` | Contract ID — use for detail lookup |
| `title` | Contract title / description |
| `maxValue` | Maximum contract value |
| `effectiveDate` | Contract start date |
| `expiryDate` | Contract end date |
| `vendorName` | Awarded vendor name |
| `commodityCode` | UNSPSC code (use for NAICS mapping) |
| `status` | `"ACTIVE"`, `"EXPIRED"`, etc. |

### Detail record (`/public/contract/{id}`)
| Field | Description |
|-------|-------------|
| `vendor.name` | Full vendor name |
| `vendor.address` | Vendor address object |
| `vendor.contactName` | Primary contact |
| `vendor.contactEmail` | Contact email |
| `vendor.contactPhone` | Contact phone |
| `contractingOfficer.name` | CO name |
| `contractingOfficer.email` | CO email |

---

## UNSPSC → NAICS mapping (construction-related)

Proactis uses UNSPSC commodity codes, not NAICS. Map by keyword:

```python
UNSPSC_NAICS_MAP = {
    "238210": ["electrical", "wiring", "lighting", "signal"],
    "236220": ["construction", "renovation", "building", "facility"],
    "237310": ["road", "bridge", "paving", "asphalt", "highway"],
    "238220": ["hvac", "plumbing", "heating", "mechanical"],
    "238120": ["steel", "structural", "metal", "iron"],
}

def map_naics(title: str, commodity_code: str = "") -> str | None:
    text = f"{title} {commodity_code}".lower()
    for naics, keywords in UNSPSC_NAICS_MAP.items():
        if any(kw in text for kw in keywords):
            return naics
    return None
```

---

## Tips
- The search API is fully public — no login or session cookie required
- `effectiveFrom` filters by contract start date; use `expiryAfter` if you want contracts still active
- Rate limit: add `time.sleep(0.5)` between pages to be polite
- The portal URL (e.g. `portal.ct.gov/...`) is just a thin wrapper — all data comes from `webprocure.proactiscloud.com`
- Different agency/state portals may configure different `eboId` values — always fetch the config bootstrap first if unsure which API base to use

---

## Real examples in this project
- `ct_ctsource_contracts.py` — CT DAS CTSource Contract Board, uses `wp-full-text-search` + `wp-contract` APIs
- `ct_recon_api.json` — captured API config bootstrap response showing all endpoint URLs
