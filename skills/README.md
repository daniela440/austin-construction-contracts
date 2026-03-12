# Web Scraping Skills — Four-Tier System

Reference: https://danielmiessler.com/blog/progressive-web-scraping-four-tier-system

Start with Tier 1 and escalate only when a lower tier fails.

| Tier | Method | When to Use |
|------|--------|-------------|
| 1 | WebFetch (built-in) | Simple public pages, no JS required |
| 2 | cURL with browser headers | Bot-detection blocks plain requests |
| 3 | Playwright (headless browser) | JS-rendered content, SPAs, dynamic pages |
| 4 | Bright Data MCP | Protected sites, CAPTCHAs, heavy anti-bot |

- [tier1_webfetch.md](tier1_webfetch.md)
- [tier2_curl.sh](tier2_curl.sh)
- [tier3_playwright.js](tier3_playwright.js)
- [tier4_brightdata.md](tier4_brightdata.md)

## Supplemental Skills

Techniques used after you've retrieved the raw content:

- [tier_json_api.md](tier_json_api.md) — Direct JSON/REST API scraping (GET + POST, Socrata, ArcGIS, pagination)
- [tier_pdf_extraction.md](tier_pdf_extraction.md) — PDF text extraction (PyMuPDF) + OCR fallback (macOS Vision) for Type3 fonts
- [tier_network_intercept.md](tier_network_intercept.md) — Playwright request/response interception for session-authenticated downloads

## Platform-Specific Skills

Patterns for specific procurement platforms used across multiple states:

- [tier_questcdn_datatables.md](tier_questcdn_datatables.md) — QuestCDN bid results portal (DataTables server-side API, session seeding, detail page award extraction)
- [tier_peopleSoft_erp.md](tier_peopleSoft_erp.md) — Oracle PeopleSoft supplier portals (TargetContent iframe, select-based pagination, innerText grid parsing)
- [tier_proactis_webprocure.md](tier_proactis_webprocure.md) — Proactis WebProcure API (config bootstrap endpoint, contract search + detail APIs, UNSPSC→NAICS mapping)
- [tier_dynamic_form_wait.md](tier_dynamic_form_wait.md) — JS form result polling, table-first/innerText fallback extraction, paginator deduplication
