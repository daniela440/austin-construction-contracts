# Project Context: Construction Procurement Lead Generator

## What This Project Does

Scrapes government procurement portals across 11 sources, standardizes the data into a Google Sheet, enriches it with company/contact info via Apollo.io, checks it against HubSpot CRM, and displays everything in a live Netlify dashboard. The goal is a steady pipeline of construction contract leads with contact details ready for outreach.

---

## Architecture at a Glance

```
11 Scrapers → Sheet A (Google Sheets) → Enrichment → HubSpot Check → Netlify Dashboard
                                                                         ↑
                               OSHA Scraper → OSHA Sheet (same workbook) ┘
```

**Sheet A:** `1HQMnHzPrx0Qa4ijuaR0BcVptpiiVG7mE3Po17rvruKQ`
- Tab: "Webscraper Tool for Procurement Sites Two"
- 14-column standard format (see below)

**OSHA Sheet:** Same workbook, tab: "OSHA company audits"

**Sheet B (Enrichment support):** `1wn90bdenhTMhPgyxeJMQXMy6G5D_Ox3eJvh3aW74RaA`
- Tabs: Companies_Enrichment, Blacklist_Rules

**Dashboard:** Static HTML hosted on Netlify, fetches CSV live from Google Sheets on every page load

---

## Script Execution Order

### Automated Schedule

Scripts run on two automated schedules:

- **`run_scrapers.sh`** — runs all 11 scrapers sequentially every **Monday at 8 AM** (local machine cron). Logs to `logs/scraper_YYYY-MM-DD.log`. Alaska DOT is automatically retried once on failure.
  - Cron entry (see `crontab.txt`): `0 8 * * 1 bash /Users/daniela/scraper-project/run_scrapers.sh`
- **`enrich_company_info.py`** — runs daily at **6 AM ET** via GitHub Actions (`.github/workflows/enrich-companies.yml`). Can also be triggered manually from the GitHub UI.

### Execution Order (when running manually)

```
STEP 1 — Collect Data (run_scrapers.sh handles all of the below)
  ├── austin_contracts.py
  ├── sf_contracts.py
  ├── txdot_contracts.py
  ├── txsmartbuy_contracts.py
  ├── fl_fdot_contracts.py
  ├── alaska_dot_contracts.py         ← auto-retried once on failure
  ├── dc_ocp_contracts.py
  ├── uiowa_buildui_contracts.py
  ├── nj_start_contracts.py           ← Playwright (headless browser)
  ├── tn_tdot_contracts.py            ← Playwright (headless browser) + PDF parsing
  └── co_vss_contracts.py             ← Playwright + PyMuPDF + macOS Vision OCR

STEP 2 — OSHA Data (independent pipeline, run separately/manually)
  └── osha_inspection_scraper.py      ← scrapes OSHA.gov violations

STEP 3 — Enrich (GitHub Actions daily, or run manually AFTER scrapers)
  ├── enrich_company_info.py          ← enriches Sheet A companies via Apollo
  └── osha_enrich.py                  ← enriches OSHA sheet companies via Apollo

STEP 4 — HubSpot Check (run LAST — needs enriched data)
  └── hubspot_check.py                ← marks "In HubSpot?" on both sheets

STEP 5 — Dashboard (automatic, no action needed)
  └── index.html on Netlify fetches live CSV from Google Sheets on every page load
```

**Note:** `import_usaspending.py` is a one-off federal contracts importer; run as needed.

---

## The 11 Scrapers

| Script | Source | Tech | Min Amount | Contact Info? | Sheet Tab |
|--------|--------|------|-----------|--------------|-----------|
| `austin_contracts.py` | Austin Finance Online | HTML parsing | $50K | Yes (vendor profiles) | Main |
| `sf_contracts.py` | SF Open Data (Socrata) | JSON API | $1M | No | Main |
| `txdot_contracts.py` | Texas Open Data (Socrata) | JSON API | $1M | No | Main |
| `txsmartbuy_contracts.py` | TxSmartBuy portal | HTML parsing | — | Partial | TxSmartBuy tab |
| `fl_fdot_contracts.py` | FDOT ArcGIS FeatureServer | REST API | $250K | No | Main |
| `alaska_dot_contracts.py` | Alaska DOT JSON API | JSON API | $250K | No | Main |
| `dc_ocp_contracts.py` | DC OCP portal | JSON API | $250K | CO email only | Main |
| `uiowa_buildui_contracts.py` | U of Iowa BuildUI | Playwright | $250K | PM email (staff) | Main |
| `nj_start_contracts.py` | NJ START portal | Playwright | $250K | Yes (vendor profiles) | Main |
| `tn_tdot_contracts.py` | TN TDOT PDFs | Playwright + PyMuPDF | — | No | Main |
| `co_vss_contracts.py` | Colorado CDOT Bid Tabs | Playwright + PyMuPDF + macOS Vision OCR | varies | No | Main |

All scrapers are **append-only** — they never clear the sheet. They deduplicate by (Company Name + Contract Name) before writing to avoid double-entries.

---

## NAICS Codes (Applied Across All Scrapers)

| Code | Category |
|------|----------|
| 238210 | Electrical Contractors |
| 236220 | Commercial & Institutional Building Construction |
| 237310 | Highway, Street & Bridge Construction |
| 238220 | Plumbing & HVAC |
| 238120 | Structural Steel & Precast Concrete |

Matching is keyword-based on contract descriptions. DC OCP additionally uses NIGP code prefixes (909, 910, 912, 913, 914).

---

## 14-Column Standard Format (Sheet A)

| Col | Field | Notes |
|-----|-------|-------|
| A | City | Source identifier (e.g., "Austin", "San Francisco") |
| B | Company Name | Vendor/contractor |
| C | Contact Name | From vendor profile or Apollo enrichment |
| D | Phone | From vendor profile or Apollo enrichment |
| E | Email | From vendor profile, CO (DC OCP), or Apollo |
| F | Address | From vendor profile (often empty) |
| G | Website | From vendor profile or Apollo enrichment |
| H | Contract Name | Contract ID / project number |
| I | Award Amount | Dollar amount |
| J | Amount Expended | Payments made (rare; Austin requires > $0) |
| K | Begin Date | YYYY-MM-DD |
| L | Award Link | URL back to source portal |
| M | Project Description | Combined title + description |
| N | Commodity Type | NAICS category label |

---

## Enrichment Scripts

### `enrich_company_info.py` — Sheet A enrichment
- Reads companies from Sheet A that have no website or company info
- Calls **Apollo.io** to find website, contact name, title, email, phone, company info
- Writes to columns C, D, E, G, O (contact fields + website + company info)
- **Limit:** Max 50 new companies per week (tracked in `apollo_enrichment_counter.json`)
- Preferred contact titles in priority order: Safety Manager → HSE Manager → Project Manager → CEO/Owner

### `osha_enrich.py` — OSHA sheet enrichment
- Same Apollo approach, targets OSHA sheet rows 5–658 (2026 batch)
- Writes to columns R–W: Website, Contact Name, Title, Email, Phone, Company Info
- Strips state license ID prefixes (e.g., "Wa317989845 - ") before searching
- Flush to sheet every 10 companies to preserve progress
- **Limit:** Set `MAX_NEW` in code (currently 100 per run)

---

## HubSpot Check (`hubspot_check.py`)
- Reads all company names from both Sheet A and OSHA sheet
- Searches HubSpot CRM API for each company
- Writes to Sheet A columns Q–U: In HubSpot?, Last Engaged, Site Visits, In Sequence?, Replies
- Writes to OSHA sheet columns X–AB: same fields
- Rerun-safe: skips rows that already have values
- Auth: `HUBSPOT_TOKEN` in `.env`

---

## OSHA Pipeline (`osha_inspection_scraper.py`)
- Scrapes OSHA IMIS enforcement database (osha.gov/ords/imis)
- Targets same 5 NAICS codes, private employers only
- Pulls: company name, city, state, inspection date, violation counts, inspection type
- Writes to "OSHA company audits" tab
- Rate-limited: 1.0s between pages, 0.5s between detail fetches

---

## Dashboard (`index.html` on Netlify)

**Two tabs:**

1. **Procurement Contracts** — filters: State/City, Company Name, Date From, Min/Max Amount, Commodity Type, HubSpot Status
2. **OSHA Leads 2026** — filters: State, Company Name, Industry, Inspection Type, HubSpot Status

**Features:** Sortable columns, expandable cells, HubSpot badges (green/amber), filtered value sum, CSV export

**Data refresh:** Fetches live CSV from Google Sheets on every page load via `gviz/tq` endpoint. Google caches this for ~5–10 minutes — new scraper data won't appear instantly.

---

## Web Scraping Tier System (`skills/` folder)

| Tier | Tool | When to Use |
|------|------|-------------|
| 1 | WebFetch (built-in) | Simple public HTML pages |
| 2 | `skills/tier2_curl.sh` | Basic bot detection (needs browser headers) |
| 3 | `skills/tier3_playwright.js` | JS-rendered pages, SPAs, dynamic tables |
| 4 | `skills/tier4_brightdata.md` | CAPTCHAs, heavy anti-bot, IP rotation needed |

Always try lower tiers first and escalate only if blocked.

---

## Credentials & Config Files

| File | Purpose |
|------|---------|
| `.env` | API keys: `APOLLO_API_KEY`, `HUBSPOT_TOKEN`, `BRAVE_API_KEY` |
| `service-account-key.json` | Google Sheets API auth (service account) |
| `apollo_enrichment_counter.json` | Tracks weekly Apollo usage for Sheet A enrichment |
| `ddg_search_counter.json` | Tracks daily DuckDuckGo searches (limit: ~50/day) |
| `package.json` | npm: `playwright ^1.58.2` |

---

## Known Limitations & Notes

- **DC OCP** — 0 construction contracts early 2026; run weekly as the year progresses
- **Alaska DOT** — occasional `RemoteDisconnected` errors (transient server issue; just retry)
- **Apollo credits** — limited weekly quota; 0 results = likely exhausted or company too small
- **OSHA company names** — may have state license ID prefixes (`"Wa317989845 - CompanyName"`); `osha_enrich.py` strips these before Apollo search
- **TxSmartBuy** — writes to its own sheet tab, not the main "Webscraper Tool" tab
- **Tennessee TDOT** — letting dates are hardcoded in the script; update for new lettings
- **DDG search** — poor results for small construction companies; returns spam/foreign sites; `Blacklist_Rules` tab in Sheet B filters junk
- **Federal vs. local overlap** — USA Spending companies rarely overlap with state/local scraper companies
