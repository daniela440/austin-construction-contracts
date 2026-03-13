#!/bin/bash
# run_scrapers.sh — Runs all procurement scrapers every Monday
# Logs to logs/scraper_YYYY-MM-DD.log

cd "$(dirname "$0")"

LOG="logs/scraper_$(date +%Y-%m-%d).log"
PYTHON=python3

echo "=====================================" | tee -a "$LOG"
echo "Scraper run: $(date)" | tee -a "$LOG"
echo "=====================================" | tee -a "$LOG"

run_scraper() {
    local name="$1"
    local script="$2"
    echo "" | tee -a "$LOG"
    echo "--- $name ---" | tee -a "$LOG"
    $PYTHON "$script" 2>&1 | tee -a "$LOG"
    local exit_code=${PIPESTATUS[0]}
    if [ $exit_code -ne 0 ]; then
        echo "  ⚠ $name failed (exit $exit_code)" | tee -a "$LOG"
    else
        echo "  ✓ $name done" | tee -a "$LOG"
    fi
}

# --- Run all scrapers ---
run_scraper "Austin"       austin_contracts.py
run_scraper "San Francisco" sf_contracts.py
run_scraper "TxDOT"        txdot_contracts.py
run_scraper "TxSmartBuy"   txsmartbuy_contracts.py
run_scraper "FL FDOT"      fl_fdot_contracts.py
run_scraper "Alaska DOT"   alaska_dot_contracts.py

# Retry Alaska once if it failed (common transient disconnect)
if grep -q "⚠ Alaska DOT failed" "$LOG"; then
    echo "" | tee -a "$LOG"
    echo "Retrying Alaska DOT..." | tee -a "$LOG"
    run_scraper "Alaska DOT (retry)" alaska_dot_contracts.py
fi

run_scraper "DC OCP"       dc_ocp_contracts.py
run_scraper "UIowa"        uiowa_buildui_contracts.py
run_scraper "NJ START"     nj_start_contracts.py
run_scraper "TN TDOT"      tn_tdot_contracts.py
run_scraper "Colorado CDOT" co_vss_contracts.py
run_scraper "CT CTSource"   ct_ctsource_contracts.py
run_scraper "Delaware MMP"  de_mmp_contracts.py
run_scraper "Idaho DPW"     idaho_dpw_contracts.py
run_scraper "Illinois BidBuy" il_bidbuy_contracts.py
run_scraper "Minnesota QuestCDN" mn_questcdn_contracts.py
run_scraper "Mississippi MDOT" ms_mdot_contracts.py
run_scraper "Montana MDT"     mt_mdt_contracts.py
run_scraper "USA Spending"     import_usaspending.py

# --- Enrichment + Scoring (depend on scraper data being present) ---
run_scraper "Apollo Enrichment"  enrich_company_info.py
run_scraper "HubSpot Check"      hubspot_check.py
run_scraper "Lead Scoring"       score_leads.py

echo "" | tee -a "$LOG"
echo "=====================================" | tee -a "$LOG"
echo "All scrapers complete: $(date)" | tee -a "$LOG"
echo "=====================================" | tee -a "$LOG"
