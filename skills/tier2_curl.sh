#!/usr/bin/env bash
# Tier 2: cURL with Chrome-like browser headers
# Bypasses basic bot detection that blocks plain curl/wget requests.
#
# Usage:
#   ./tier2_curl.sh "https://target-site.com"
#   ./tier2_curl.sh "https://target-site.com" | python3 -c "import sys; from bs4 import BeautifulSoup; print(BeautifulSoup(sys.stdin.read(), 'html.parser').get_text())"

URL="${1:?Usage: $0 <url>}"

curl -L \
  -A "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" \
  -H "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8" \
  -H "Accept-Language: en-US,en;q=0.9" \
  -H "Accept-Encoding: gzip, deflate, br" \
  -H "DNT: 1" \
  -H "Connection: keep-alive" \
  -H "Upgrade-Insecure-Requests: 1" \
  -H "Sec-Fetch-Dest: document" \
  -H "Sec-Fetch-Mode: navigate" \
  -H "Sec-Fetch-Site: none" \
  -H "Sec-Fetch-User: ?1" \
  -H "Cache-Control: max-age=0" \
  --compressed \
  "$URL"

# Key flags:
#   -L              Follow redirects
#   -A              Set User-Agent string (mimics Chrome on macOS)
#   --compressed    Handle gzip/brotli response encoding automatically
