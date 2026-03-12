// Tier 3: Playwright Headless Browser
// Use when the page requires JavaScript to render content (SPAs, React/Vue sites,
// lazy-loaded tables, etc.) and Tiers 1–2 return incomplete data.
//
// Install: npm install playwright
// Then:    npx playwright install chromium
//
// Usage:   node tier3_playwright.js "https://dynamic-site.com"

import { chromium } from 'playwright';

const url = process.argv[2];
if (!url) {
  console.error('Usage: node tier3_playwright.js <url>');
  process.exit(1);
}

const browser = await chromium.launch();
const page = await browser.newPage();

// Mimic a real browser to avoid basic bot detection
await page.setExtraHTTPHeaders({
  'Accept-Language': 'en-US,en;q=0.9',
});
await page.setViewportSize({ width: 1280, height: 800 });

await page.goto(url, { waitUntil: 'networkidle' });

// Wait for dynamic content to settle
await page.waitForLoadState('networkidle');

const content = await page.content();
console.log(content);

await browser.close();

// Tips:
//   - Replace 'networkidle' with 'domcontentloaded' for faster loads if JS isn't needed
//   - Use page.waitForSelector('css-selector') to wait for a specific element
//   - Use page.evaluate(() => document.body.innerText) for plain text only
//   - Add page.screenshot({ path: 'debug.png' }) to debug what the browser sees
