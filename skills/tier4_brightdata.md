# Tier 4: Bright Data MCP

Use as a last resort when Tiers 1–3 fail due to CAPTCHAs, heavy anti-bot systems,
or sites that require residential IP rotation.

## Setup

Add to `.claude/.mcp.json` (or `~/.claude/mcp.json`):

```json
{
  "mcpServers": {
    "brightdata": {
      "command": "bunx",
      "args": ["-y", "@brightdata/mcp"],
      "env": {
        "API_TOKEN": "your_bright_data_api_token_here"
      }
    }
  }
}
```

Get an API token at: https://brightdata.com

---

## Tool Reference

### Scrape a single URL
```
mcp__Brightdata__scrape_as_markdown({ url: "https://complex-site.com" })
```

### Scrape multiple URLs (batch, up to 10)
```
mcp__Brightdata__scrape_batch({
  urls: ["https://site1.com", "https://site2.com", "https://site3.com"]
})
```

### Search engine scraping
```
mcp__Brightdata__search_engine({ query: "target company name", engine: "google" })
```

### Batch search queries
```
mcp__Brightdata__search_engine_batch({
  queries: [
    { query: "query one", engine: "google" },
    { query: "query two", engine: "bing" }
  ]
})
```

---

## When to use Tier 4 vs DDG search
- Tier 4 for scraping specific known URLs that are bot-protected
- DDG (`duckduckgo_search` package) for discovery searches — but note DDG rate-limits
  aggressively after ~50 queries; Bright Data is more reliable for bulk search needs
