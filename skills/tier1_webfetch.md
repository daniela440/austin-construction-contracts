# Tier 1: WebFetch (Built-in Tool)

The simplest approach. Use this first for any public page that doesn't require JavaScript rendering.

## Usage

```
WebFetch({
  url: "https://example.com",
  prompt: "Extract all content from this page and convert to markdown"
})
```

## When to escalate to Tier 2
- Returns empty content
- Returns a bot-detection/CAPTCHA page
- Returns a login wall
- Content appears truncated or missing key data
