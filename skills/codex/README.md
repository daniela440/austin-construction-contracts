# Codex CLI Integration

Lets Claude have multi-turn conversations with OpenAI Codex CLI for plan review, code audits, brainstorming, and architecture decisions.

## Installation

1. Download this zip file
2. In Claude Code, run:
   ```
   /install-skill codex-v1.0.0.zip
   ```
3. Follow the prompts to install the Codex CLI system dependency

## System Requirements

### Codex CLI (required)

This skill wraps the OpenAI Codex CLI. You must install and authenticate it:

```bash
# Install
npm i -g @openai/codex

# Authenticate with your OpenAI account
codex login
```

Verify installation:
```bash
codex --version
```

**Note:** An active OpenAI account is required. Codex CLI manages its own authentication — no API key configuration in Claude is needed.

## Usage

Once installed, trigger the skill naturally:

```
"Talk to Codex about this plan"
"Ask Codex to review this code"
"Have Codex review my approach"
"Follow up with Codex about the edge cases"
"Continue with Codex — what about error handling?"
```

See `WORKFLOW.md` for detailed examples and tips.

## How It Works

The skill runs `codex exec` with your prompt and returns the response. Sessions are saved to `~/.codex-claude-session.json` so conversations can continue across turns.

- Model: gpt-5.4 (always)
- Reasoning: high (first message) or medium/high based on context
- Session timeout: 10 minutes per request

---
Packaged with Claude Code /export-skill
Commands provided by Authority Hacker's AI Accelerator — learn more: https://www.authorityhacker.com/ai-accelerator/. Enjoy!
