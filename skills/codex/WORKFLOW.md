# How to Use: Codex CLI Integration

This skill lets Claude have multi-turn conversations with OpenAI Codex CLI. Think of it as getting a second AI opinion — useful for plan reviews, code audits, brainstorming, and architecture decisions.

## Prerequisites

Codex CLI must be installed and authenticated before using this skill:

```bash
npm i -g @openai/codex
codex login  # authenticate with your OpenAI account
```

## Quick Start

Just ask Claude naturally:

```
"Talk to Codex about this implementation plan"
"Ask Codex to review my approach"
"Have Codex review this code"
"Follow up with Codex about the edge cases"
```

## Example Workflows

### Plan Review
```
"Ask Codex to review this plan for implementing user authentication.
Look for security issues and suggest improvements."
```

### Code Review
```
"Have Codex review this Python function and suggest optimizations.
[paste code here]"
```

### Architecture Decision
```
"Talk to Codex: should we use a message queue or direct API calls
for this notification system? Give me the trade-offs."
```

### Multi-turn Discussion
```
Turn 1: "Ask Codex to review my caching strategy"
Turn 2: "Follow up with Codex — what about Redis vs Memcached?"
Turn 3: "Continue with Codex — how do we handle cache invalidation?"
```

## Reasoning Levels

The skill uses **gpt-5.4** by default. Claude picks the reasoning level automatically:

| Situation | Reasoning |
|-----------|-----------|
| First message in a conversation | high (always) |
| Complex follow-up, architecture decisions | high |
| Simple clarifications, quick questions | medium |

You can specify explicitly:
```
"Ask Codex with high reasoning to audit this security flow"
"Quick question for Codex with medium reasoning..."
```

## Session Continuity

Sessions are automatically saved to `~/.codex-claude-session.json`. Claude will continue the same Codex conversation thread when you say "follow up with Codex" or "continue with Codex."

Note: Session continuation requires being inside a git repository. Use `working_directory` to point to a project if needed.

## When to Use

- Second opinion on a design decision
- Code review before shipping
- Brainstorming alternative approaches
- Debugging a tricky problem
- Reviewing a spec or plan document
- Sanity-checking your reasoning

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "codex: command not found" | Run `npm i -g @openai/codex` |
| "Not authenticated" | Run `codex login` |
| Session not continuing | Make sure you're in a git repo |
| Timeout after 10 minutes | Break your prompt into smaller chunks |
| Exit code 2 | Check if Codex CLI is up to date: `npm update -g @openai/codex` |
