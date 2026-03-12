# Codex Models and Reasoning Reference

## Available Models

### gpt-5.4 (Default)
- **Best for:** Complex reasoning, plan review, architecture decisions
- **Speed:** Fast
- **Cost:** Moderate-High
- **Notes:** Latest and most capable GPT model. Default for high reasoning effort.

### gpt-5.2
- **Best for:** General-purpose tasks, brainstorming, routine reviews
- **Speed:** Fast
- **Cost:** Moderate
- **Notes:** Good balance of capability and speed

### gpt-5-codex
- **Best for:** Pure coding tasks, debugging, code generation
- **Speed:** Fast
- **Cost:** Moderate
- **Notes:** Purpose-built for Codex CLI, optimized for agentic coding

### o3
- **Best for:** Complex reasoning, architectural decisions, trade-off analysis
- **Speed:** Slower (extended thinking)
- **Cost:** Higher
- **Notes:** Use when deep analysis is needed

### o4-mini
- **Best for:** Quick questions, simple reviews
- **Speed:** Very fast
- **Cost:** Lower
- **Notes:** Good for iterative conversations where speed matters

## Reasoning Effort Levels

### minimal
- Fastest response time
- Light analysis
- Good for: Quick checks, simple questions

### low
- Fast with some reasoning
- Basic analysis
- Good for: Routine reviews

### medium
- Balanced speed and depth
- Moderate analysis
- Good for: Standard code review

### high (Default)
- Thorough reasoning
- Detailed analysis
- Good for: Plan review, architecture decisions

### xhigh
- Maximum reasoning effort
- Deep, comprehensive analysis
- Good for: Critical decisions, complex debugging, security review
- Note: Significantly slower

## Recommended Combinations

| Task | Model | Reasoning |
|------|-------|-----------|
| Quick question | gpt-5.2 | medium |
| Plan review | gpt-5.4 | high |
| Complex architecture | gpt-5.4 | high |
| Code debugging | gpt-5-codex | high |
| Security audit | gpt-5.4 | xhigh |
| Rapid iteration | o4-mini | medium |
| Deep analysis | gpt-5.4 | xhigh |

## Configuration

Reasoning effort can also be set globally in `~/.codex/config.toml`:

```toml
model_reasoning_effort = "high"
```

Per-request overrides via the `-c` flag always take precedence.

## Sources

- [Codex Models](https://developers.openai.com/codex/models/)
- [Codex Configuration Reference](https://developers.openai.com/codex/config-reference/)
