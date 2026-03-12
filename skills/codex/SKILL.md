---
name: codex
description: Talk to OpenAI Codex CLI for plan refinement, code review, research, and brainstorming. Use when user says "talk to codex", "ask codex", "have codex review", "continue with codex", or "follow up with codex". Uses gpt-5.4 with medium or high reasoning effort. Maintains conversation sessions for multi-turn discussions.
version: 1.0.0
requires_system:
  - name: codex
    description: OpenAI Codex CLI for agentic coding conversations
    check: codex --version
    install: npm i -g @openai/codex
    notes: Requires an OpenAI account. Codex CLI handles its own authentication after installation.
---

# Codex CLI Integration

This skill enables multi-turn conversations with OpenAI Codex CLI for:
- Plan refinement and review
- Code review and debugging
- Research and brainstorming
- Getting a second opinion on approaches

## How to Use

### Starting a Conversation

Run the script with a JSON prompt via stdin:

```bash
echo '{"prompt": "Review this implementation plan and suggest improvements..."}' | python3 scripts/codex_chat.py
```

### Continuing a Conversation

The script stores session IDs automatically. To continue the last conversation:

```bash
echo '{"prompt": "What about edge cases?", "continue_session": true}' | python3 scripts/codex_chat.py
```

### Reasoning Selection

Always uses **gpt-5.4**. Reasoning effort strategy:
- **First message** of a conversation: always use **high**
- **Follow-up messages**: choose based on conversation complexity so far
  - **high** — Complex threads, deep analysis, architecture decisions
  - **medium** — Simple follow-ups, clarifications, quick questions

```bash
echo '{"prompt": "...", "reasoning": "medium"}' | python3 scripts/codex_chat.py
```

## Background Execution

For long-running Codex tasks, use the Task tool with background mode:

```
When Codex might take a while:
1. Use Task tool with run_in_background: true
2. Pass the Bash command that runs the script
3. Continue other work
4. Check TaskOutput when ready
```

## Input JSON Schema

```json
{
  "prompt": "Required - the prompt to send to Codex",
  "reasoning": "Optional - 'medium' or 'high' (default: high)",
  "continue_session": "Optional - true to continue last conversation",
  "working_directory": "Optional - project directory for context"
}
```

## Output JSON Schema

```json
{
  "response": "Codex's response text",
  "session_id": "ID for continuing this conversation",
  "model": "Model that was used",
  "reasoning": "Reasoning level that was used",
  "success": true
}
```

## Session Management

Sessions are stored in `~/.codex-claude-session.json`. Each new conversation creates a new session unless `continue_session: true` is passed.

**Important:** Session continuation requires being in a git repository. New sessions can start anywhere, but to continue a session you need to be in a git repo (use `working_directory` to specify a project path).

## Exit Codes

- 0: Success
- 1: Input/configuration error
- 2: Codex CLI error
- 3: Codex not installed

## Examples

### Basic plan review
```bash
echo '{"prompt": "Review this plan for implementing user authentication. Look for security issues and suggest improvements."}' | python3 scripts/codex_chat.py
```

### Quick question with medium reasoning
```bash
echo '{"prompt": "What are the pros and cons of this approach?", "reasoning": "medium"}' | python3 scripts/codex_chat.py
```

### Continue a conversation
```bash
echo '{"prompt": "What about the error handling I mentioned earlier?", "continue_session": true}' | python3 scripts/codex_chat.py
```
