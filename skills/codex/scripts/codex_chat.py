#!/usr/bin/env python3
"""
Codex CLI Wrapper for Claude Code

Wraps the OpenAI Codex CLI to enable Claude to have conversations with Codex.
Supports session management, model selection, and reasoning effort configuration.

Usage:
    echo '{"prompt": "Review this plan"}' | python3 codex_chat.py
    echo '{"prompt": "Follow up", "continue_session": true}' | python3 codex_chat.py
    python3 codex_chat.py --help

Input JSON:
    {
        "prompt": "Your prompt here",           # Required
        "model": "gpt-5.4",                     # Optional, default: gpt-5.4
        "reasoning": "high",                    # Optional, default: high
        "continue_session": false,              # Optional, resume last session
        "working_directory": "/path/to/project" # Optional, defaults to cwd
    }

Exit codes:
    0 - Success
    1 - Configuration/input error
    2 - Codex CLI error
    3 - Dependency error (codex not installed)
"""

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Defaults
DEFAULT_MODEL = "gpt-5.4"
DEFAULT_REASONING = "high"
SESSION_FILE = Path.home() / ".codex-claude-session.json"


def check_codex_installed() -> bool:
    """Check if codex CLI is installed and accessible."""
    try:
        result = subprocess.run(
            ["codex", "--version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def load_session() -> dict | None:
    """Load the last session from disk."""
    if SESSION_FILE.exists():
        try:
            with open(SESSION_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    return None


def save_session(session_id: str, prompt: str, model: str) -> None:
    """Save session info to disk for continuation."""
    session_data = {
        "session_id": session_id,
        "created_at": datetime.now().isoformat(),
        "last_prompt": prompt[:200],  # Truncate for readability
        "model": model
    }
    try:
        with open(SESSION_FILE, "w") as f:
            json.dump(session_data, f, indent=2)
    except IOError as e:
        print(f"Warning: Could not save session: {e}", file=sys.stderr)


def extract_session_id(output: str) -> str | None:
    """Extract session ID from codex JSON output."""
    # Codex outputs JSONL, look for session/conversation ID
    for line in output.strip().split("\n"):
        try:
            event = json.loads(line)
            # Look for thread_id in thread.started event
            if event.get("type") == "thread.started":
                return event.get("thread_id")
            # Fallback to other session ID fields
            if "session_id" in event:
                return event["session_id"]
            if "conversation_id" in event:
                return event["conversation_id"]
            if "id" in event and event.get("type") == "session":
                return event["id"]
        except json.JSONDecodeError:
            continue
    return None


def extract_response(output: str) -> str:
    """Extract the assistant's response from codex JSON output."""
    messages = []

    for line in output.strip().split("\n"):
        try:
            event = json.loads(line)
            # Look for item.completed with agent_message
            if event.get("type") == "item.completed":
                item = event.get("item", {})
                if item.get("type") == "agent_message":
                    text = item.get("text", "")
                    if text:
                        messages.append(text)
            # Look for assistant messages (older format)
            elif event.get("type") == "message" and event.get("role") == "assistant":
                content = event.get("content", "")
                if content:
                    messages.append(content)
            elif event.get("type") == "text" or event.get("type") == "content":
                text = event.get("text", "") or event.get("content", "")
                if text:
                    messages.append(text)
        except json.JSONDecodeError:
            continue

    if messages:
        return "\n".join(messages)

    # Fallback: return the raw output if no structured messages found
    return output


def run_codex(
    prompt: str,
    model: str = DEFAULT_MODEL,
    reasoning: str = DEFAULT_REASONING,
    continue_session: bool = False,
    working_directory: str | None = None
) -> tuple[str, str | None, int]:
    """
    Run codex exec with the given parameters.

    Returns:
        tuple: (response_text, session_id, exit_code)
    """
    # Build command
    cmd = ["codex", "exec"]

    # Handle session continuation
    session_id = None
    is_resuming = False
    if continue_session:
        session = load_session()
        if session and session.get("session_id"):
            session_id = session["session_id"]
            cmd.append("resume")
            cmd.append(session_id)
            is_resuming = True

    # Add prompt
    cmd.append(prompt)

    # For resume, use -c for model override; for new sessions use --model
    if is_resuming:
        cmd.extend(["-c", f'model="{model}"'])
    else:
        cmd.extend(["--model", model])

    # Add reasoning effort
    cmd.extend(["-c", f"model_reasoning_effort={reasoning}"])

    # These flags only work for new sessions (not resume)
    if not is_resuming:
        # Add JSON output
        cmd.append("--json")
        # Allow running outside git repos
        cmd.append("--skip-git-repo-check")

    # Set working directory
    cwd = working_directory if working_directory else os.getcwd()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=cwd,
            timeout=600  # 10 minute timeout
        )

        output = result.stdout
        stderr = result.stderr

        if result.returncode != 0:
            error_msg = stderr or output or "Unknown error"
            return f"Codex error: {error_msg}", None, 2

        # Extract session ID for future continuation
        new_session_id = extract_session_id(output) or session_id

        # Extract the response
        response = extract_response(output)

        return response, new_session_id, 0

    except subprocess.TimeoutExpired:
        return "Codex timed out after 10 minutes", None, 2
    except Exception as e:
        return f"Error running codex: {str(e)}", None, 2


def main():
    # Check codex is installed
    if not check_codex_installed():
        print("Error: codex CLI is not installed or not in PATH", file=sys.stderr)
        print("Install with: npm i -g @openai/codex", file=sys.stderr)
        sys.exit(3)

    # Read input from stdin
    try:
        input_data = sys.stdin.read()
        if not input_data.strip():
            print("Error: No input provided. Expected JSON via stdin.", file=sys.stderr)
            sys.exit(1)

        params = json.loads(input_data)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON input: {e}", file=sys.stderr)
        sys.exit(1)

    # Extract parameters
    prompt = params.get("prompt")
    if not prompt:
        print("Error: 'prompt' is required", file=sys.stderr)
        sys.exit(1)

    model = DEFAULT_MODEL  # Always gpt-5.4
    reasoning = params.get("reasoning", DEFAULT_REASONING)
    continue_session = params.get("continue_session", False)
    working_directory = params.get("working_directory")

    # Validate reasoning
    valid_reasoning = ["medium", "high"]
    if reasoning not in valid_reasoning:
        print(f"Error: reasoning must be one of {valid_reasoning}", file=sys.stderr)
        sys.exit(1)

    # Run codex
    response, session_id, exit_code = run_codex(
        prompt=prompt,
        model=model,
        reasoning=reasoning,
        continue_session=continue_session,
        working_directory=working_directory
    )

    # Save session for future continuation
    if session_id and exit_code == 0:
        save_session(session_id, prompt, model)

    # Output result
    result = {
        "response": response,
        "session_id": session_id,
        "model": model,
        "reasoning": reasoning,
        "success": exit_code == 0
    }

    print(json.dumps(result, indent=2))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
