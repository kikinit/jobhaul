"""Claude CLI subprocess adapter."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from jobhaul.analysis.adapter import LLMAdapter
from jobhaul.log import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT = 90


class LLMTimeoutError(RuntimeError):
    """Raised when the LLM subprocess exceeds its timeout."""


def _refresh_claude_token() -> None:
    """Inject OpenClaw's live OAuth token into claude CLI credentials."""
    auth_path = Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth.json"
    creds_path = Path.home() / ".claude" / ".credentials.json"

    if not auth_path.exists() or not creds_path.exists():
        return

    try:
        token = json.loads(auth_path.read_text())["anthropic"]["key"]
        creds = json.loads(creds_path.read_text())
        creds.setdefault("claudeAiOauth", {})["accessToken"] = token
        creds["claudeAiOauth"]["expiresAt"] = int((time.time() + 86400 * 30) * 1000)
        creds_path.write_text(json.dumps(creds))
        logger.debug("Refreshed claude CLI token from OpenClaw")
    except Exception as e:
        logger.warning("Failed to refresh claude token: %s", e)


class ClaudeCliAdapter(LLMAdapter):
    def __init__(self, model: str = "claude-sonnet-4-20250514", timeout: int = DEFAULT_TIMEOUT):
        self.model = model
        self.timeout = timeout
        _refresh_claude_token()

    async def analyze(self, prompt: str) -> str:
        try:
            proc = await asyncio.create_subprocess_exec(
                "claude",
                "-p",
                "--model",
                self.model,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(input=prompt.encode()), timeout=self.timeout
            )

            if proc.returncode != 0:
                err_msg = stderr.decode().strip()
                raise RuntimeError(f"claude CLI failed (exit {proc.returncode}): {err_msg}")

            return stdout.decode().strip()

        except asyncio.TimeoutError:
            proc.kill()
            raise LLMTimeoutError(f"claude CLI timed out after {self.timeout}s")
        except FileNotFoundError:
            raise RuntimeError(
                "claude CLI not found. Install it or configure a different LLM adapter."
            )
