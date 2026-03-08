"""Claude CLI subprocess adapter."""

from __future__ import annotations

import asyncio

from jobhaul.analysis.adapter import LLMAdapter
from jobhaul.log import get_logger

logger = get_logger(__name__)

TIMEOUT = 60


class ClaudeCliAdapter(LLMAdapter):
    def __init__(self, model: str = "claude-sonnet-4-20250514"):
        self.model = model

    async def analyze(self, prompt: str) -> str:
        try:
            proc = await asyncio.create_subprocess_exec(
                "claude",
                "-p",
                prompt,
                "--model",
                self.model,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=TIMEOUT
            )

            if proc.returncode != 0:
                err_msg = stderr.decode().strip()
                raise RuntimeError(f"claude CLI failed (exit {proc.returncode}): {err_msg}")

            return stdout.decode().strip()

        except asyncio.TimeoutError:
            proc.kill()
            raise RuntimeError(f"claude CLI timed out after {TIMEOUT}s")
        except FileNotFoundError:
            raise RuntimeError(
                "claude CLI not found. Install it or configure a different LLM adapter."
            )
