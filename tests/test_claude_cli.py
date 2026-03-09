"""Tests for Claude CLI adapter timeout and configuration."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from jobhaul.analysis.claude_cli import (
    DEFAULT_TIMEOUT,
    ClaudeCliAdapter,
    LLMTimeoutError,
)


class TestClaudeCliTimeout:
    def test_default_timeout(self):
        """Default timeout is 90 seconds."""
        with patch("jobhaul.analysis.claude_cli._refresh_claude_token"):
            adapter = ClaudeCliAdapter()
        assert adapter.timeout == 90

    def test_timeout_is_configurable(self):
        """Timeout can be set via constructor."""
        with patch("jobhaul.analysis.claude_cli._refresh_claude_token"):
            adapter = ClaudeCliAdapter(timeout=30)
        assert adapter.timeout == 30

    @pytest.mark.asyncio
    async def test_timeout_fires_on_hanging_subprocess(self):
        """A hanging subprocess should raise LLMTimeoutError."""
        with patch("jobhaul.analysis.claude_cli._refresh_claude_token"):
            adapter = ClaudeCliAdapter(timeout=1)

        mock_proc = AsyncMock()
        mock_proc.kill = AsyncMock()

        async def hang_forever(*args, **kwargs):
            await asyncio.sleep(999)

        mock_proc.communicate = hang_forever

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            with pytest.raises(LLMTimeoutError, match="timed out"):
                await adapter.analyze("test prompt")

    @pytest.mark.asyncio
    async def test_timeout_error_is_runtime_error_subclass(self):
        """LLMTimeoutError should be a subclass of RuntimeError."""
        assert issubclass(LLMTimeoutError, RuntimeError)

    def test_default_timeout_constant(self):
        """DEFAULT_TIMEOUT constant is 90."""
        assert DEFAULT_TIMEOUT == 90
