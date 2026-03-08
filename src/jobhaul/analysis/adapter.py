"""Abstract LLM adapter interface."""

from __future__ import annotations

from abc import ABC, abstractmethod


class LLMAdapter(ABC):
    @abstractmethod
    async def analyze(self, prompt: str) -> str: ...
