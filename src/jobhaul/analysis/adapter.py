"""Abstract LLM adapter interface.

Defines the base class that all LLM backends must implement. This allows the
rest of the analysis code to work with any language model without knowing the
details of how it is called (CLI subprocess, HTTP API, etc.).
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class LLMAdapter(ABC):
    """Base class for language model backends.

    Every LLM adapter must implement the ``complete`` method, which takes a
    plain-text prompt and returns the model's response as a string. The
    adapter is responsible for handling authentication, subprocess management,
    or HTTP calls internally.
    """

    @abstractmethod
    async def complete(self, prompt: str) -> str:
        """Send a prompt to the language model and return its text response.

        Args:
            prompt: The full prompt text to send to the model.

        Returns:
            The model's response as a plain string.

        Raises:
            RuntimeError: If the model call fails for any reason.
        """
        ...
