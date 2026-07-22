"""Typed, versioned PA-authored prompt registry."""

from pa.prompts.catalog import PROMPTS
from pa.prompts.registry import (
    PromptDefinition,
    PromptRegistry,
    PromptRenderError,
    PromptVariable,
    RenderedPrompt,
)

__all__ = [
    "PROMPTS",
    "PromptDefinition",
    "PromptRegistry",
    "PromptRenderError",
    "PromptVariable",
    "RenderedPrompt",
]
