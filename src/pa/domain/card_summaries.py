"""Deterministic, side-effect-free card summary fallbacks."""

from __future__ import annotations

import re

DEFAULT_CARD_SUMMARY = "No details provided yet."
MAX_CARD_SUMMARY_LENGTH = 320
MAX_CARD_SUMMARY_SENTENCES = 3

_FENCE_RE = re.compile(r"```(?:[^\n]*)\n?(.*?)```", re.DOTALL)
_LINK_RE = re.compile(r"!?\[([^\]]+)\]\([^)]*\)")
_MARKUP_RE = re.compile(r"(^|\s)(?:#{1,6}|[-*+]>|\d+[.)])\s+|[*_~`]", re.MULTILINE)
_SPACE_RE = re.compile(r"\s+")
_SENTENCE_RE = re.compile(r".+?(?:[.!?](?=\s|$)|$)")


def fallback_card_summary(body: str, *, limit: int = MAX_CARD_SUMMARY_LENGTH) -> str:
    """Return a bounded plain-text summary without calling an agent.

    The fallback keeps at most three sentences, collapses Markdown structure, and
    trims on a word boundary so it is safe to compute during writes/migrations.
    """

    text = _FENCE_RE.sub(lambda match: match.group(1), body or "")
    text = _LINK_RE.sub(r"\1", text)
    text = _MARKUP_RE.sub(r"\1", text)
    text = _SPACE_RE.sub(" ", text).strip()
    if not text:
        return DEFAULT_CARD_SUMMARY

    sentences = [match.group(0).strip() for match in _SENTENCE_RE.finditer(text)]
    summary = " ".join(sentences[:MAX_CARD_SUMMARY_SENTENCES]).strip() or text
    if len(summary) <= limit:
        return summary

    clipped = summary[: max(1, limit - 1)].rstrip()
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0].rstrip()
    return f"{clipped}…"
