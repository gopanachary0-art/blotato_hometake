"""Trigger matching: does an inbound event start a given automation?"""
from __future__ import annotations

import re
import unicodedata

from .definition import MatchConfig, TriggerSpec
from .models import InboundEvent

_NON_WORD = re.compile(r"[^\w\s]+", re.UNICODE)
_SPACES = re.compile(r"\s+")


def normalize(text: str) -> str:
    """Case-fold, strip accents-insensitive compat forms, punctuation and emoji."""
    text = unicodedata.normalize("NFKC", text or "").casefold()
    text = _NON_WORD.sub(" ", text)
    return _SPACES.sub(" ", text).strip()


def text_matches(cfg: MatchConfig, text: str) -> bool:
    if cfg.mode == "any":
        return True
    norm = normalize(text)
    for kw in cfg.keywords:
        k = normalize(kw)
        if not k:
            continue
        if cfg.mode == "exact" and norm == k:
            return True
        # Word-boundary match: "pricing?" matches, "repricing" does not.
        if cfg.mode == "contains_word" and re.search(rf"(?<!\w){re.escape(k)}(?!\w)", norm):
            return True
    return False


def trigger_applies(trigger: TriggerSpec, event: InboundEvent) -> bool:
    if trigger.type is not event.event_type or trigger.platform != event.platform:
        return False
    if trigger.scope == "posts" and event.post_id not in trigger.post_ids:
        return False
    return text_matches(trigger.match, event.text)
