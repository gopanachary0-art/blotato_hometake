"""Reply validators for wait_for_reply steps. Each returns the extracted value or None."""
from __future__ import annotations

import re
from typing import Callable

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
_PHONE_RE = re.compile(r"\+?\d[\d\s().-]{6,}\d")


def _email(text: str) -> str | None:
    # Search rather than full-match: people write "sure, it's jane@x.com!"
    m = _EMAIL_RE.search(text or "")
    return m.group(0).lower() if m else None


def _phone(text: str) -> str | None:
    m = _PHONE_RE.search(text or "")
    if not m:
        return None
    digits = re.sub(r"[^\d+]", "", m.group(0))
    return digits if 7 <= len(digits.lstrip("+")) <= 15 else None


def _any(text: str) -> str | None:
    text = (text or "").strip()
    return text or None


VALIDATORS: dict[str, Callable[[str], str | None]] = {
    "email": _email,
    "phone": _phone,
    "any": _any,
}


def extract(validator: str, text: str) -> str | None:
    return VALIDATORS[validator](text)
