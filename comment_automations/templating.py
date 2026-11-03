"""Minimal, safe message templating: {{ email }}, {{ trigger.username }}, {{ email|url }}.

Deliberately not Jinja: customer-authored templates must not be able to run code.
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote

_TPL = re.compile(r"\{\{\s*([\w.]+)\s*(?:\|\s*(\w+)\s*)?\}\}")

_FILTERS = {
    "url": lambda s: quote(s, safe=""),
    "upper": str.upper,
    "lower": str.lower,
}


def _lookup(path: str, ctx: dict[str, Any]) -> Any:
    head, *rest = path.split(".")
    # Bare names resolve to collected variables; dotted names to ctx sections.
    node: Any = ctx.get("vars", {}).get(head) if not rest else ctx.get(head)
    for part in rest:
        node = node.get(part) if isinstance(node, dict) else None
    return node


def render(template: str, ctx: dict[str, Any]) -> str:
    def sub(m: re.Match[str]) -> str:
        value = _lookup(m.group(1), ctx)
        s = "" if value is None else str(value)
        f = _FILTERS.get(m.group(2) or "")
        return f(s) if f else s

    return _TPL.sub(sub, template)
