"""Automation definitions: parsing and validation.

An automation is a trigger plus a graph of typed steps. Definitions are stored
as JSON and are immutable per version. Validating them at publish time means
the runtime never has to deal with dangling references or unknown types.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from .models import EventType
from .validators import VALIDATORS

END = "end"

STEP_TYPES = {
    "reply_to_comment",
    "send_message",
    "wait_for_reply",
    "condition",
    "set_variable",
    "tag_contact",
}
MATCH_MODES = {"contains_word", "exact", "any"}
REENTRY_POLICIES = {"skip_if_active", "restart", "allow_parallel"}
CONDITION_OPS = {"exists", "eq", "contains", "matches"}

_DURATION_RE = re.compile(r"^\s*(\d+)\s*([smhd])\s*$")
_UNIT_S = {"s": 1, "m": 60, "h": 3600, "d": 86400}


class DefinitionError(ValueError):
    pass


@dataclass(frozen=True)
class MatchConfig:
    keywords: tuple[str, ...]
    mode: str = "contains_word"


@dataclass(frozen=True)
class TriggerSpec:
    type: EventType
    platform: str
    scope: str                       # "any_post" | "posts"
    post_ids: tuple[str, ...]
    match: MatchConfig


@dataclass(frozen=True)
class Step:
    id: str
    type: str
    config: dict[str, Any]
    next: str = END

    def targets(self) -> list[str]:
        """Every step id this step can transition to."""
        if self.type == "condition":
            out = [self.config["then"], self.config["else"]]
        else:
            out = [self.next]
        if self.type == "wait_for_reply":
            out.append(self.config.get("on_timeout", END))
            out.append((self.config.get("on_invalid") or {}).get("next", END))
        return out


@dataclass(frozen=True)
class AutomationDefinition:
    trigger: TriggerSpec
    start: str
    steps: dict[str, Step]
    reentry_policy: str = "skip_if_active"
    raw: dict[str, Any] = field(default_factory=dict)


def parse_duration(value: Any) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
        return float(value)
    if isinstance(value, str):
        m = _DURATION_RE.match(value)
        if m:
            return float(int(m.group(1)) * _UNIT_S[m.group(2)])
    raise DefinitionError(f"invalid duration: {value!r} (use seconds or e.g. '30m', '23h')")


def _require_str(cfg: Mapping[str, Any], key: str, sid: str) -> str:
    v = cfg.get(key)
    if not isinstance(v, str) or not v.strip():
        raise DefinitionError(f"step {sid!r}: '{key}' must be a non-empty string")
    return v


def _parse_trigger(raw: Any) -> TriggerSpec:
    if not isinstance(raw, Mapping):
        raise DefinitionError("'trigger' is required")
    try:
        ttype = EventType(raw.get("type"))
    except ValueError:
        raise DefinitionError(f"unknown trigger type {raw.get('type')!r}") from None

    scope = raw.get("scope", "any_post")
    post_ids = tuple(str(p) for p in raw.get("post_ids", ()))
    if scope not in ("any_post", "posts"):
        raise DefinitionError(f"unknown trigger scope {scope!r}")
    if scope == "posts" and not post_ids:
        raise DefinitionError("scope 'posts' requires 'post_ids'")

    match = raw.get("match") or {}
    mode = match.get("mode", "contains_word")
    if mode not in MATCH_MODES:
        raise DefinitionError(f"unknown match mode {mode!r}")
    keywords = tuple(k for k in match.get("keywords", ()) if isinstance(k, str) and k.strip())
    if mode != "any" and not keywords:
        raise DefinitionError("trigger.match.keywords must contain at least one keyword")

    return TriggerSpec(
        type=ttype,
        platform=raw.get("platform", "instagram"),
        scope=scope,
        post_ids=post_ids,
        match=MatchConfig(keywords=keywords, mode=mode),
    )


def _parse_step(sid: str, raw: Any, trigger: TriggerSpec) -> Step:
    if sid == END:
        raise DefinitionError(f"'{END}' is reserved and cannot be a step id")
    if not isinstance(raw, Mapping):
        raise DefinitionError(f"step {sid!r} must be an object")
    stype = raw.get("type")
    if stype not in STEP_TYPES:
        raise DefinitionError(f"step {sid!r}: unknown type {stype!r}")

    cfg = {k: v for k, v in raw.items() if k not in ("type", "next")}
    nxt = raw.get("next", END)

    if stype == "reply_to_comment":
        _require_str(cfg, "text", sid)
        if trigger.type is not EventType.COMMENT_CREATED:
            raise DefinitionError(f"step {sid!r}: reply_to_comment needs a comment trigger")
    elif stype == "send_message":
        _require_str(cfg, "text", sid)
    elif stype == "wait_for_reply":
        validator = cfg.get("validate", "any")
        if validator not in VALIDATORS:
            raise DefinitionError(f"step {sid!r}: unknown validator {validator!r}")
        cfg["validate"] = validator
        cfg["timeout_s"] = parse_duration(cfg.get("timeout", "23h"))
        on_invalid = cfg.get("on_invalid") or {}
        if not isinstance(on_invalid, Mapping):
            raise DefinitionError(f"step {sid!r}: on_invalid must be an object")
        max_attempts = on_invalid.get("max_attempts", 0)
        if not isinstance(max_attempts, int) or max_attempts < 0:
            raise DefinitionError(f"step {sid!r}: on_invalid.max_attempts must be an int >= 0")
        if max_attempts and not isinstance(on_invalid.get("reprompt"), str):
            raise DefinitionError(f"step {sid!r}: on_invalid.reprompt is required when max_attempts > 0")
        cfg["on_invalid"] = dict(on_invalid)
    elif stype == "condition":
        cond = cfg.get("if")
        if not isinstance(cond, Mapping) or not isinstance(cond.get("var"), str):
            raise DefinitionError(f"step {sid!r}: condition needs 'if': {{'var': ...}}")
        if cond.get("op", "exists") not in CONDITION_OPS:
            raise DefinitionError(f"step {sid!r}: unknown condition op {cond.get('op')!r}")
        for k in ("then", "else"):
            if not isinstance(cfg.get(k), str):
                raise DefinitionError(f"step {sid!r}: condition needs '{k}'")
    elif stype == "set_variable":
        _require_str(cfg, "name", sid)
        if "value" not in cfg:
            raise DefinitionError(f"step {sid!r}: set_variable needs 'value'")
    elif stype == "tag_contact":
        _require_str(cfg, "tag", sid)

    return Step(id=sid, type=stype, config=cfg, next=nxt)


def parse_definition(raw: Mapping[str, Any]) -> AutomationDefinition:
    if not isinstance(raw, Mapping):
        raise DefinitionError("definition must be an object")
    trigger = _parse_trigger(raw.get("trigger"))

    steps_raw = raw.get("steps")
    if not isinstance(steps_raw, Mapping) or not steps_raw:
        raise DefinitionError("'steps' must be a non-empty object")
    steps = {sid: _parse_step(sid, s, trigger) for sid, s in steps_raw.items()}

    start = raw.get("start") or next(iter(steps))
    if start not in steps:
        raise DefinitionError(f"start step {start!r} does not exist")
    for step in steps.values():
        for target in step.targets():
            if target != END and target not in steps:
                raise DefinitionError(f"step {step.id!r} points to unknown step {target!r}")

    policy = raw.get("reentry_policy", "skip_if_active")
    if policy not in REENTRY_POLICIES:
        raise DefinitionError(f"unknown reentry_policy {policy!r}")

    return AutomationDefinition(
        trigger=trigger, start=start, steps=steps, reentry_policy=policy, raw=dict(raw)
    )
