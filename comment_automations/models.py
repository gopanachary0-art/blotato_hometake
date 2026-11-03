"""Core value types shared across the system."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventType(str, Enum):
    COMMENT_CREATED = "comment_created"
    MESSAGE_RECEIVED = "message_received"


class RunStatus(str, Enum):
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


ACTIVE_STATUSES = (RunStatus.RUNNING.value, RunStatus.WAITING.value)


@dataclass(frozen=True)
class InboundEvent:
    """A platform-agnostic event produced by a webhook normalizer.

    Everything downstream of ingestion (router, runtime) only sees this type,
    so adding a new platform means writing a new normalizer + executor only.
    """

    platform_event_id: str          # globally unique, used for dedupe
    event_type: EventType
    platform: str                   # "instagram", later "facebook", "tiktok", ...
    account_id: str                 # OUR internal connected-account id
    author_platform_id: str         # who wrote the comment / message
    text: str
    occurred_at: float              # epoch seconds, as reported by the platform
    post_id: str | None = None
    comment_id: str | None = None
    message_id: str | None = None
    author_username: str | None = None
    channel: str = "dm"
    is_from_self: bool = False      # authored by the connected account itself
    raw: dict[str, Any] = field(default_factory=dict)
