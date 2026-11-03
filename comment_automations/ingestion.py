"""Webhook ingestion: signature verification + normalisation to InboundEvent.

In production this layer only verifies, persists and publishes to the event
bus (partitioned by account_id), then returns 200 within milliseconds. Here the
FastAPI app calls the engine directly to keep the reference self-contained.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Any, Callable

from .models import EventType, InboundEvent

log = logging.getLogger(__name__)

AccountResolver = Callable[[str, str], "str | None"]  # (platform, platform_account_id) -> account_id


def verify_signature(app_secret: str, body: bytes, header: str | None) -> bool:
    """Validate Meta's X-Hub-Signature-256 header ("sha256=<hex>")."""
    if not header or not header.startswith("sha256="):
        return False
    expected = hmac.new(app_secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header.split("=", 1)[1])


def normalize_instagram_webhook(payload: dict[str, Any], resolve_account: AccountResolver,
                                ) -> list[InboundEvent]:
    if payload.get("object") != "instagram":
        return []
    events: list[InboundEvent] = []
    for entry in payload.get("entry", []):
        ig_id = str(entry.get("id"))
        account_id = resolve_account("instagram", ig_id)
        if account_id is None:
            log.warning("webhook for unknown instagram account %s", ig_id)
            continue
        entry_time = float(entry.get("time") or 0)

        for change in entry.get("changes", []):
            if change.get("field") != "comments":
                continue
            v = change.get("value") or {}
            if "id" not in v:
                continue
            author = v.get("from") or {}
            author_id = str(author.get("id", ""))
            events.append(InboundEvent(
                platform_event_id=f"ig:comment:{v['id']}",
                event_type=EventType.COMMENT_CREATED,
                platform="instagram",
                account_id=account_id,
                author_platform_id=author_id,
                author_username=author.get("username"),
                text=v.get("text", ""),
                occurred_at=entry_time,
                post_id=(v.get("media") or {}).get("id"),
                comment_id=str(v["id"]),
                is_from_self=author_id == ig_id,
                raw=v,
            ))

        for m in entry.get("messaging", []):
            msg = m.get("message")
            if not msg or "mid" not in msg:
                continue  # read receipts, reactions, postbacks: not handled in v1
            sender = str((m.get("sender") or {}).get("id", ""))
            is_echo = bool(msg.get("is_echo")) or sender == ig_id
            ts = m.get("timestamp")
            events.append(InboundEvent(
                platform_event_id=f"ig:message:{msg['mid']}",
                event_type=EventType.MESSAGE_RECEIVED,
                platform="instagram",
                account_id=account_id,
                author_platform_id=sender,
                text=msg.get("text", ""),
                occurred_at=float(ts) / 1000.0 if ts else entry_time,
                message_id=msg["mid"],
                channel="dm",
                is_from_self=is_echo,
                raw=m,
            ))
    return events
