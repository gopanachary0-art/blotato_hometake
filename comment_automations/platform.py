"""Platform clients. The engine never imports these; only the Worker does.

Instagram messaging rules that shape the executor:
  * Private reply: ONE DM to a commenter, referencing the comment, within
    7 days of the comment. That's how the first DM after "pricing" is sent.
  * Standard window: after the user messages you, you may message them for
    24 hours. The email-received follow-up relies on this.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Callable, Protocol

PRIVATE_REPLY_WINDOW_S = 7 * 24 * 3600
STANDARD_WINDOW_S = 24 * 3600


class PlatformError(Exception):
    pass


class TransientError(PlatformError):
    """Retry with backoff (rate limits, 5xx, network)."""


class PermanentError(PlatformError):
    """Do not retry (bad request, revoked token, policy violation)."""


class MessagingWindowClosed(PermanentError):
    """We're not allowed to message this user right now."""


class PlatformClient(Protocol):
    def reply_to_comment(self, account: dict[str, Any], comment_id: str, text: str) -> str: ...
    def send_private_reply(self, account: dict[str, Any], comment_id: str, text: str) -> str: ...
    def send_dm(self, account: dict[str, Any], recipient_id: str, text: str) -> str: ...


# Meta error codes that are throttling, i.e. safe to retry later.
_RATE_LIMIT_CODES = {4, 17, 32, 613}


def classify_meta_error(http_status: int, body: dict[str, Any]) -> PlatformError:
    err = body.get("error") or {}
    code, sub = err.get("code"), err.get("error_subcode")
    msg = err.get("message") or f"HTTP {http_status}"
    if http_status == 429 or http_status >= 500 or code in _RATE_LIMIT_CODES or err.get("is_transient"):
        return TransientError(msg)
    if code == 10 and sub == 2018278:  # "message sent outside of allowed window"
        return MessagingWindowClosed(msg)
    # e.g. 190 = invalid/expired token -> should also flag the account for re-auth.
    return PermanentError(msg)


class InstagramGraphClient:
    """Instagram API with Instagram Login (graph.instagram.com)."""

    def __init__(self, base_url: str = "https://graph.instagram.com", api_version: str = "v21.0",
                 timeout: float = 10.0,
                 opener: Callable[..., Any] = urllib.request.urlopen) -> None:
        self.base = f"{base_url.rstrip('/')}/{api_version}"
        self.timeout = timeout
        self._open = opener

    def _post(self, account: dict[str, Any], path: str, body: dict[str, Any]) -> dict[str, Any]:
        req = urllib.request.Request(
            f"{self.base}/{path}",
            data=json.dumps(body).encode(),
            method="POST",
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {account['access_token']}"},
        )
        try:
            with self._open(req, timeout=self.timeout) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            try:
                payload = json.loads(e.read() or b"{}")
            except ValueError:
                payload = {}
            raise classify_meta_error(e.code, payload) from e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            raise TransientError(str(e)) from e

    def reply_to_comment(self, account, comment_id, text):
        return str(self._post(account, f"{comment_id}/replies", {"message": text}).get("id", ""))

    def send_private_reply(self, account, comment_id, text):
        r = self._post(account, "me/messages", {"recipient": {"comment_id": comment_id}, "message": {"text": text}})
        return str(r.get("message_id", ""))

    def send_dm(self, account, recipient_id, text):
        r = self._post(account, "me/messages", {"recipient": {"id": recipient_id}, "message": {"text": text}})
        return str(r.get("message_id", ""))


class FakeInstagramClient:
    """In-memory client for tests and the demo. Records every call."""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self._failures: list[Exception] = []

    def fail_next(self, *errors: Exception) -> None:
        self._failures.extend(errors)

    def _record(self, kind: str, **fields: Any) -> str:
        if self._failures:
            raise self._failures.pop(0)
        mid = f"fake_{len(self.sent) + 1}"
        self.sent.append({"kind": kind, "id": mid, **fields})
        return mid

    def reply_to_comment(self, account, comment_id, text):
        return self._record("comment_reply", comment_id=comment_id, text=text)

    def send_private_reply(self, account, comment_id, text):
        return self._record("private_reply", comment_id=comment_id, text=text)

    def send_dm(self, account, recipient_id, text):
        return self._record("dm", recipient_id=recipient_id, text=text)
