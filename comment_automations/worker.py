"""Worker: fires due timers and drains the outbox through rate-limited executors."""
from __future__ import annotations

import json
import logging
import random
import time
from typing import Any, Callable

from .engine import AutomationEngine
from .platform import (PRIVATE_REPLY_WINDOW_S, STANDARD_WINDOW_S, MessagingWindowClosed, PermanentError,
                       PlatformClient, TransientError)
from .store import Store

log = logging.getLogger(__name__)


class TokenBucketLimiter:
    """Per-key token bucket (key = connected account). One viral post queues
    up instead of blowing through Meta's per-account limits."""

    def __init__(self, rate_per_s: float = 5.0, burst: int = 20,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.rate, self.burst, self.clock = rate_per_s, burst, clock
        self._buckets: dict[str, tuple[float, float]] = {}

    def try_acquire(self, key: str) -> bool:
        now = self.clock()
        tokens, last = self._buckets.get(key, (float(self.burst), now))
        tokens = min(self.burst, tokens + (now - last) * self.rate)
        if tokens < 1:
            self._buckets[key] = (tokens, now)
            return False
        self._buckets[key] = (tokens - 1, now)
        return True


class ActionExecutor:
    """Turns outbox rows into platform API calls, enforcing messaging windows."""

    def __init__(self, store: Store, clients: dict[str, PlatformClient], clock: Callable[[], float]) -> None:
        self.store, self.clients, self.clock = store, clients, clock

    def execute(self, row: dict[str, Any]) -> dict[str, Any]:
        client = self.clients.get(row["platform"])
        account = self.store.get_account(row["account_id"])
        if client is None or account is None:
            raise PermanentError(f"no client/account for {row['platform']}/{row['account_id']}")
        payload = json.loads(row["payload"])

        if row["action"] == "reply_to_comment":
            return {"id": client.reply_to_comment(account, payload["comment_id"], payload["text"]),
                    "via": "comment_reply"}
        if row["action"] == "send_message":
            return self._send_message(row, account, client, payload)
        raise PermanentError(f"unknown action {row['action']!r}")

    def _send_message(self, row, account, client, payload) -> dict[str, Any]:
        now = self.clock()
        recipient = payload["recipient_id"]
        contact = self.store.get_contact_by_platform(row["account_id"], row["platform"], recipient)
        last_in = contact["last_inbound_at"] if contact else None

        if last_in is not None and now - last_in < STANDARD_WINDOW_S:
            return {"id": client.send_dm(account, recipient, payload["text"]), "via": "dm"}

        cid = payload.get("comment_id")
        created = payload.get("comment_created_at") or 0
        if cid and now - created < PRIVATE_REPLY_WINDOW_S:
            with self.store.tx():
                owned = self.store.claim_private_reply(cid, row["id"], now)
            if owned:
                return {"id": client.send_private_reply(account, cid, payload["text"]), "via": "private_reply"}

        raise MessagingWindowClosed(
            "no open 24h window and the comment's private reply is used or expired")


class Worker:
    def __init__(self, store: Store, engine: AutomationEngine, clients: dict[str, PlatformClient], *,
                 clock: Callable[[], float] = time.time, limiter: TokenBucketLimiter | None = None,
                 max_attempts: int = 5, backoff_base_s: float = 2.0, backoff_cap_s: float = 300.0,
                 rng: random.Random | None = None) -> None:
        self.store, self.engine, self.clock = store, engine, clock
        self.executor = ActionExecutor(store, clients, clock)
        self.limiter = limiter or TokenBucketLimiter(clock=clock)
        self.max_attempts = max_attempts
        self.backoff_base_s, self.backoff_cap_s = backoff_base_s, backoff_cap_s
        self.rng = rng or random.Random()

    # --------------------------------------------------------------- timers
    def run_due_timers(self, limit: int = 100) -> int:
        fired = 0
        for timer in self.store.due_timers(self.clock(), limit):
            try:
                self.engine.handle_timer(timer)
                fired += 1
            except Exception:
                log.exception("timer %s failed", timer["id"])
        return fired

    # --------------------------------------------------------------- outbox
    def drain_outbox(self, limit: int = 100) -> int:
        now = self.clock()
        processed = 0
        blocked: set[str] = set()
        for row in self.store.due_outbox(now, limit):
            run_id = row["run_id"]
            if run_id in blocked or self.store.has_earlier_pending(run_id, row["id"]):
                blocked.add(run_id)  # preserve per-run ordering
                continue
            if not self.limiter.try_acquire(row["account_id"]):
                blocked.add(run_id)  # stays pending; picked up next tick
                continue
            if not self._execute_row(row, now):
                blocked.add(run_id)
            processed += 1
        return processed

    def _execute_row(self, row: dict[str, Any], now: float) -> bool:
        attempts = row["attempts"] + 1
        try:
            result = self.executor.execute(row)
        except PermanentError as e:
            self._dead_letter(row, attempts, e)
            return False
        except Exception as e:  # TransientError and unknown errors: retry with backoff
            if attempts >= self.max_attempts:
                self._dead_letter(row, attempts, e)
                return False
            delay = min(self.backoff_cap_s, self.backoff_base_s * 2 ** (attempts - 1))
            delay *= 1 + self.rng.random() * 0.1  # jitter
            with self.store.tx():
                self.store.update_outbox(row["id"], status="pending", attempts=attempts,
                                         next_attempt_at=now + delay, error=str(e))
                self.store.log_step(row["run_id"], row["step_id"], "action_retry",
                                    {"action": row["action"], "attempt": attempts, "error": str(e),
                                     "transient": isinstance(e, TransientError)}, now)
            return False
        with self.store.tx():
            self.store.update_outbox(row["id"], status="done", attempts=attempts, result=result)
            self.store.log_step(row["run_id"], row["step_id"], "action_sent",
                                {"action": row["action"], **result}, now)
        return True

    def _dead_letter(self, row: dict[str, Any], attempts: int, err: Exception) -> None:
        with self.store.tx():
            self.store.update_outbox(row["id"], status="dead", attempts=attempts, error=str(err))
            self.store.log_step(row["run_id"], row["step_id"], "action_dead",
                                {"action": row["action"], "error": str(err),
                                 "window_closed": isinstance(err, MessagingWindowClosed)}, self.clock())
        self.engine.fail_run(row["run_id"], f"{row['action']} failed: {err}")

    # ----------------------------------------------------------------- loop
    def tick(self) -> tuple[int, int]:
        return self.run_due_timers(), self.drain_outbox()

    def run_forever(self, interval_s: float = 0.5) -> None:  # pragma: no cover
        while True:
            timers, actions = self.tick()
            if not timers and not actions:
                time.sleep(interval_s)
