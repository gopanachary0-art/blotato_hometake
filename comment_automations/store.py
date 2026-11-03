"""Persistence. SQLite here so the reference runs anywhere; the schema and the
transactional patterns (outbox, compare-and-set on runs, partial unique index
for re-entry) map one-to-one onto Postgres.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

from .definition import AutomationDefinition, parse_definition
from .models import InboundEvent

SCHEMA = """
CREATE TABLE IF NOT EXISTS connected_accounts (
  account_id          TEXT PRIMARY KEY,
  platform            TEXT NOT NULL,
  platform_account_id TEXT NOT NULL,
  access_token        TEXT NOT NULL,          -- encrypt at rest in production
  UNIQUE (platform, platform_account_id)
);
CREATE TABLE IF NOT EXISTS automations (
  id                TEXT PRIMARY KEY,
  account_id        TEXT NOT NULL REFERENCES connected_accounts(account_id),
  name              TEXT,
  status            TEXT NOT NULL,            -- active | paused
  active_version_id TEXT,
  created_at        REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS automation_versions (   -- immutable
  id            TEXT PRIMARY KEY,
  automation_id TEXT NOT NULL REFERENCES automations(id),
  version       INTEGER NOT NULL,
  definition    TEXT NOT NULL,
  created_at    REAL NOT NULL,
  UNIQUE (automation_id, version)
);
CREATE TABLE IF NOT EXISTS triggers (               -- denormalised for fast routing
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  automation_id TEXT NOT NULL,
  version_id    TEXT NOT NULL,
  account_id    TEXT NOT NULL,
  platform      TEXT NOT NULL,
  event_type    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_triggers_lookup ON triggers (account_id, platform, event_type);
CREATE TABLE IF NOT EXISTS events (                 -- webhook dedupe + replay
  platform_event_id TEXT PRIMARY KEY,
  platform          TEXT NOT NULL,
  event_type        TEXT NOT NULL,
  payload           TEXT NOT NULL,
  received_at       REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS contacts (
  id               TEXT PRIMARY KEY,
  account_id       TEXT NOT NULL,
  platform         TEXT NOT NULL,
  platform_user_id TEXT NOT NULL,
  username         TEXT,
  last_inbound_at  REAL,                       -- drives the 24h messaging window
  attributes       TEXT NOT NULL DEFAULT '{}',
  tags             TEXT NOT NULL DEFAULT '[]',
  UNIQUE (account_id, platform, platform_user_id)
);
CREATE TABLE IF NOT EXISTS runs (
  id               TEXT PRIMARY KEY,
  automation_id    TEXT NOT NULL,
  version_id       TEXT NOT NULL,              -- runs are pinned to a version
  account_id       TEXT NOT NULL,
  contact_id       TEXT NOT NULL,
  status           TEXT NOT NULL,
  current_step     TEXT NOT NULL,
  context          TEXT NOT NULL,
  reentry_slot     TEXT NOT NULL,
  rev              INTEGER NOT NULL DEFAULT 0, -- optimistic concurrency
  trigger_event_id TEXT,
  error            TEXT,
  created_at       REAL NOT NULL,
  updated_at       REAL NOT NULL
);
-- One active run per (automation, contact) unless reentry_policy = allow_parallel
CREATE UNIQUE INDEX IF NOT EXISTS ux_runs_active
  ON runs (automation_id, contact_id, reentry_slot) WHERE status IN ('running', 'waiting');
CREATE TABLE IF NOT EXISTS run_steps (               -- audit trail / customer timeline
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id  TEXT NOT NULL,
  step_id TEXT,
  kind    TEXT NOT NULL,
  detail  TEXT,
  ts      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_run_steps_run ON run_steps (run_id, id);
CREATE TABLE IF NOT EXISTS waits (
  run_id     TEXT PRIMARY KEY,
  step_id    TEXT NOT NULL,
  account_id TEXT NOT NULL,
  platform   TEXT NOT NULL,
  contact_id TEXT NOT NULL,
  channel    TEXT NOT NULL,
  expires_at REAL NOT NULL,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_waits_lookup ON waits (account_id, platform, contact_id, channel);
CREATE TABLE IF NOT EXISTS timers (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id  TEXT NOT NULL,
  step_id TEXT NOT NULL,
  kind    TEXT NOT NULL,
  fire_at REAL NOT NULL,
  status  TEXT NOT NULL DEFAULT 'pending'      -- pending | fired | cancelled
);
CREATE INDEX IF NOT EXISTS ix_timers_due ON timers (status, fire_at);
CREATE TABLE IF NOT EXISTS outbox (                  -- transactional outbox for side effects
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  idempotency_key TEXT NOT NULL UNIQUE,
  run_id          TEXT NOT NULL,
  step_id         TEXT NOT NULL,
  account_id      TEXT NOT NULL,
  platform        TEXT NOT NULL,
  action          TEXT NOT NULL,
  payload         TEXT NOT NULL,
  status          TEXT NOT NULL DEFAULT 'pending', -- pending | done | dead | cancelled
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at REAL NOT NULL,
  last_error      TEXT,
  result          TEXT,
  created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_outbox_due ON outbox (status, next_attempt_at);
CREATE TABLE IF NOT EXISTS private_replies (         -- Instagram allows ONE per comment
  comment_id TEXT PRIMARY KEY,
  outbox_id  INTEGER NOT NULL,
  ts         REAL NOT NULL
);
"""


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:20]}"


def _row(r: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(r) if r is not None else None


class Store:
    def __init__(self, path: str = ":memory:") -> None:
        self.conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)
        self._lock = threading.RLock()
        self._depth = 0
        self._defs: dict[str, AutomationDefinition] = {}

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------ tx
    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """Transaction; nested calls become savepoints so an inner failure
        (e.g. one automation erroring) can roll back without losing the rest."""
        with self._lock:
            self._depth += 1
            sp = f"sp{self._depth}"
            self.conn.execute("BEGIN IMMEDIATE" if self._depth == 1 else f"SAVEPOINT {sp}")
            try:
                yield self.conn
            except BaseException:
                self.conn.execute("ROLLBACK" if self._depth == 1 else f"ROLLBACK TO {sp}")
                if self._depth > 1:
                    self.conn.execute(f"RELEASE {sp}")
                raise
            else:
                self.conn.execute("COMMIT" if self._depth == 1 else f"RELEASE {sp}")
            finally:
                self._depth -= 1

    def _q(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def _one(self, sql: str, *args: Any) -> dict[str, Any] | None:
        return _row(self.conn.execute(sql, args).fetchone())

    # ------------------------------------------------------------ accounts
    def register_account(self, account_id: str, platform: str, platform_account_id: str,
                         access_token: str) -> None:
        with self.tx():
            self.conn.execute(
                "INSERT INTO connected_accounts VALUES (?,?,?,?) "
                "ON CONFLICT(account_id) DO UPDATE SET access_token = excluded.access_token",
                (account_id, platform, platform_account_id, access_token),
            )

    def account_for_platform_id(self, platform: str, platform_account_id: str) -> str | None:
        r = self._one("SELECT account_id FROM connected_accounts WHERE platform=? AND platform_account_id=?",
                      platform, platform_account_id)
        return r["account_id"] if r else None

    def get_account(self, account_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM connected_accounts WHERE account_id=?", account_id)

    # --------------------------------------------------------- automations
    def create_automation(self, account_id: str, definition: dict[str, Any], *, name: str = "",
                          now: float) -> str:
        parse_definition(definition)  # fail fast before writing anything
        automation_id = _new_id("aut")
        with self.tx():
            self.conn.execute("INSERT INTO automations VALUES (?,?,?,?,?,?)",
                              (automation_id, account_id, name, "active", None, now))
            self.publish_version(automation_id, definition, now=now)
        return automation_id

    def publish_version(self, automation_id: str, definition: dict[str, Any], *, now: float) -> str:
        defn = parse_definition(definition)
        version_id = _new_id("ver")
        with self.tx():
            aut = self._one("SELECT * FROM automations WHERE id=?", automation_id)
            if aut is None:
                raise KeyError(automation_id)
            n = self._one("SELECT COALESCE(MAX(version),0)+1 AS n FROM automation_versions WHERE automation_id=?",
                          automation_id)["n"]
            self.conn.execute("INSERT INTO automation_versions VALUES (?,?,?,?,?)",
                              (version_id, automation_id, n, json.dumps(definition), now))
            self.conn.execute("UPDATE automations SET active_version_id=? WHERE id=?", (version_id, automation_id))
            # Swap the routing index atomically. In-flight runs keep their own version.
            self.conn.execute("DELETE FROM triggers WHERE automation_id=?", (automation_id,))
            self.conn.execute(
                "INSERT INTO triggers (automation_id, version_id, account_id, platform, event_type) VALUES (?,?,?,?,?)",
                (automation_id, version_id, aut["account_id"], defn.trigger.platform, defn.trigger.type.value),
            )
        return version_id

    def set_automation_status(self, automation_id: str, status: str) -> None:
        with self.tx():
            self.conn.execute("UPDATE automations SET status=? WHERE id=?", (status, automation_id))

    def triggers_for(self, account_id: str, platform: str, event_type: str) -> list[dict[str, Any]]:
        return self._q(
            "SELECT t.* FROM triggers t JOIN automations a ON a.id = t.automation_id "
            "WHERE t.account_id=? AND t.platform=? AND t.event_type=? "
            "AND a.status='active' AND a.active_version_id = t.version_id ORDER BY t.id",
            account_id, platform, event_type,
        )

    def get_definition(self, version_id: str) -> AutomationDefinition:
        # Versions are immutable, so caching forever is safe.
        defn = self._defs.get(version_id)
        if defn is None:
            r = self._one("SELECT definition FROM automation_versions WHERE id=?", version_id)
            if r is None:
                raise KeyError(version_id)
            defn = self._defs[version_id] = parse_definition(json.loads(r["definition"]))
        return defn

    # -------------------------------------------------------------- events
    def record_event(self, ev: InboundEvent, now: float) -> bool:
        """Returns False if we've already seen this event (webhook retry)."""
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?)",
            (ev.platform_event_id, ev.platform, ev.event_type.value, json.dumps(ev.raw, default=str), now),
        )
        return cur.rowcount == 1

    # ------------------------------------------------------------ contacts
    def upsert_contact(self, account_id: str, platform: str, platform_user_id: str,
                       username: str | None, inbound_at: float | None) -> dict[str, Any]:
        self.conn.execute(
            "INSERT INTO contacts (id, account_id, platform, platform_user_id, username, last_inbound_at) "
            "VALUES (?,?,?,?,?,?) ON CONFLICT(account_id, platform, platform_user_id) DO UPDATE SET "
            "username = COALESCE(excluded.username, contacts.username), "
            "last_inbound_at = MAX(COALESCE(contacts.last_inbound_at, 0), COALESCE(excluded.last_inbound_at, 0))",
            (_new_id("con"), account_id, platform, platform_user_id, username, inbound_at),
        )
        return self.get_contact_by_platform(account_id, platform, platform_user_id)  # type: ignore[return-value]

    def get_contact(self, contact_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM contacts WHERE id=?", contact_id)

    def get_contact_by_platform(self, account_id: str, platform: str, platform_user_id: str) -> dict[str, Any] | None:
        r = self._one("SELECT * FROM contacts WHERE account_id=? AND platform=? AND platform_user_id=?",
                      account_id, platform, platform_user_id)
        if r and not r["last_inbound_at"]:
            r["last_inbound_at"] = None
        return r

    def set_contact_attr(self, contact_id: str, key: str, value: Any) -> None:
        c = self.get_contact(contact_id)
        attrs = json.loads(c["attributes"])
        attrs[key] = value
        self.conn.execute("UPDATE contacts SET attributes=? WHERE id=?", (json.dumps(attrs), contact_id))

    def add_contact_tag(self, contact_id: str, tag: str) -> None:
        c = self.get_contact(contact_id)
        tags = json.loads(c["tags"])
        if tag not in tags:
            tags.append(tag)
            self.conn.execute("UPDATE contacts SET tags=? WHERE id=?", (json.dumps(tags), contact_id))

    # ---------------------------------------------------------------- runs
    def insert_run(self, *, run_id: str, automation_id: str, version_id: str, account_id: str,
                   contact_id: str, start_step: str, context: dict[str, Any], reentry_slot: str,
                   trigger_event_id: str, now: float) -> None:
        self.conn.execute(
            "INSERT INTO runs (id, automation_id, version_id, account_id, contact_id, status, current_step, "
            "context, reentry_slot, trigger_event_id, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, automation_id, version_id, account_id, contact_id, "running", start_step,
             json.dumps(context), reentry_slot, trigger_event_id, now, now),
        )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM runs WHERE id=?", run_id)

    def find_active_run(self, automation_id: str, contact_id: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM runs WHERE automation_id=? AND contact_id=? AND status IN ('running','waiting') "
            "ORDER BY created_at LIMIT 1", automation_id, contact_id)

    def cas_update_run(self, run_id: str, rev: int, *, status: str, current_step: str,
                       context: dict[str, Any], now: float) -> bool:
        cur = self.conn.execute(
            "UPDATE runs SET status=?, current_step=?, context=?, rev=rev+1, updated_at=? WHERE id=? AND rev=?",
            (status, current_step, json.dumps(context), now, run_id, rev),
        )
        return cur.rowcount == 1

    def finish_run(self, run_id: str, status: str, error: str | None, now: float) -> bool:
        cur = self.conn.execute(
            "UPDATE runs SET status=?, error=?, rev=rev+1, updated_at=? "
            "WHERE id=? AND status IN ('running','waiting')",
            (status, error, now, run_id),
        )
        return cur.rowcount == 1

    def log_step(self, run_id: str, step_id: str | None, kind: str, detail: Any, now: float) -> None:
        self.conn.execute("INSERT INTO run_steps (run_id, step_id, kind, detail, ts) VALUES (?,?,?,?,?)",
                          (run_id, step_id, kind, json.dumps(detail, default=str), now))

    def run_timeline(self, run_id: str) -> list[dict[str, Any]]:
        rows = self._q("SELECT step_id, kind, detail, ts FROM run_steps WHERE run_id=? ORDER BY id", run_id)
        for r in rows:
            r["detail"] = json.loads(r["detail"]) if r["detail"] else None
        return rows

    def runs_for_automation(self, automation_id: str) -> list[dict[str, Any]]:
        return self._q("SELECT * FROM runs WHERE automation_id=? ORDER BY created_at, id", automation_id)

    # --------------------------------------------------------------- waits
    def insert_wait(self, *, run_id: str, step_id: str, account_id: str, platform: str,
                    contact_id: str, channel: str, expires_at: float, now: float) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO waits VALUES (?,?,?,?,?,?,?,?)",
            (run_id, step_id, account_id, platform, contact_id, channel, expires_at, now),
        )

    def find_wait(self, account_id: str, platform: str, contact_id: str, channel: str) -> dict[str, Any] | None:
        # If several automations wait on the same contact, the most recent one
        # is the conversation the person is most likely replying to.
        return self._one(
            "SELECT * FROM waits WHERE account_id=? AND platform=? AND contact_id=? AND channel=? "
            "ORDER BY created_at DESC LIMIT 1", account_id, platform, contact_id, channel)

    def delete_wait(self, run_id: str) -> None:
        self.conn.execute("DELETE FROM waits WHERE run_id=?", (run_id,))

    # -------------------------------------------------------------- timers
    def insert_timer(self, run_id: str, step_id: str, kind: str, fire_at: float) -> None:
        self.conn.execute("INSERT INTO timers (run_id, step_id, kind, fire_at) VALUES (?,?,?,?)",
                          (run_id, step_id, kind, fire_at))

    def due_timers(self, now: float, limit: int) -> list[dict[str, Any]]:
        # Postgres: SELECT ... FOR UPDATE SKIP LOCKED so many workers can share the table.
        return self._q("SELECT * FROM timers WHERE status='pending' AND fire_at<=? ORDER BY fire_at LIMIT ?",
                       now, limit)

    def claim_timer(self, timer_id: int) -> bool:
        cur = self.conn.execute("UPDATE timers SET status='fired' WHERE id=? AND status='pending'", (timer_id,))
        return cur.rowcount == 1

    def cancel_timers(self, run_id: str) -> None:
        self.conn.execute("UPDATE timers SET status='cancelled' WHERE run_id=? AND status='pending'", (run_id,))

    # -------------------------------------------------------------- outbox
    def enqueue_action(self, *, key: str, run_id: str, step_id: str, account_id: str, platform: str,
                       action: str, payload: dict[str, Any], now: float) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO outbox (idempotency_key, run_id, step_id, account_id, platform, action, "
            "payload, next_attempt_at, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (key, run_id, step_id, account_id, platform, action, json.dumps(payload), now, now),
        )
        return cur.rowcount == 1

    def due_outbox(self, now: float, limit: int) -> list[dict[str, Any]]:
        return self._q("SELECT * FROM outbox WHERE status='pending' AND next_attempt_at<=? ORDER BY id LIMIT ?",
                       now, limit)

    def has_earlier_pending(self, run_id: str, outbox_id: int) -> bool:
        """Per-run ordering: never send step N+1 while step N is still retrying."""
        return self._one("SELECT 1 AS x FROM outbox WHERE run_id=? AND id<? AND status='pending' LIMIT 1",
                         run_id, outbox_id) is not None

    def update_outbox(self, outbox_id: int, *, status: str, attempts: int, next_attempt_at: float | None = None,
                      error: str | None = None, result: Any = None) -> None:
        self.conn.execute(
            "UPDATE outbox SET status=?, attempts=?, next_attempt_at=COALESCE(?, next_attempt_at), "
            "last_error=?, result=? WHERE id=?",
            (status, attempts, next_attempt_at, error, json.dumps(result) if result is not None else None, outbox_id),
        )

    def cancel_pending_actions(self, run_id: str) -> None:
        self.conn.execute("UPDATE outbox SET status='cancelled' WHERE run_id=? AND status='pending'", (run_id,))

    def outbox_for_run(self, run_id: str) -> list[dict[str, Any]]:
        return self._q("SELECT * FROM outbox WHERE run_id=? ORDER BY id", run_id)

    def claim_private_reply(self, comment_id: str, outbox_id: int, now: float) -> bool:
        """True if this outbox row owns the single private reply for the comment.
        Idempotent per outbox row so a retried send can still use it."""
        self.conn.execute("INSERT OR IGNORE INTO private_replies VALUES (?,?,?)", (comment_id, outbox_id, now))
        r = self._one("SELECT outbox_id FROM private_replies WHERE comment_id=?", comment_id)
        return r is not None and r["outbox_id"] == outbox_id
