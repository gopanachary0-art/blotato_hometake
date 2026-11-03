"""Workflow runtime: event routing + step interpreter.

Responsibilities
  * Route each inbound event: first try to resume a run waiting on that
    contact, otherwise match triggers and start new runs.
  * Interpret the step graph. Steps never call the platform directly; they
    write to the transactional outbox, which the Worker drains. That makes a
    state transition and its side effect atomic (no "state advanced but DM
    never queued", no "DM sent twice because we crashed before saving").
  * Park runs on wait_for_reply (waits row + timeout timer) and resume them
    from a later DM or a timer firing.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from .definition import END, AutomationDefinition, Step
from .matcher import trigger_applies
from .models import ACTIVE_STATUSES, EventType, InboundEvent, RunStatus
from .store import Store
from .templating import render
from .validators import extract

log = logging.getLogger(__name__)

MAX_TRANSITIONS_PER_TICK = 50  # guards against condition loops in customer graphs


class StepError(Exception):
    """A step cannot run (bad config for this context). Fails the run."""


class ConcurrencyConflict(Exception):
    """Someone else advanced the run first; the caller should retry/requeue."""


@dataclass
class StepOutcome:
    next_step: str
    status: str = RunStatus.RUNNING.value


@dataclass
class EventResult:
    event_id: str
    duplicate: bool = False
    ignored_reason: str | None = None
    resumed_run_ids: list[str] = field(default_factory=list)
    started_run_ids: list[str] = field(default_factory=list)
    skipped_automation_ids: list[str] = field(default_factory=list)
    errored_automation_ids: list[str] = field(default_factory=list)


class AutomationEngine:
    def __init__(self, store: Store, clock: Callable[[], float] = time.time) -> None:
        self.store = store
        self.clock = clock
        self._handlers: dict[str, Callable[..., StepOutcome]] = {
            "reply_to_comment": self._step_reply_to_comment,
            "send_message": self._step_send_message,
            "wait_for_reply": self._step_wait_for_reply,
            "condition": self._step_condition,
            "set_variable": self._step_set_variable,
            "tag_contact": self._step_tag_contact,
        }

    # ================================================================ routing
    def handle_event(self, ev: InboundEvent) -> EventResult:
        result = EventResult(event_id=ev.platform_event_id)
        now = self.clock()
        with self.store.tx():
            if not self.store.record_event(ev, now):
                result.duplicate = True
                return result
            if ev.is_from_self:
                # Our own replies/echoes come back as webhooks: never let them trigger loops.
                result.ignored_reason = "authored_by_account"
                return result

            contact = self.store.upsert_contact(
                ev.account_id, ev.platform, ev.author_platform_id, ev.author_username,
                inbound_at=ev.occurred_at if ev.event_type is EventType.MESSAGE_RECEIVED else None,
            )

            # 1) A DM from someone we're waiting on belongs to that conversation.
            if ev.event_type is EventType.MESSAGE_RECEIVED:
                wait = self.store.find_wait(ev.account_id, ev.platform, contact["id"], ev.channel)
                if wait is not None:
                    self._advance(wait["run_id"], inbound=ev)
                    result.resumed_run_ids.append(wait["run_id"])
                    return result

            # 2) Otherwise, does it start any automation?
            for trig in self.store.triggers_for(ev.account_id, ev.platform, ev.event_type.value):
                defn = self.store.get_definition(trig["version_id"])
                if not trigger_applies(defn.trigger, ev):
                    continue
                try:
                    with self.store.tx():  # savepoint: one broken automation can't block others
                        run_id = self._start_run(trig, defn, contact, ev)
                except Exception:
                    log.exception("failed to start automation %s", trig["automation_id"])
                    result.errored_automation_ids.append(trig["automation_id"])
                    continue
                if run_id:
                    result.started_run_ids.append(run_id)
                else:
                    result.skipped_automation_ids.append(trig["automation_id"])
        return result

    def _start_run(self, trig: dict[str, Any], defn: AutomationDefinition,
                   contact: dict[str, Any], ev: InboundEvent) -> str | None:
        now = self.clock()
        policy = defn.reentry_policy
        active = self.store.find_active_run(trig["automation_id"], contact["id"])
        if active and policy == "skip_if_active":
            return None
        if active and policy == "restart":
            self._terminate(active["id"], RunStatus.CANCELLED, "restarted by a new trigger")

        run_id = f"run_{uuid.uuid4().hex[:20]}"
        context = {
            "trigger": {
                "event_id": ev.platform_event_id,
                "comment_id": ev.comment_id,
                "post_id": ev.post_id,
                "text": ev.text,
                "username": ev.author_username,
                "occurred_at": ev.occurred_at,
            },
            "contact": {"id": contact["id"], "platform_user_id": contact["platform_user_id"],
                        "username": contact["username"]},
            "vars": {},
            "_visits": {},
            "_invalid": {},
        }
        try:
            self.store.insert_run(
                run_id=run_id, automation_id=trig["automation_id"], version_id=trig["version_id"],
                account_id=ev.account_id, contact_id=contact["id"], start_step=defn.start, context=context,
                reentry_slot=run_id if policy == "allow_parallel" else "active",
                trigger_event_id=ev.platform_event_id, now=now,
            )
        except sqlite3.IntegrityError:
            return None  # lost a race with a concurrent start for the same contact
        self.store.log_step(run_id, None, "run_started",
                            {"automation_id": trig["automation_id"], "event": ev.platform_event_id}, now)
        self._advance(run_id)
        return run_id

    # ============================================================ interpreter
    def _advance(self, run_id: str, inbound: InboundEvent | None = None) -> None:
        for _ in range(MAX_TRANSITIONS_PER_TICK):
            run = self.store.get_run(run_id)
            if run is None or run["status"] not in ACTIVE_STATUSES:
                return
            if run["status"] == RunStatus.WAITING.value and inbound is None:
                return  # parked; only an inbound message or a timer can move it
            if run["current_step"] == END:
                self.store.finish_run(run_id, RunStatus.COMPLETED.value, None, self.clock())
                self.store.log_step(run_id, None, "run_completed", None, self.clock())
                return

            defn = self.store.get_definition(run["version_id"])
            step = defn.steps[run["current_step"]]
            ctx = json.loads(run["context"])
            try:
                out = self._handlers[step.type](run, step, ctx, inbound)
            except StepError as e:
                self._terminate(run_id, RunStatus.FAILED, f"step {step.id}: {e}")
                return
            inbound = None

            if not self.store.cas_update_run(run_id, run["rev"], status=out.status,
                                             current_step=out.next_step, context=ctx, now=self.clock()):
                raise ConcurrencyConflict(run_id)
            if out.status == RunStatus.WAITING.value:
                return
        self._terminate(run_id, RunStatus.FAILED, "too many transitions in one tick (loop in graph?)")

    # ----------------------------------------------------------------- steps
    def _visit(self, ctx: dict[str, Any], step: Step) -> int:
        ctx["_visits"][step.id] = ctx["_visits"].get(step.id, 0) + 1
        return ctx["_visits"][step.id]

    def _enqueue(self, run: dict[str, Any], step: Step, ctx: dict[str, Any], action: str,
                 payload: dict[str, Any], suffix: str = "") -> None:
        # Deterministic key: re-executing the same step visit can never create a second send.
        key = f"{run['id']}:{step.id}:{ctx['_visits'].get(step.id, 0)}{suffix}"
        platform = self.store.get_definition(run["version_id"]).trigger.platform
        self.store.enqueue_action(key=key, run_id=run["id"], step_id=step.id, account_id=run["account_id"],
                                  platform=platform, action=action, payload=payload, now=self.clock())
        self.store.log_step(run["id"], step.id, "action_queued", {"action": action, "key": key}, self.clock())

    def _dm_payload(self, ctx: dict[str, Any], text: str) -> dict[str, Any]:
        return {
            "recipient_id": ctx["contact"]["platform_user_id"],
            "text": text,
            # Lets the executor fall back to an Instagram "private reply" when
            # there's no open 24h window yet (first DM after a comment).
            "comment_id": ctx["trigger"].get("comment_id"),
            "comment_created_at": ctx["trigger"].get("occurred_at"),
        }

    def _step_reply_to_comment(self, run, step, ctx, inbound) -> StepOutcome:
        self._visit(ctx, step)
        comment_id = ctx["trigger"].get("comment_id")
        if not comment_id:
            raise StepError("reply_to_comment requires a comment trigger")
        self._enqueue(run, step, ctx, "reply_to_comment",
                      {"comment_id": comment_id, "text": render(step.config["text"], ctx)})
        return StepOutcome(step.next)

    def _step_send_message(self, run, step, ctx, inbound) -> StepOutcome:
        self._visit(ctx, step)
        self._enqueue(run, step, ctx, "send_message", self._dm_payload(ctx, render(step.config["text"], ctx)))
        return StepOutcome(step.next)

    def _step_wait_for_reply(self, run, step, ctx, inbound: InboundEvent | None) -> StepOutcome:
        cfg = step.config
        now = self.clock()

        if inbound is None:  # entering the step: park the run
            self._visit(ctx, step)
            ctx["_invalid"][step.id] = 0
            expires = now + cfg["timeout_s"]
            self.store.insert_wait(run_id=run["id"], step_id=step.id, account_id=run["account_id"],
                                   platform=self.store.get_definition(run["version_id"]).trigger.platform,
                                   contact_id=run["contact_id"], channel="dm", expires_at=expires, now=now)
            self.store.insert_timer(run["id"], step.id, "wait_timeout", expires)
            self.store.log_step(run["id"], step.id, "waiting", {"validate": cfg["validate"], "until": expires}, now)
            return StepOutcome(step.id, RunStatus.WAITING.value)

        # Resumed by a reply.
        ctx["vars"]["last_reply"] = inbound.text
        value = extract(cfg["validate"], inbound.text)
        if value is not None:
            if cfg.get("save_as"):
                ctx["vars"][cfg["save_as"]] = value
                self.store.set_contact_attr(run["contact_id"], cfg["save_as"], value)
            self._clear_wait(run["id"])
            self.store.log_step(run["id"], step.id, "reply_accepted", {"value": value}, now)
            return StepOutcome(step.next)

        invalid = ctx["_invalid"][step.id] = ctx["_invalid"].get(step.id, 0) + 1
        on_invalid = cfg.get("on_invalid") or {}
        self.store.log_step(run["id"], step.id, "reply_rejected", {"text": inbound.text, "count": invalid}, now)
        if invalid <= on_invalid.get("max_attempts", 0):
            self._enqueue(run, step, ctx, "send_message",
                          self._dm_payload(ctx, render(on_invalid["reprompt"], ctx)), suffix=f":reprompt{invalid}")
            return StepOutcome(step.id, RunStatus.WAITING.value)  # keep waiting, same timeout
        self._clear_wait(run["id"])
        return StepOutcome(on_invalid.get("next", END))

    def _step_condition(self, run, step, ctx, inbound) -> StepOutcome:
        self._visit(ctx, step)
        cond = step.config["if"]
        val = ctx["vars"].get(cond["var"])
        op, target = cond.get("op", "exists"), cond.get("value")
        if op == "exists":
            ok = val not in (None, "")
        elif op == "eq":
            ok = str(val) == str(target)
        elif op == "contains":
            ok = str(target).casefold() in str(val or "").casefold()
        else:  # matches
            ok = re.search(str(target), str(val or "")) is not None
        return StepOutcome(step.config["then"] if ok else step.config["else"])

    def _step_set_variable(self, run, step, ctx, inbound) -> StepOutcome:
        self._visit(ctx, step)
        value = step.config["value"]
        ctx["vars"][step.config["name"]] = render(value, ctx) if isinstance(value, str) else value
        return StepOutcome(step.next)

    def _step_tag_contact(self, run, step, ctx, inbound) -> StepOutcome:
        self._visit(ctx, step)
        self.store.add_contact_tag(run["contact_id"], render(step.config["tag"], ctx))
        return StepOutcome(step.next)

    # ================================================================ timers
    def handle_timer(self, timer: dict[str, Any]) -> None:
        with self.store.tx():
            if not self.store.claim_timer(timer["id"]):
                return  # already fired or cancelled
            run = self.store.get_run(timer["run_id"])
            # Compare-and-set semantics: the reply may have won the race.
            if (run is None or run["status"] != RunStatus.WAITING.value
                    or run["current_step"] != timer["step_id"]):
                return
            step = self.store.get_definition(run["version_id"]).steps[timer["step_id"]]
            self._clear_wait(run["id"])
            nxt = step.config.get("on_timeout", END)
            self.store.log_step(run["id"], step.id, "wait_timed_out", {"next": nxt}, self.clock())
            if not self.store.cas_update_run(run["id"], run["rev"], status=RunStatus.RUNNING.value,
                                             current_step=nxt, context=json.loads(run["context"]),
                                             now=self.clock()):
                raise ConcurrencyConflict(run["id"])
            self._advance(run["id"])

    # ============================================================== lifecycle
    def _clear_wait(self, run_id: str) -> None:
        self.store.delete_wait(run_id)
        self.store.cancel_timers(run_id)

    def _terminate(self, run_id: str, status: RunStatus, reason: str) -> None:
        self._clear_wait(run_id)
        self.store.cancel_pending_actions(run_id)
        if self.store.finish_run(run_id, status.value, reason, self.clock()):
            self.store.log_step(run_id, None, f"run_{status.value}", {"reason": reason}, self.clock())

    def fail_run(self, run_id: str, reason: str) -> None:
        with self.store.tx():
            self._terminate(run_id, RunStatus.FAILED, reason)

    def cancel_run(self, run_id: str, reason: str = "cancelled by user") -> None:
        with self.store.tx():
            self._terminate(run_id, RunStatus.CANCELLED, reason)
