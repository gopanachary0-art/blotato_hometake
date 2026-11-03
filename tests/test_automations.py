import copy
import hashlib
import hmac
import json
import unittest

from comment_automations import (AutomationEngine, DefinitionError, FakeInstagramClient, Store, Worker,
                                 normalize_instagram_webhook, parse_definition, verify_signature)
from comment_automations.examples import PRICING_AUTOMATION
from comment_automations.matcher import text_matches
from comment_automations.definition import MatchConfig
from comment_automations.platform import PermanentError, TransientError

IG = "17841400000000001"
DAY = 86400


class Clock:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


_OPEN = []


class Env:
    def __init__(self, definition=PRICING_AUTOMATION):
        self.clock = Clock()
        self.store = Store()
        _OPEN.append(self.store)
        self.engine = AutomationEngine(self.store, self.clock)
        self.ig = FakeInstagramClient()
        self.worker = Worker(self.store, self.engine, {"instagram": self.ig}, clock=self.clock,
                             backoff_base_s=1.0)
        self.store.register_account("acct_1", "instagram", IG, "token")
        self.automation_id = self.store.create_automation("acct_1", definition, now=self.clock())

    def send(self, payload):
        evs = normalize_instagram_webhook(payload, self.store.account_for_platform_id)
        return [self.engine.handle_event(e) for e in evs]

    def comment(self, cid, text, user="900", media="m1", t=None, author=None):
        t = self.clock() if t is None else t
        return self.send({"object": "instagram", "entry": [{"id": IG, "time": t, "changes": [{
            "field": "comments", "value": {"id": cid, "text": text, "media": {"id": media},
                                           "from": {"id": author or user, "username": "jane"}}}]}]})

    def dm(self, mid, text, user="900"):
        return self.send({"object": "instagram", "entry": [{"id": IG, "time": self.clock(), "messaging": [{
            "sender": {"id": user}, "recipient": {"id": IG}, "timestamp": int(self.clock() * 1000),
            "message": {"mid": mid, "text": text}}]}]})

    def runs(self):
        return self.store.runs_for_automation(self.automation_id)

    def kinds(self):
        return [s["kind"] for s in self.ig.sent]


class TestCase(unittest.TestCase):
    def tearDown(self):
        while _OPEN:
            _OPEN.pop().close()


class HappyPathTest(TestCase):
    def test_full_flow(self):
        env = Env()
        [res] = env.comment("c1", "What's the PRICING?? 🔥")
        self.assertEqual(len(res.started_run_ids), 1)
        env.worker.tick()
        self.assertEqual(env.kinds(), ["comment_reply", "private_reply"])
        self.assertEqual(env.ig.sent[0]["text"], "Sent you a DM!")
        self.assertEqual(env.ig.sent[1]["comment_id"], "c1")
        self.assertEqual(env.runs()[0]["status"], "waiting")

        env.clock.t += 120
        [res] = env.dm("mid1", "sure, it's Jane@Example.com")
        self.assertEqual(res.resumed_run_ids, [env.runs()[0]["id"]])
        env.worker.tick()
        self.assertEqual(env.kinds(), ["comment_reply", "private_reply", "dm"])
        self.assertIn("e=jane%40example.com", env.ig.sent[2]["text"])

        run = env.runs()[0]
        self.assertEqual(run["status"], "completed")
        contact = env.store.get_contact(run["contact_id"])
        self.assertEqual(json.loads(contact["attributes"])["email"], "jane@example.com")
        self.assertEqual(json.loads(contact["tags"]), ["lead:pricing"])


class RoutingTest(TestCase):
    def test_duplicate_webhook_is_ignored(self):
        env = Env()
        env.comment("c1", "pricing")
        [res] = env.comment("c1", "pricing")
        self.assertTrue(res.duplicate)
        env.worker.tick()
        self.assertEqual(len(env.runs()), 1)
        self.assertEqual(len(env.ig.sent), 2)

    def test_keyword_matching(self):
        cfg = MatchConfig(keywords=("pricing",))
        self.assertTrue(text_matches(cfg, "PRICING!!"))
        self.assertTrue(text_matches(cfg, "send me pricing pls"))
        self.assertFalse(text_matches(cfg, "repricing soon"))
        self.assertFalse(text_matches(cfg, "price?"))
        self.assertTrue(text_matches(MatchConfig(keywords=("price list",)), "Price-list?"))

    def test_non_matching_comment_starts_nothing(self):
        env = Env()
        [res] = env.comment("c1", "love this")
        self.assertEqual(res.started_run_ids, [])

    def test_own_comment_is_ignored(self):
        env = Env()
        [res] = env.comment("c1", "pricing", author=IG)
        self.assertEqual(res.ignored_reason, "authored_by_account")
        self.assertEqual(env.runs(), [])

    def test_reentry_skip_if_active(self):
        env = Env()
        env.comment("c1", "pricing", media="m1")
        [res] = env.comment("c2", "pricing", media="m2")
        self.assertEqual(res.started_run_ids, [])
        self.assertEqual(len(env.runs()), 1)

    def test_reentry_restart(self):
        d = copy.deepcopy(PRICING_AUTOMATION)
        d["reentry_policy"] = "restart"
        env = Env(d)
        env.comment("c1", "pricing")
        env.comment("c2", "pricing")
        self.assertEqual(sorted(r["status"] for r in env.runs()), ["cancelled", "waiting"])

    def test_paused_automation_does_not_trigger(self):
        env = Env()
        env.store.set_automation_status(env.automation_id, "paused")
        [res] = env.comment("c1", "pricing")
        self.assertEqual(res.started_run_ids, [])

    def test_post_scope(self):
        d = copy.deepcopy(PRICING_AUTOMATION)
        d["trigger"].update(scope="posts", post_ids=["m2"])
        env = Env(d)
        self.assertEqual(env.comment("c1", "pricing", media="m1")[0].started_run_ids, [])
        self.assertEqual(len(env.comment("c2", "pricing", media="m2")[0].started_run_ids), 1)


class WaitTest(TestCase):
    def test_invalid_reply_reprompts_then_accepts(self):
        env = Env()
        env.comment("c1", "pricing")
        env.worker.tick()
        env.dm("m1", "why do you need it?")
        env.worker.tick()
        self.assertEqual(env.kinds()[-1], "dm")
        self.assertIn("doesn't look like an email", env.ig.sent[-1]["text"])
        env.dm("m2", "a@b.co")
        env.worker.tick()
        self.assertIn("pricing", env.ig.sent[-1]["text"])
        self.assertEqual(env.runs()[0]["status"], "completed")

    def test_gives_up_after_max_attempts(self):
        env = Env()
        env.comment("c1", "pricing")
        env.worker.tick()
        for i in range(3):
            env.dm(f"m{i}", "nope")
            env.worker.tick()
        # 1 comment reply + 1 private reply + 2 reprompts, no link
        self.assertEqual(env.kinds(), ["comment_reply", "private_reply", "dm", "dm"])
        self.assertEqual(env.runs()[0]["status"], "completed")

    def test_timeout_ends_run_and_later_dm_is_not_captured(self):
        env = Env()
        env.comment("c1", "pricing")
        env.worker.tick()
        env.clock.t += 23 * 3600 + 1
        env.worker.tick()
        self.assertEqual(env.runs()[0]["status"], "completed")
        [res] = env.dm("m1", "late@x.com")
        self.assertEqual(res.resumed_run_ids, [])

    def test_reply_before_timeout_cancels_timer(self):
        env = Env()
        env.comment("c1", "pricing")
        env.dm("m1", "a@b.com")
        env.clock.t += DAY
        self.assertEqual(env.worker.run_due_timers(), 0)

    def test_dm_without_wait_is_ignored(self):
        env = Env()
        [res] = env.dm("m1", "hello")
        self.assertEqual((res.resumed_run_ids, res.started_run_ids), ([], []))


class DeliveryTest(TestCase):
    def test_transient_error_retries_and_preserves_order(self):
        env = Env()
        env.ig.fail_next(TransientError("rate limited"))
        env.comment("c1", "pricing")
        env.worker.tick()
        self.assertEqual(env.ig.sent, [])  # reply failed, DM held back behind it
        env.clock.t += 5
        env.worker.tick()
        self.assertEqual(env.kinds(), ["comment_reply", "private_reply"])

    def test_permanent_error_fails_run(self):
        env = Env()
        env.ig.fail_next(PermanentError("comment deleted"))
        env.comment("c1", "pricing")
        env.worker.tick()
        run = env.runs()[0]
        self.assertEqual(run["status"], "failed")
        self.assertIn("comment deleted", run["error"])
        self.assertEqual(env.ig.sent, [])
        self.assertIsNone(env.store.find_wait("acct_1", "instagram", run["contact_id"], "dm"))

    def test_retries_exhausted_dead_letters(self):
        env = Env()
        env.ig.fail_next(*[TransientError("503")] * 5)
        env.comment("c1", "pricing")
        for _ in range(6):
            env.worker.tick()
            env.clock.t += 60
        self.assertEqual(env.runs()[0]["status"], "failed")

    def test_old_comment_window_closed(self):
        env = Env()
        env.comment("c1", "pricing", t=env.clock() - 8 * DAY)  # webhook delivered very late
        env.worker.tick()
        self.assertEqual(env.kinds(), ["comment_reply"])
        self.assertEqual(env.runs()[0]["status"], "failed")

    def test_private_reply_used_once_per_comment(self):
        d = copy.deepcopy(PRICING_AUTOMATION)
        d["steps"]["s2"]["next"] = "s2b"
        d["steps"]["s2b"] = {"type": "send_message", "text": "second DM", "next": "s3"}
        env = Env(d)
        env.comment("c1", "pricing")
        env.worker.tick()
        # Second DM has no window: private reply already spent, user hasn't replied.
        self.assertEqual(env.kinds(), ["comment_reply", "private_reply"])
        self.assertEqual(env.runs()[0]["status"], "failed")

    def test_rate_limiter_defers_but_does_not_drop(self):
        env = Env()
        env.worker.limiter.burst = 1
        env.worker.limiter.rate = 1.0
        env.comment("c1", "pricing")
        env.worker.tick()
        self.assertEqual(len(env.ig.sent), 1)
        env.clock.t += 1
        env.worker.tick()
        self.assertEqual(len(env.ig.sent), 2)


class VersioningTest(TestCase):
    def test_runs_are_pinned_to_their_version(self):
        env = Env()
        env.comment("c1", "pricing", user="900")
        v2 = copy.deepcopy(PRICING_AUTOMATION)
        v2["steps"]["s4"]["text"] = "V2 link: https://example.com/v2"
        env.store.publish_version(env.automation_id, v2, now=env.clock())
        env.dm("m1", "a@b.com", user="900")
        env.worker.tick()
        self.assertIn("example.com/pricing", env.ig.sent[-1]["text"])

        env.comment("c2", "pricing", user="901")
        env.dm("m2", "c@d.com", user="901")
        env.worker.tick()
        self.assertIn("V2 link", env.ig.sent[-1]["text"])


class DefinitionTest(TestCase):
    def test_example_is_valid(self):
        d = parse_definition(PRICING_AUTOMATION)
        self.assertEqual(d.steps["s3"].config["timeout_s"], 23 * 3600)

    def test_rejects_bad_definitions(self):
        bad = [
            {"steps": {"a": {"type": "send_message", "text": "x"}}},
            {**PRICING_AUTOMATION, "steps": {"s1": {"type": "send_message", "text": "x", "next": "nope"}}},
            {**PRICING_AUTOMATION, "steps": {"s1": {"type": "launch_rocket"}}},
            {**PRICING_AUTOMATION, "steps": {"s1": {"type": "wait_for_reply", "validate": "ssn"}}},
            {**PRICING_AUTOMATION, "steps": {"s1": {"type": "wait_for_reply", "timeout": "soon"}}},
            {**PRICING_AUTOMATION, "trigger": {"type": "comment_created", "match": {"keywords": []}}},
        ]
        for d in bad:
            with self.assertRaises(DefinitionError, msg=d):
                parse_definition(d)

    def test_condition_branching(self):
        d = copy.deepcopy(PRICING_AUTOMATION)
        d["steps"]["s3"]["next"] = "c1"
        d["steps"]["c1"] = {"type": "condition", "if": {"var": "email", "op": "contains", "value": "@acme.com"},
                            "then": "vip", "else": "s4"}
        d["steps"]["vip"] = {"type": "send_message", "text": "Booking you a call!", "next": "end"}
        env = Env(d)
        env.comment("c1", "pricing")
        env.dm("m1", "ceo@acme.com")
        env.worker.tick()
        self.assertEqual(env.ig.sent[-1]["text"], "Booking you a call!")


class IngestionTest(TestCase):
    def test_signature(self):
        body = b'{"a":1}'
        sig = "sha256=" + hmac.new(b"s", body, hashlib.sha256).hexdigest()
        self.assertTrue(verify_signature("s", body, sig))
        self.assertFalse(verify_signature("s", body + b" ", sig))
        self.assertFalse(verify_signature("s", body, None))

    def test_echo_and_unknown_account(self):
        payload = {"object": "instagram", "entry": [
            {"id": IG, "time": 1, "messaging": [{"sender": {"id": IG}, "recipient": {"id": "900"},
                                                  "timestamp": 1000, "message": {"mid": "x", "is_echo": True}}]},
            {"id": "unknown", "time": 1, "changes": [{"field": "comments", "value": {"id": "c9"}}]},
        ]}
        evs = normalize_instagram_webhook(payload, lambda p, i: "acct_1" if i == IG else None)
        self.assertEqual(len(evs), 1)
        self.assertTrue(evs[0].is_from_self)


if __name__ == "__main__":
    unittest.main()
