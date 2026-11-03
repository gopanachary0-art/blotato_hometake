"""End-to-end walkthrough with a fake Instagram client and a controllable clock.

    python demo.py
"""
import hashlib
import hmac
import json

from comment_automations import (AutomationEngine, FakeInstagramClient, Store, Worker,
                                 normalize_instagram_webhook, verify_signature)
from comment_automations.examples import PRICING_AUTOMATION

IG_ACCOUNT = "17841400000000001"
APP_SECRET = "demo-secret"


class Clock:
    def __init__(self, t: float) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def webhook(store, engine, payload):
    body = json.dumps(payload).encode()
    sig = "sha256=" + hmac.new(APP_SECRET.encode(), body, hashlib.sha256).hexdigest()
    assert verify_signature(APP_SECRET, body, sig)
    return [engine.handle_event(e) for e in normalize_instagram_webhook(json.loads(body),
                                                                         store.account_for_platform_id)]


def comment(cid, text, user_id="900", username="jane", media="m1", t=0.0):
    return {"object": "instagram", "entry": [{"id": IG_ACCOUNT, "time": t, "changes": [{
        "field": "comments",
        "value": {"id": cid, "text": text, "from": {"id": user_id, "username": username}, "media": {"id": media}},
    }]}]}


def dm(mid, text, user_id="900", t=0.0):
    return {"object": "instagram", "entry": [{"id": IG_ACCOUNT, "time": t, "messaging": [{
        "sender": {"id": user_id}, "recipient": {"id": IG_ACCOUNT}, "timestamp": int(t * 1000),
        "message": {"mid": mid, "text": text},
    }]}]}


def main():
    clock = Clock(1_800_000_000.0)
    store = Store()
    engine = AutomationEngine(store, clock)
    ig = FakeInstagramClient()
    worker = Worker(store, engine, {"instagram": ig}, clock=clock)

    store.register_account("acct_1", "instagram", IG_ACCOUNT, "token")
    automation_id = store.create_automation("acct_1", PRICING_AUTOMATION, name="Pricing DM", now=clock())

    print("1. Jane comments 'What's the PRICING? 😍'")
    webhook(store, engine, comment("c1", "What's the PRICING? 😍", t=clock()))
    print("   (Meta retries the same webhook)", webhook(store, engine, comment("c1", "x", t=clock()))[0].duplicate)
    worker.tick()

    clock.t += 60
    print("2. Jane replies 'umm why?'")
    webhook(store, engine, dm("mid1", "umm why?", t=clock()))
    worker.tick()

    clock.t += 60
    print("3. Jane replies 'ok fine: Jane.Doe@Example.com'")
    webhook(store, engine, dm("mid2", "ok fine: Jane.Doe@Example.com", t=clock()))
    worker.tick()

    print("\nMessages sent to Instagram:")
    for s in ig.sent:
        print(f"  [{s['kind']:<13}] {s['text']}")

    run = store.runs_for_automation(automation_id)[0]
    print(f"\nRun {run['id']} -> {run['status']}")
    for e in store.run_timeline(run["id"]):
        print(f"  {e['step_id'] or '-':<3} {e['kind']:<15} {json.dumps(e['detail'])[:80]}")
    contact = store.get_contact(run["contact_id"])
    print("\nContact:", contact["username"], contact["attributes"], contact["tags"])


if __name__ == "__main__":
    main()
