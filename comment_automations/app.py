"""Optional HTTP layer (pip install fastapi uvicorn).

    uvicorn comment_automations.app:app

Env: IG_APP_SECRET, IG_VERIFY_TOKEN, AUTOMATIONS_DB (default automations.db).
Run the Worker in a separate process: python -m comment_automations.worker_main
"""
from __future__ import annotations

import json
import os

from fastapi import FastAPI, HTTPException, Query, Request, Response

from .definition import DefinitionError
from .engine import AutomationEngine
from .ingestion import normalize_instagram_webhook, verify_signature
from .store import Store


def create_app(store: Store, engine: AutomationEngine, app_secret: str, verify_token: str) -> FastAPI:
    app = FastAPI(title="Comment automations")

    @app.get("/webhooks/instagram")
    def verify(mode: str = Query(alias="hub.mode"), token: str = Query(alias="hub.verify_token"),
               challenge: str = Query(alias="hub.challenge")):
        if mode == "subscribe" and token == verify_token:
            return Response(content=challenge, media_type="text/plain")
        raise HTTPException(403)

    @app.post("/webhooks/instagram")
    async def receive(request: Request):
        body = await request.body()
        if not verify_signature(app_secret, body, request.headers.get("X-Hub-Signature-256")):
            raise HTTPException(401, "bad signature")
        events = normalize_instagram_webhook(json.loads(body), store.account_for_platform_id)
        # Production: publish `events` to the bus and return immediately.
        for ev in events:
            engine.handle_event(ev)
        return {"received": len(events)}

    @app.post("/accounts/{account_id}/automations")
    async def create_automation(account_id: str, request: Request):
        try:
            aid = store.create_automation(account_id, await request.json(), now=engine.clock())
        except DefinitionError as e:
            raise HTTPException(422, str(e))
        return {"automation_id": aid}

    @app.put("/automations/{automation_id}")
    async def publish(automation_id: str, request: Request):
        try:
            vid = store.publish_version(automation_id, await request.json(), now=engine.clock())
        except DefinitionError as e:
            raise HTTPException(422, str(e))
        except KeyError:
            raise HTTPException(404)
        return {"version_id": vid}

    @app.get("/automations/{automation_id}/runs")
    def runs(automation_id: str):
        return [{**r, "timeline": store.run_timeline(r["id"])} for r in store.runs_for_automation(automation_id)]

    return app


_store = Store(os.environ.get("AUTOMATIONS_DB", "automations.db"))
app = create_app(_store, AutomationEngine(_store),
                 os.environ.get("IG_APP_SECRET", ""), os.environ.get("IG_VERIFY_TOKEN", ""))
