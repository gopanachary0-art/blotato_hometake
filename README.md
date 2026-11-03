# Comment-triggered automations (reference backend)

Python 3.10+, standard library only. FastAPI is optional and only needed for `app.py`.

```bash
python demo.py                               # end-to-end walkthrough with a fake Instagram client
python -m unittest discover -s tests -v      # 26 tests
pip install fastapi uvicorn && uvicorn comment_automations.app:app   # webhook API
python -m comment_automations.worker_main    # worker (timers + outbox) with the real Graph client
```

## Layout

| Module | Role in the design |
|---|---|
| `models.py` | `InboundEvent`, the platform-agnostic event type; run statuses |
| `definition.py` | Trigger + typed-step graph; validated at publish time (dangling refs, unknown types, durations) |
| `matcher.py` | Keyword matching (case/emoji/punctuation-insensitive, word boundaries) |
| `store.py` | Schema + persistence (SQLite here, maps 1:1 to Postgres) |
| `engine.py` | Event router (resume waiting run, else match triggers) + step interpreter, waits, timeouts |
| `worker.py` | Fires timers; drains the outbox with per-account rate limits, retries/backoff, dead-lettering |
| `platform.py` | Instagram Graph client, Meta error classification, fake client for tests |
| `ingestion.py` | `X-Hub-Signature-256` verification, webhook to `InboundEvent` normalisation |
| `app.py` | FastAPI: webhook verify/receive, create/publish automations, run timelines |
| `examples.py` | The "pricing" automation from the brief |

## Key guarantees

- **Duplicate webhooks**: deduped on the platform event ID.
- **Exactly-once side effects**: state transitions and outbox rows commit in one transaction, and every action has a deterministic idempotency key.
- **Per-run ordering**: a DM is never sent while an earlier step for that run is still retrying.
- **Runs pinned to a version**: editing an automation never breaks runs in progress.
- **Re-entry policy**: `skip_if_active` / `restart` / `allow_parallel`, enforced with a partial unique index.
- **Instagram messaging rules**: the first DM is a private reply (one per comment, within 7 days). Later DMs need the 24h window opened by the user's reply. A violation fails the run with a clear reason.
- **Loop protection**: the account's own comments and echoes are ignored.

## Production swaps

- **Event bus**: SQLite → Postgres, using `FOR UPDATE SKIP LOCKED` for timers and the outbox. Ingestion publishes to Kafka or SQS, partitioned by `account_id`, instead of calling the engine inline.
- **Storage and secrets**: tokens are encrypted, and the rate-limiter state moves to Redis.
