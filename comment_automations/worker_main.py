"""python -m comment_automations.worker_main  (uses the real Instagram client)."""
import logging
import os

from .engine import AutomationEngine
from .platform import InstagramGraphClient
from .store import Store
from .worker import Worker

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    store = Store(os.environ.get("AUTOMATIONS_DB", "automations.db"))
    Worker(store, AutomationEngine(store), {"instagram": InstagramGraphClient()}).run_forever()
