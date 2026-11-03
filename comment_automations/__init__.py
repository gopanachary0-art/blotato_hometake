"""Comment-triggered social media automations: reference backend."""
from .definition import DefinitionError, parse_definition
from .engine import AutomationEngine
from .ingestion import normalize_instagram_webhook, verify_signature
from .models import EventType, InboundEvent, RunStatus
from .platform import FakeInstagramClient, InstagramGraphClient
from .store import Store
from .worker import TokenBucketLimiter, Worker

__all__ = [
    "AutomationEngine", "DefinitionError", "EventType", "FakeInstagramClient", "InboundEvent",
    "InstagramGraphClient", "RunStatus", "Store", "TokenBucketLimiter", "Worker",
    "normalize_instagram_webhook", "parse_definition", "verify_signature",
]
