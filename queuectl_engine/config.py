"""
Minimal configuration manager for QueueCTL.
"""
from typing import Dict
from queuectl_engine.db import Database

DEFAULTS = {
    "max-retries": "3",
    "backoff-base": "2",
    "stale-timeout": "30",
}


class ConfigManager:
    def __init__(self, db: Database):
        self.db = db

    def get(self, key: str) -> str:
        return self.db.config_get(key, DEFAULTS.get(key, ""))

    def set(self, key: str, value: str):
        self.db.config_set(key, str(value))

    def get_all(self) -> Dict[str, str]:
        return {k: self.get(k) for k in DEFAULTS}
