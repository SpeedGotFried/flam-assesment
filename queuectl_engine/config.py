"""
Configuration management module for QueueCTL.
"""

from typing import Dict, Any, Optional
from queuectl_engine.db import Database

VALID_CONFIG_KEYS = {
    "max-retries": {"type": int, "default": "3", "min": 0},
    "backoff-base": {"type": float, "default": "2", "min": 1.0},
    "stale-timeout": {"type": int, "default": "30", "min": 1},
}


class ConfigManager:
    def __init__(self, db: Database):
        self.db = db

    def get(self, key: str) -> str:
        if key in VALID_CONFIG_KEYS:
            default = VALID_CONFIG_KEYS[key]["default"]
            return self.db.config_get(key, default)
        return self.db.config_get(key, "")

    def set(self, key: str, value: str):
        if key in VALID_CONFIG_KEYS:
            spec = VALID_CONFIG_KEYS[key]
            try:
                val = spec["type"](value)
                if "min" in spec and val < spec["min"]:
                    raise ValueError(f"Value for {key} must be >= {spec['min']}")
            except ValueError as e:
                raise ValueError(f"Invalid value '{value}' for key '{key}': {str(e)}")

        self.db.config_set(key, value)

    def get_all(self) -> Dict[str, str]:
        configs = {}
        for key in VALID_CONFIG_KEYS:
            configs[key] = self.get(key)
        return configs
