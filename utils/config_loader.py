"""
config_loader.py - Centralised config loader used by all modules
"""
import yaml
from pathlib import Path
from functools import lru_cache

_CONFIG_PATH = Path(__file__).parent.parent / "config" / "settings.yaml"


@lru_cache(maxsize=1)
def load_config() -> dict:
    """Load and cache settings.yaml. Call load_config.cache_clear() to reload."""
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
