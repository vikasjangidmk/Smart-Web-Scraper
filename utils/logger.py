
"""
logger.py - Centralized logging configuration
"""
import logging
import sys
import yaml
from pathlib import Path

_CONFIG_PATH = Path(__file__).parent.parent / "config" / "settings.yaml"


def _load_log_config() -> dict:
    try:
        with open(_CONFIG_PATH, "r") as f:
            cfg = yaml.safe_load(f)
        return cfg.get("logging", {})
    except Exception:
        return {"level": "INFO", "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s", "file": "scraper.log"}


def get_logger(name: str) -> logging.Logger:
    """
    Returns a named logger with file + console handlers.
    Call once per module: logger = get_logger(__name__)
    """
    log_cfg = _load_log_config()
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    fmt = log_cfg.get("format", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    log_file = log_cfg.get("file", "scraper.log")

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # Already configured

    logger.setLevel(level)
    formatter = logging.Formatter(fmt)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler
    try:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception:
        pass

    return logger
