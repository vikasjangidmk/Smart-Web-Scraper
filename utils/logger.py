"""
logger.py - Centralized logging (Optimized)
Console: INFO, File: WARNING (reduced verbosity for speed)
"""
import logging
import sys
import yaml  # type: ignore
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
    """Returns a named logger. Console=INFO, File=WARNING."""
    log_cfg = _load_log_config()
    console_level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    file_level_str = log_cfg.get("file_level", "WARNING").upper()
    file_level = getattr(logging, file_level_str, logging.WARNING)
    fmt = log_cfg.get("format", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    log_file = log_cfg.get("file", "scraper.log")

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(min(console_level, file_level))
    formatter = logging.Formatter(fmt)

    # Console handler - shows INFO+
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(console_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler - shows WARNING+ only (reduced verbosity)
    try:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(file_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception:
        pass

    return logger
