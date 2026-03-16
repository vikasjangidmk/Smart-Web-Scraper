"""
save_json.py - Structured JSON storage (Optimized).
Now saves ONCE at end instead of per-record (major I/O optimization).
"""
import json
import os
from datetime import date
from pathlib import Path

from utils.logger import get_logger  # type: ignore

logger = get_logger(__name__)

CHECKPOINT_FILE = "checkpoint.json"


def compute_completeness_score(record: dict) -> int:
    """
    5-field completeness score:
    1. company_name
    2. phone
    3. gst_number
    4. city
    5. primary_offerings
    """
    fields = [
        record.get("company_name"),
        (record.get("contact_info") or {}).get("phone"),
        (record.get("business_credentials") or {}).get("gst_number"),
        (record.get("address") or {}).get("city"),
        (record.get("products_services") or {}).get("primary_offerings")
    ]
    
    filled = sum(1 for f in fields if f and (isinstance(f, list) and len(f) > 0 or not isinstance(f, list)))
    score = int((filled / len(fields)) * 100)
    return score


def _confidence_level(score: int) -> str:
    if score >= 65:
        return "High"
    if score >= 35:
        return "Medium"
    return "Low"


def build_verification_summary(record: dict) -> dict:
    """Calculate and attach the verification block."""
    score = compute_completeness_score(record)
    source = (
        record.get("primary_source_url")
        or record.get("source_url")
        or ""
    )
    return {
        "data_completeness_score": score,
        "confidence_level": _confidence_level(score),
        "last_verified": date.today().isoformat(),
        "sources_used": [source] if source else [],
        "notes": ""
    }


def save_leads(records: list[dict], output_path: str) -> None:
    """Save all records to the output JSON file (called ONCE at end)."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    logger.info(f"[storage] Saved {len(records)} records → {output_path}")


def save_checkpoint(visited_urls: list[str]) -> None:
    """Save visited URLs for resume support."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump({"visited": visited_urls}, f)


def load_checkpoint() -> list[str]:
    """Load previously visited URLs."""
    if not os.path.exists(CHECKPOINT_FILE):
        return []
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("visited", [])
    except Exception:
        return []
