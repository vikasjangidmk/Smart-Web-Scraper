"""
main.py - Lead Scraper Pipeline (IndiaMART Only)
=================================================
Three-Stage Zero-Hallucination Pipeline:

  Playwright Crawler (IndiaMART only, multi-keyword × multi-city)
       ↓
  HTML Cleaning
       ↓
  LLM Stage 1: Raw Extraction (values ONLY visible in HTML)
       ↓
  Python Stage 2: Validation & Normalization (data_cleaner.py)
       ↓
  LLM Stage 3: Schema Formatting (structural mapping only)
       ↓
  Final JSON Output (Output/leads.json)

Target: 25+ valid records with zero hallucination.

Run: python main.py
"""
import sys
import os
import re
import yaml  # type: ignore
import requests  # type: ignore
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv  # type: ignore

# ─── Path setup ───────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

load_dotenv(dotenv_path=BASE_DIR / ".env")

from utils.logger import get_logger  # type: ignore
from crawler.indiamart import crawl_indiamart  # type: ignore
from parser.llm_extractor import extract_raw_data, format_schema  # type: ignore
from validator.data_cleaner import validate_and_normalize, extract_phones_from_text, extract_gst_from_text, clean_html  # type: ignore
from storage.save_json import (  # type: ignore
    save_leads,
    save_checkpoint,
    load_checkpoint,
    append_lead,
    build_verification_summary,
    compute_completeness_score,
)

logger = get_logger(__name__)
CONFIG_PATH = BASE_DIR / "config" / "settings.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def _pre_extract_from_html(html_text: str) -> dict:
    """
    Python-side intelligence: scan raw HTML for phones/GST BEFORE the LLM.
    This is the fallback guarantee — what regex finds, the LLM can't lose.
    """
    phones = extract_phones_from_text(html_text)
    gsts = extract_gst_from_text(html_text)
    return {"phones_pre": phones, "gsts_pre": gsts}


def _quality_gate(validated: dict) -> tuple[bool, str]:
    """
    Quality gate — accept records with at minimum:
    - A company name
    - At least ONE of: phone, email, or GST
    - Must be in Gujarat state (or state unknown but city is in Gujarat)
    """
    if not validated.get("company_name"):
        return False, "missing_company_name"

    has_contact = (
        validated.get("phone_numbers") or
        validated.get("emails") or
        validated.get("gst_numbers")
    )
    if not has_contact:
        return False, "no_contact_info"

    if not validated.get("_is_valid_state"):
        return False, "not_gujarat_state"

    return True, "ok"


def run_pipeline():
    """Main orchestration — IndiaMART only, multi-keyword multi-city."""
    logger.info("=" * 60)
    logger.info("🚀 Smart Lead Scraper — IndiaMART Pipeline")
    logger.info(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    config = load_config()
    scraper_cfg = config.get("scraper", {})
    llm_cfg = config.get("llm", {})

    target_records = scraper_cfg.get("target_records", 25)
    crawl_budget = scraper_cfg.get("max_companies", 120)
    max_html_chars = llm_cfg.get("html_truncate_chars", 18000)

    output_file = scraper_cfg.get("output_file", "leads.json")
    output_path = BASE_DIR / "Output" / output_file
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Clear old output file for fresh run
    if output_path.exists():
        output_path.unlink()
        logger.info(f"[main] Cleared old output file: {output_path}")

    # Clear old checkpoint for fresh run 
    checkpoint_path = BASE_DIR / "checkpoint.json"
    if checkpoint_path.exists():
        checkpoint_path.unlink()
        logger.info("[main] Cleared old checkpoint for fresh run")

    # ── Phase 1: Crawl IndiaMART ──────────────────────────────────────────
    logger.info(f"\n📦 Phase 1: Crawling IndiaMART (multi-keyword × multi-city)")

    all_raw_pages: list[tuple[str, str, str]] = []
    try:
        im_pages = crawl_indiamart(
            max_companies=crawl_budget,
            visited_urls=[],
        )
        logger.info(f"[main] IndiaMART: Collected {len(im_pages)} company pages")
        for url, html in im_pages:
            all_raw_pages.append((url, html, "IndiaMART"))
    except Exception as e:
        logger.error(f"[main] IndiaMART crawl failed: {e}", exc_info=True)

    logger.info(f"\n✅ Total pages collected: {len(all_raw_pages)}")

    if not all_raw_pages:
        logger.error("[main] No pages collected. Check internet/VPN.")
        sys.exit(1)

    # ── Phase 2: Three-Stage Extraction Pipeline ──────────────────────────
    logger.info("\n🧠 Phase 2: Three-Stage Extraction Pipeline")
    logger.info("   Stage 1 → LLM Raw Extraction")
    logger.info("   Stage 2 → Python Validation & Normalization")
    logger.info("   Stage 3 → LLM Schema Formatting")

    records: list[dict] = []
    seen_companies: set[str] = set()
    entity_counter = 1
    dropped_stats: dict[str, int] = {}

    for page_idx, (url, html, source_platform) in enumerate(all_raw_pages):
        if len(records) >= target_records:
            logger.info(f"[main] 🎯 Target of {target_records} records reached!")
            break

        logger.info(f"\n[{page_idx + 1}/{len(all_raw_pages)}] Processing: {url}")

        # ── STAGE 0: HTML Cleaning ─────────────────────────────────────────
        cleaned_html = clean_html(html)
        if len(cleaned_html) < 100:
            logger.warning(f"[main] HTML too sparse for: {url}")
            dropped_stats["html_too_sparse"] = dropped_stats.get("html_too_sparse", 0) + 1
            continue

        # ── PRE-EXTRACTION: Python regex scan on raw HTML ──────────────────
        pre_extracted = _pre_extract_from_html(cleaned_html)

        # ── STAGE 1: LLM Raw Extraction ───────────────────────────────────
        raw = extract_raw_data(cleaned_html, url, max_chars=max_html_chars)
        if not raw:
            logger.warning(f"[main] Stage 1 LLM returned nothing for: {url}")
            dropped_stats["stage1_llm_fail"] = dropped_stats.get("stage1_llm_fail", 0) + 1
            continue

        # Augment LLM output with Python pre-extracted values
        existing_phones = raw.get("phone_numbers") or []
        if isinstance(existing_phones, str):
            existing_phones = [existing_phones]
        combined_phones = list(set(existing_phones) | set(pre_extracted["phones_pre"]))
        raw["phone_numbers"] = combined_phones

        existing_gsts = raw.get("gst_numbers") or []
        if isinstance(existing_gsts, str):
            existing_gsts = [existing_gsts]
        combined_gsts = list(set(existing_gsts) | set(pre_extracted["gsts_pre"]))
        raw["gst_numbers"] = combined_gsts

        raw["source_platform"] = source_platform

        # ── STAGE 2: Python Validation & Normalization ─────────────────────
        validated = validate_and_normalize(raw, html_text=cleaned_html)

        # Quality Gate
        passed, reason = _quality_gate(validated)
        if not passed:
            logger.warning(f"[main] Entity dropped [{reason}]: {url}")
            dropped_stats[reason] = dropped_stats.get(reason, 0) + 1
            continue

        # Global Deduplication — normalise company name
        comp_norm = re.sub(r'[^a-z0-9]', '', validated["company_name"].lower())
        if comp_norm in seen_companies:
            logger.info(f"[main] Deduplicating {validated['company_name']} (already seen)")
            dropped_stats["duplicate"] = dropped_stats.get("duplicate", 0) + 1
            continue
        seen_companies.add(comp_norm)

        # ── STAGE 3: LLM Schema Formatting ────────────────────────────────
        record = format_schema(validated)
        if not record:
            logger.warning(f"[main] Stage 3 LLM formatting failed for: {url}")
            dropped_stats["stage3_llm_fail"] = dropped_stats.get("stage3_llm_fail", 0) + 1
            continue

        # ── POST-PROCESSING ────────────────────────────────────────────────
        if "market_presence" not in record:
            record["market_presence"] = {}
        if "ratings_reviews" not in record["market_presence"]:
            record["market_presence"]["ratings_reviews"] = {}
        record["market_presence"]["ratings_reviews"]["platform"] = source_platform

        record["verification_summary"] = build_verification_summary(record)
        record["entity_number"] = entity_counter

        # Log quality
        verification = record.get("verification_summary", {})
        score = verification.get("data_completeness_score", 0)
        confidence = verification.get("confidence_level", "Low")
        gst_val = (record.get("business_credentials") or {}).get("gst_number", "—")
        phones_val = (record.get("contact_info") or {}).get("phone", [])

        logger.info(
            f"   ✅ #{entity_counter} {record.get('company_name')} | "
            f"Score: {score}% | Confidence: {confidence} | "
            f"GST: {gst_val} | Phones: {len(phones_val)}"
        )

        records.append(record)
        current_count: int = entity_counter
        entity_counter = current_count + 1

        # Progressive save
        append_lead(record, str(output_path))

    # ── Phase 3: Final Save ───────────────────────────────────────────────
    logger.info(f"\n💾 Phase 3: Final save — {len(records)} records → {output_path}")
    save_leads(records, str(output_path))

    # ── Summary Report ────────────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("🎉 Pipeline Complete!")
    logger.info(f"   Pages crawled:    {len(all_raw_pages)}")
    logger.info(f"   Records saved:    {len(records)}")
    logger.info(f"   Output file:      {output_path}")

    if records:
        avg_score = sum(compute_completeness_score(r) for r in records) / len(records)
        high_conf = sum(1 for r in records if r.get("verification_summary", {}).get("confidence_level") == "High")
        med_conf = sum(1 for r in records if r.get("verification_summary", {}).get("confidence_level") == "Medium")
        with_gst = sum(1 for r in records if (r.get("business_credentials") or {}).get("gst_number"))
        with_phone = sum(1 for r in records if (r.get("contact_info") or {}).get("phone"))
        with_email = sum(1 for r in records if (r.get("contact_info") or {}).get("email"))
        with_products = sum(1 for r in records if (r.get("products_services") or {}).get("primary_offerings"))

        logger.info(f"\n   📊 Data Quality Summary:")
        logger.info(f"   Avg completeness:  {avg_score:.0f}%")
        logger.info(f"   High confidence:   {high_conf}/{len(records)}")
        logger.info(f"   Medium confidence: {med_conf}/{len(records)}")
        logger.info(f"   With GST:          {with_gst}/{len(records)}")
        logger.info(f"   With phone:        {with_phone}/{len(records)}")
        logger.info(f"   With email:        {with_email}/{len(records)}")
        logger.info(f"   With products:     {with_products}/{len(records)}")

    if dropped_stats:
        logger.info(f"\n   ⚠️  Drop Reasons:")
        for reason, count in sorted(dropped_stats.items(), key=lambda x: -x[1]):
            logger.info(f"   └─ {reason}: {count}")

    if len(records) >= target_records:
        logger.info(f"\n   ✅ Target of {target_records} records: MET ({len(records)} collected)")
    else:
        logger.warning(
            f"\n   ⚠️  Only {len(records)}/{target_records} records collected. "
            f"Try adding more search_queries in settings.yaml."
        )

    logger.info("=" * 60)
    return records


if __name__ == "__main__":
    run_pipeline()
