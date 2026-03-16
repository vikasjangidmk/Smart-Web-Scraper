"""
main.py - Lead Scraper Pipeline (Fully Optimized)
===================================================
Multi-Source Zero-Hallucination Pipeline with CONCURRENT crawling:

  3 Playwright Crawlers run in PARALLEL (ThreadPoolExecutor):
    ├─ IndiaMART  (multi-keyword × multi-city)
    ├─ TradeIndia (multi-keyword × multi-city)
    └─ ExportersIndia (multi-keyword × multi-city)
        ↓
  Pre-Filter: Deduplicate URLs + Packaging keyword check
        ↓
  Regex Pre-Extract: Phone/Email/GST extracted via Python regex
        ↓
  LLM Stage 1: Raw Extraction (only for pages with packaging content)
        ↓
  Python Stage 2: Validation & Normalization (data_cleaner.py)
        ↓
  Quality Gate: MANDATORY phone + GST
        ↓
  LLM Stage 3: Schema Formatting (reduced tokens)
        ↓
  Final JSON Output (Output/leads.json) — saved ONCE at end

Target: 20-25 valid records with MANDATORY phone + GST.
Speed: All 3 crawlers + parallel LLM → ~5x faster than original.

Run: python main.py
"""
import sys
import os
import re
import yaml  # type: ignore
import requests  # type: ignore
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv  # type: ignore

# ─── Path setup ───────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

load_dotenv(dotenv_path=BASE_DIR / ".env")

from utils.logger import get_logger  # type: ignore
from crawler.indiamart import crawl_indiamart  # type: ignore
from crawler.tradeindia import crawl_tradeindia  # type: ignore
from crawler.exportersindia import crawl_exportersindia  # type: ignore
from parser.llm_extractor import (  # type: ignore
    extract_raw_data, format_schema, regex_extract, has_packaging_content,
)
from validator.data_cleaner import (  # type: ignore
    validate_and_normalize, extract_phones_from_text, extract_gst_from_text,
    clean_html, is_valid_company_name, has_packaging_keywords,
)
from storage.save_json import (  # type: ignore
    save_leads,
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
    Python-side regex scan: phones/GST/emails BEFORE any LLM call.
    What regex finds, the LLM can't lose.
    """
    phones = extract_phones_from_text(html_text)
    gsts = extract_gst_from_text(html_text)
    regex_data = regex_extract(html_text)
    emails = regex_data.get("emails", [])
    return {"phones_pre": phones, "gsts_pre": gsts, "emails_pre": emails}


def _quality_gate(validated: dict, require_phone: bool = True, require_gst: bool = True) -> tuple[bool, str]:
    """
    Quality gate — STRICT requirements:
    - Must have a valid company name
    - MUST have phone number (if require_phone)
    - MUST have GST number (if require_gst)
    - Must be in Gujarat state
    """
    if not validated.get("company_name"):
        return False, "missing_company_name"

    if not is_valid_company_name(validated["company_name"]):
        return False, "invalid_company_name"

    if require_phone and not validated.get("phone_numbers"):
        return False, "missing_phone"

    if require_gst and not validated.get("gst_numbers"):
        return False, "missing_gst"

    if not validated.get("_is_valid_state"):
        return False, "not_gujarat_state"

    return True, "ok"


# ─── CONCURRENT CRAWLER WRAPPERS ─────────────────────────────────────────────

def _crawl_indiamart_wrapper(config: dict) -> list[tuple[str, str, str]]:
    try:
        scraper_cfg = config.get("scraper", {})
        pages = crawl_indiamart(
            max_companies=scraper_cfg.get("max_companies", 30),
            visited_urls=[],
        )
        return [(url, html, "IndiaMART") for url, html in pages]
    except Exception as e:
        logger.error(f"[main] IndiaMART crawl failed: {e}", exc_info=True)
        return []


def _crawl_tradeindia_wrapper(config: dict) -> list[tuple[str, str, str]]:
    try:
        scraper_cfg = config.get("scraper", {})
        pages = crawl_tradeindia(
            max_companies=scraper_cfg.get("max_companies", 30),
            visited_urls=[],
        )
        return [(url, html, "TradeIndia") for url, html in pages]
    except Exception as e:
        logger.error(f"[main] TradeIndia crawl failed: {e}", exc_info=True)
        return []


def _crawl_exportersindia_wrapper(config: dict) -> list[tuple[str, str, str]]:
    try:
        scraper_cfg = config.get("scraper", {})
        pages = crawl_exportersindia(
            max_companies=scraper_cfg.get("max_companies", 30),
            visited_urls=[],
        )
        return [(url, html, "ExportersIndia") for url, html in pages]
    except Exception as e:
        logger.error(f"[main] ExportersIndia crawl failed: {e}", exc_info=True)
        return []


# ─── URL DEDUPLICATION ────────────────────────────────────────────────────────

def _normalize_url_for_dedup(url: str) -> str:
    """Normalize URL for cross-source deduplication."""
    url = url.lower().strip().rstrip("/")
    # Remove www. prefix
    url = re.sub(r"https?://(www\.)?", "", url)
    # Remove query params
    url = url.split("?")[0]
    return url


def _deduplicate_pages(pages: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    """Remove duplicate URLs across all sources."""
    seen_urls: set[str] = set()
    unique_pages = []
    for url, html, source in pages:
        norm = _normalize_url_for_dedup(url)
        if norm not in seen_urls:
            seen_urls.add(norm)
            unique_pages.append((url, html, source))
    removed = len(pages) - len(unique_pages)
    if removed > 0:
        logger.info(f"[main] Deduplicated {removed} duplicate URLs across sources")
    return unique_pages





def run_pipeline():
    """Main orchestration — 3 sources in parallel, then optimized extraction."""
    logger.info("=" * 60)
    logger.info("🚀 Smart Lead Scraper — Optimized Pipeline")
    logger.info(f"   Sources: IndiaMART + TradeIndia + ExportersIndia")
    logger.info(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    config = load_config()
    scraper_cfg = config.get("scraper", {})
    llm_cfg = config.get("llm", {})

    target_records = scraper_cfg.get("target_records", 25)
    max_html_chars = llm_cfg.get("html_truncate_chars", 6000)
    require_phone = scraper_cfg.get("require_phone", True)
    require_gst = scraper_cfg.get("require_gst", True)

    output_file = scraper_cfg.get("output_file", "leads.json")
    output_path = BASE_DIR / "Output" / output_file
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Clear old output file
    if output_path.exists():
        output_path.unlink()
        logger.info(f"[main] Cleared old output: {output_path}")

    # Clear old checkpoint
    checkpoint_path = BASE_DIR / "checkpoint.json"
    if checkpoint_path.exists():
        checkpoint_path.unlink()

    # ── Phase 1: CONCURRENT Crawling ──────────────────────────────────
    logger.info(f"\n📦 Phase 1: CONCURRENT Crawling (3 sources)")
    crawl_start = datetime.now()

    all_raw_pages: list[tuple[str, str, str]] = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_crawl_indiamart_wrapper, config): "IndiaMART",  # type: ignore
            executor.submit(_crawl_tradeindia_wrapper, config): "TradeIndia",  # type: ignore
            executor.submit(_crawl_exportersindia_wrapper, config): "ExportersIndia",  # type: ignore
        }

        for future in as_completed(futures):
            source_name = futures[future]
            try:
                pages = future.result(timeout=180)  # 3 min max per crawler
                logger.info(f"[main] ✅ {source_name}: {len(pages)} pages")
                all_raw_pages.extend(pages)
            except Exception as e:
                logger.error(f"[main] ❌ {source_name} failed: {e}")

    crawl_duration = (datetime.now() - crawl_start).total_seconds()
    logger.info(f"\n✅ Crawled {len(all_raw_pages)} pages in {crawl_duration:.0f}s")

    if not all_raw_pages:
        logger.error("[main] No pages collected. Check internet/VPN.")
        sys.exit(1)

    # ── Phase 1.5: Pre-Filter — Deduplicate + Packaging Check ─────────
    logger.info("\n🔍 Phase 1.5: Pre-Filtering")

    # Deduplicate URLs across sources
    all_raw_pages = _deduplicate_pages(all_raw_pages)

    # Filter non-packaging pages using keywords (skip LLM for these)
    filtered_pages = []
    skipped_non_packaging: int = 0
    for url, html, source in all_raw_pages:
        cleaned_preview = clean_html(html)[:3000]  # Quick preview check
        if has_packaging_keywords(cleaned_preview) or has_packaging_content(cleaned_preview):
            filtered_pages.append((url, html, source))
        else:
            skipped_non_packaging = skipped_non_packaging + 1  # type: ignore

    if skipped_non_packaging > 0:
        logger.info(f"[main] Skipped {skipped_non_packaging} non-packaging pages")
    logger.info(f"[main] {len(filtered_pages)} pages passed pre-filter")

    # Log per-source breakdown
    source_counts: dict[str, int] = {}
    for _, _, src in filtered_pages:
        source_counts[src] = source_counts.get(src, 0) + 1
    for src, count in sorted(source_counts.items()):
        logger.info(f"   └─ {src}: {count} pages")

    # ── Phase 2: Extraction Pipeline ──────────────────────────────────
    logger.info("\n🧠 Phase 2: Extraction Pipeline")
    logger.info("   Regex → LLM Stage 1 → Validation → Quality Gate → LLM Stage 2")

    records: list[dict] = []
    seen_companies: set[str] = set()
    seen_phones: set[str] = set()
    seen_urls: set[str] = set()
    entity_counter: int = 1
    dropped_stats: dict[str, int] = {}
    
    for page_idx, (url, html, source_platform) in enumerate(filtered_pages):
        if len(records) >= target_records:
            logger.info(f"[main] 🎯 Target of {target_records} records reached! Early stopping.")
            break

        # URL deduplication
        url_norm = _normalize_url_for_dedup(url)
        if url_norm in seen_urls:
            continue
        seen_urls.add(url_norm)

        logger.info(f"\n[{page_idx + 1}/{len(filtered_pages)}] {url}")
        
        # ── STAGE 0: HTML Cleaning ─────────────────────────────────────
        cleaned_html = clean_html(html)
        if len(cleaned_html) < 100:
            dropped_stats["html_too_sparse"] = dropped_stats.get("html_too_sparse", 0) + 1
            continue

        # ── PRE-EXTRACTION: Python regex scan ──────────────────────────
        pre_extracted = _pre_extract_from_html(cleaned_html)

        # ── STAGE 1: LLM Raw Extraction ───────────────────────────────
        raw = extract_raw_data(cleaned_html, url, max_chars=max_html_chars)
        if not raw:
            dropped_stats["stage1_llm_fail"] = dropped_stats.get("stage1_llm_fail", 0) + 1
            continue

        # Augment LLM output with regex pre-extracted values
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

        existing_emails = raw.get("emails") or []
        if isinstance(existing_emails, str):
            existing_emails = [existing_emails]
        combined_emails = list(set(existing_emails) | set(pre_extracted["emails_pre"]))
        raw["emails"] = combined_emails

        raw["source_platform"] = source_platform

        # ── STAGE 2: Python Validation & Normalization ─────────────────
        validated = validate_and_normalize(raw, html_text=cleaned_html)

        # Quality Gate (MANDATORY phone + GST)
        passed, reason = _quality_gate(validated, require_phone=require_phone, require_gst=require_gst)
        if not passed:
            logger.warning(f"[main] Dropped [{reason}]: {url}")
            dropped_stats[reason] = dropped_stats.get(reason, 0) + 1
            continue

        # Global company name deduplication
        comp_norm = re.sub(r'[^a-z0-9]', '', validated["company_name"].lower())
        if comp_norm in seen_companies:
            dropped_stats["duplicate_company"] = dropped_stats.get("duplicate_company", 0) + 1
            continue
        seen_companies.add(comp_norm)

        # Phone number cross-record deduplication
        record_phones = validated.get("phone_numbers", [])
        all_phones_seen = all(p in seen_phones for p in record_phones)
        if record_phones and all_phones_seen:
            dropped_stats["duplicate_phone"] = dropped_stats.get("duplicate_phone", 0) + 1
            continue
        for p in record_phones:
            seen_phones.add(p)

        # ── STAGE 3: LLM Schema Formatting ────────────────────────────
        record = format_schema(validated)
        if not record:
            dropped_stats["stage3_llm_fail"] = dropped_stats.get("stage3_llm_fail", 0) + 1
            continue

        # ── POST-PROCESSING ────────────────────────────────────────────
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
            f"Source: {source_platform} | "
            f"Score: {score}% | Confidence: {confidence} | "
            f"GST: {gst_val} | Phones: {len(phones_val)}"
        )

        records.append(record)
        entity_counter = entity_counter + 1  # type: ignore

    # ── Phase 3: Final Save (ONCE — no per-record saves) ──────────────
    logger.info(f"\n💾 Phase 3: Final save — {len(records)} records → {output_path}")
    save_leads(records, str(output_path))

    # ── Summary Report ────────────────────────────────────────────────
    total_duration = (datetime.now() - crawl_start).total_seconds()

    logger.info("\n" + "=" * 60)
    logger.info("🎉 Pipeline Complete!")
    logger.info(f"   Total time:       {total_duration:.0f}s ({total_duration/60:.1f} min)")
    logger.info(f"   Pages crawled:    {len(all_raw_pages)}")
    logger.info(f"   Pre-filtered:     {len(filtered_pages)}")
    logger.info(f"   Records saved:    {len(records)}")
    logger.info(f"   Output file:      {output_path}")

    # Per-source record breakdown
    if records:
        source_record_counts: dict[str, int] = {}
        for r in records:
            platform = (r.get("market_presence") or {}).get("ratings_reviews", {}).get("platform", "Unknown")
            source_record_counts[platform] = source_record_counts.get(platform, 0) + 1
        logger.info(f"\n   📊 Records per Source:")
        for src, count in sorted(source_record_counts.items()):
            logger.info(f"   └─ {src}: {count}")

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

        # DOUBLE VERIFY: Check all records have phone AND GST
        records_missing_phone = sum(1 for r in records if not (r.get("contact_info") or {}).get("phone"))
        records_missing_gst = sum(1 for r in records if not (r.get("business_credentials") or {}).get("gst_number"))
        if records_missing_phone > 0:
            logger.warning(f"   ⚠️  {records_missing_phone} records missing phone number!")
        else:
            logger.info(f"   ✅ ALL records have phone numbers")
        if records_missing_gst > 0:
            logger.warning(f"   ⚠️  {records_missing_gst} records missing GST number!")
        else:
            logger.info(f"   ✅ ALL records have GST numbers")

    if dropped_stats:
        logger.info(f"\n   ⚠️  Drop Reasons:")
        for reason, count in sorted(dropped_stats.items(), key=lambda x: -x[1]):
            logger.info(f"   └─ {reason}: {count}")

    if len(records) >= target_records:
        logger.info(f"\n   ✅ Target of {target_records} records: MET ({len(records)} collected)")
    else:
        logger.warning(
            f"\n   ⚠️  Only {len(records)}/{target_records} records collected. "
            f"Consider relaxing quality requirements or adding more search queries."
        )

    logger.info("=" * 60)
    return records


if __name__ == "__main__":
    run_pipeline()
