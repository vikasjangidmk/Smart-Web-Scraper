"""
llm_extractor.py - Optimized Two-Stage LLM Extraction Pipeline
================================================================
Optimizations:
  1. Regex-first phone/email/GST extraction BEFORE LLM
  2. Skip LLM if no packaging keywords found
  3. Reduced max_tokens (800 extract, 512 format)
  4. Aggressive HTML truncation (6000 chars)
  5. Parallel batch support via ThreadPoolExecutor
"""
import os
import json
import re
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests  # type: ignore
from dotenv import load_dotenv  # type: ignore
from bs4 import BeautifulSoup

from utils.logger import get_logger  # type: ignore
from utils.retry import retry_with_backoff  # type: ignore

load_dotenv()
logger = get_logger(__name__)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY")
API_BASE = "https://openrouter.ai/api/v1/chat/completions"
MODEL_ID = "deepseek/deepseek-chat-v3-0324"


# ─── HTML CLEANING ────────────────────────────────────────────────────────────
def clean_html(html: str) -> str:
    """Remove noise tags and return clean text."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "head",
                     "svg", "noscript", "iframe", "form", "button",
                     "meta", "link", "img", "picture"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines)


# ─── REGEX EXTRACTORS (run BEFORE LLM for speed) ─────────────────────────────
PHONE_REGEX = re.compile(r"(?:\+91[\s\-]?)?(?:0)?[6-9]\d{9}")
GST_REGEX = re.compile(r"\b[0-3][0-9][A-Z]{5}[0-9]{4}[A-Z][A-Z0-9]Z[A-Z0-9]\b")
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


def regex_extract(text: str) -> dict:
    """Fast regex extraction of phones, GST, emails from text."""
    text_upper = text.upper() if text else ""
    phones = list(set(PHONE_REGEX.findall(text or "")))
    gsts = list(set(GST_REGEX.findall(text_upper)))
    emails = list(set(EMAIL_REGEX.findall(text or "")))
    return {"phones": phones[:10], "gsts": gsts[:5], "emails": emails[:5]}  # type: ignore


# ─── PACKAGING KEYWORD CHECK ─────────────────────────────────────────────────
PACKAGING_KEYWORDS = [
    "bubble", "packaging", "packing", "wrap", "roll", "sheet",
    "corrugated", "stretch", "foam", "cushion", "poly",
    "shrink", "lamination", "carton", "box", "tape",
    "ldpe", "hdpe", "bopp", "thermocol", "epe",
]


def has_packaging_content(text: str) -> bool:
    """Check if text is related to packaging/bubble products."""
    if not text:
        return False
    text_lower = text.lower()
    return any(kw in text_lower for kw in PACKAGING_KEYWORDS)


# ─── STAGE 1 PROMPT ──────────────────────────────────────────────────────────
STAGE_1_SYSTEM = """\
You are a precise data extraction engine for Indian B2B business directories.

Your ONLY job is to extract raw values that are EXPLICITLY VISIBLE in the text.

STRICT RULES:
1. NEVER invent, guess, or infer any value
2. NEVER use placeholder text like "N/A", "Unknown", "Contact Us"
3. For missing data: use [] for arrays, "" for strings, null for numbers
4. For phone numbers: extract ALL number patterns you see (10+ digits)
5. For GST: look for 15-character alphanumeric codes like "24ABCDE1234F1Z5"
6. For emails: look for @ patterns
7. Return ONLY a JSON object — no markdown, no explanation

OUTPUT SCHEMA (return exactly this):
{
  "company_name": "",
  "phone_numbers": [],
  "emails": [],
  "gst_numbers": [],
  "address_text": "",
  "products": [],
  "contact_person": "",
  "website": "",
  "business_type": "",
  "established_year": null,
  "annual_turnover": "",
  "employee_count": "",
  "certifications": [],
  "designation": "",
  "min_order_quantity": "",
  "rating": null,
  "review_count": null
}"""


def _call_llm(system_prompt: str, user_prompt: str, max_tokens: int = 800) -> Optional[str]:
    """Central LLM call with reduced token budget."""
    if not OPENROUTER_API_KEY:
        logger.error("[llm] OPENROUTER_API_KEY not set")
        return None

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://smartleadscraper.local",
        "X-Title": "Smart Lead Scraper"
    }
    payload = {
        "model": MODEL_ID,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"}
    }

    resp = requests.post(API_BASE, headers=headers, json=payload, timeout=30)

    if resp.status_code == 429:
        logger.warning("[llm] Rate limited (429). Will retry.")
        raise requests.RequestException("Rate limited")

    if resp.status_code != 200:
        logger.error(f"[llm] API error {resp.status_code}: {resp.text[:200]}")
        return None

    content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
    return content.strip() if content else None


def _parse_json_safe(content: str) -> Optional[dict]:
    """Robustly parse JSON from LLM output."""
    if not content:
        return None
    content = re.sub(r"^```(?:json)?\s*", "", content.strip())
    content = re.sub(r"\s*```$", "", content.strip())
    content = content.strip()

    open_braces = content.count("{") - content.count("}")
    if open_braces > 0:
        content += "}" * open_braces

    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except Exception:
                pass
        logger.error(f"[llm] JSON parse failed: {e}")
        return None


# ─── STAGE 1: RAW EXTRACTION ─────────────────────────────────────────────────
@retry_with_backoff(max_retries=1, base_delay=1.0, exceptions=(requests.RequestException, ValueError))
def extract_raw_data(html_text: str, source_url: str, max_chars: int = 6000) -> Optional[dict]:
    """
    STAGE 1: Extract raw values from cleaned HTML using LLM.
    Now with regex pre-extraction and packaging keyword check.
    """
    # Skip if no packaging-related content
    if not has_packaging_content(html_text):
        logger.info(f"[llm-s1] Skipping non-packaging page: {source_url}")
        return None

    truncated = str(html_text)[:max_chars]  # type: ignore

    user_prompt = (
        f"Extract business information from this page text.\n"
        f"Source URL: {source_url}\n\n"
        f"PAGE TEXT:\n{truncated}"
    )

    content = _call_llm(STAGE_1_SYSTEM, user_prompt, max_tokens=800)
    if not content:
        return None

    data = _parse_json_safe(content)
    if not data:
        return None

    data["source_url"] = source_url
    logger.debug(f"[llm-s1] Extracted: {data.get('company_name', 'Unknown')}")
    return data


# ─── STAGE 2: SCHEMA FORMATTING ──────────────────────────────────────────────
STAGE_2_SYSTEM = """\
You receive cleaned, validated business data from a Python validation pipeline.
Your job is to map this data into the exact final JSON schema.

ABSOLUTE RULES:
1. Map values exactly as given — do NOT change any values
2. Do NOT add, invent, or guess any field that isn't in the input
3. For missing fields: use null for objects/numbers, "" for strings, [] for arrays
4. Return ONLY valid JSON — no comments, no markdown

FINAL SCHEMA (map to exactly this):
{
  "company_name": "",
  "business_type": "",
  "contact_info": {
    "phone": [],
    "email": [],
    "website": ""
  },
  "business_credentials": {
    "gst_number": "",
    "registration_type": "",
    "established_year": null,
    "certifications": []
  },
  "address": {
    "full_address": "",
    "locality": "",
    "city": "",
    "state": "",
    "pin_code": ""
  },
  "products_services": {
    "primary_offerings": [],
    "min_order_quantity": "",
    "export_capability": null
  },
  "key_personnel": {
    "contact_person": "",
    "designation": ""
  },
  "business_details": {
    "annual_turnover": "",
    "employee_count": "",
    "established_year": null
  },
  "market_presence": {
    "ratings_reviews": {
      "rating": null,
      "review_count": null,
      "platform": ""
    }
  },
  "primary_source_url": ""
}"""


@retry_with_backoff(max_retries=1, base_delay=1.0, exceptions=(requests.RequestException, ValueError))
def format_schema(validated_data: dict) -> Optional[dict]:
    """STAGE 2: Map validated data into final JSON schema. Reduced tokens."""
    clean_input = {k: v for k, v in validated_data.items() if not k.startswith("_")}

    user_prompt = (
        f"Map the following validated business data to the required schema.\n\n"
        f"VALIDATED INPUT:\n{json.dumps(clean_input, indent=2, ensure_ascii=False)}\n\n"
        f"Return EXACT final schema JSON only."
    )

    content = _call_llm(STAGE_2_SYSTEM, user_prompt, max_tokens=512)
    if not content:
        return None

    data = _parse_json_safe(content)
    if not data:
        return None

    # Always override source URL from Python side
    data["primary_source_url"] = validated_data.get("source_url", "")

    logger.debug(f"[llm-s2] Formatted: {data.get('company_name', 'Unknown')}")
    return data


# ─── BATCH EXTRACTION (Parallel LLM) ─────────────────────────────────────────
def extract_batch(
    pages: list[tuple[str, str, str]],
    max_chars: int = 6000,
    max_workers: int = 3,
) -> list[tuple[str, dict, str]]:
    """
    Extract raw data from multiple pages in parallel batches.
    
    Args:
        pages: List of (url, html, source_platform) tuples
        max_chars: HTML truncation limit
        max_workers: Number of parallel LLM calls
    
    Returns:
        List of (url, extracted_data, source_platform) tuples
    """
    results = []

    def _extract_one(item: tuple[str, str, str]) -> Optional[tuple[str, dict, str]]:
        url, html, platform = item
        try:
            from validator.data_cleaner import clean_html as dc_clean_html  # type: ignore
            cleaned = dc_clean_html(html)
            if len(cleaned) < 100:
                return None
            raw = extract_raw_data(cleaned, url, max_chars=max_chars)
            if raw:
                raw["source_platform"] = platform
                return (url, raw, platform)
        except Exception as e:
            logger.warning(f"[llm-batch] Failed {url}: {e}")
        return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_extract_one, page): page for page in pages}  # type: ignore
        for future in as_completed(futures):
            try:
                result = future.result(timeout=60)
                if result:
                    results.append(result)
            except Exception as e:
                logger.warning(f"[llm-batch] Batch item failed: {e}")

    return results
