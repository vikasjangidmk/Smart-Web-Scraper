"""
llm_extractor.py - Two-Stage LLM Extraction Pipeline
====================================================
Stage 1: Raw extraction — LLM extracts only values visible in HTML.
Stage 2: Schema formatting — LLM maps clean validated data to final schema.

Zero hallucination is enforced by the data_cleaner Python layer between stages.
"""
import os
import json
import re
from typing import Optional
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
    """
    Remove all noise tags and return clean, readable text.
    Preserves business-relevant sections (phone, GST, address patterns).
    """
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")

    # Remove completely useless tags
    for tag in soup(["script", "style", "nav", "footer", "head",
                     "svg", "noscript", "iframe", "form", "button",
                     "meta", "link", "img", "picture"]):
        tag.decompose()

    # Get clean text
    text = soup.get_text(separator="\n", strip=True)

    # Collapse excessive whitespace / blank lines
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines)


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


def _call_llm(system_prompt: str, user_prompt: str, max_tokens: int = 1500) -> Optional[str]:
    """Central LLM call function with robust error handling."""
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

    resp = requests.post(API_BASE, headers=headers, json=payload, timeout=90)

    if resp.status_code == 429:
        logger.warning("[llm] Rate limited (429). Will retry.")
        raise requests.RequestException("Rate limited")

    if resp.status_code != 200:
        logger.error(f"[llm] API error {resp.status_code}: {resp.text[:200]}")
        return None

    content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
    return content.strip() if content else None


def _parse_json_safe(content: str) -> Optional[dict]:
    """Robustly parse a JSON string from LLM output."""
    if not content:
        return None

    # Strip markdown code fences if present
    content = re.sub(r"^```(?:json)?\s*", "", content.strip())
    content = re.sub(r"\s*```$", "", content.strip())
    content = content.strip()

    # Auto-fix unclosed JSON (LLM truncation)
    open_braces = content.count("{") - content.count("}")
    if open_braces > 0:
        content += "}" * open_braces

    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        # Try extracting first JSON object
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except Exception:
                pass
        logger.error(f"[llm] JSON parse failed: {e} | Content snippet: {content[:100]}")
        return None


# ─── STAGE 1: RAW EXTRACTION ─────────────────────────────────────────────────
@retry_with_backoff(max_retries=3, base_delay=2.0, exceptions=(requests.RequestException, ValueError))
def extract_raw_data(html_text: str, source_url: str, max_chars: int = 15000) -> Optional[dict]:
    """
    STAGE 1: Extract raw values from cleaned HTML text using LLM.

    The LLM only returns values it can literally see in the text.
    Python validation (data_cleaner.py) runs AFTER this stage.
    """
    truncated = str(html_text)[:max_chars]

    user_prompt = (
        f"Extract business information from this page text.\n"
        f"Source URL: {source_url}\n\n"
        f"PAGE TEXT:\n{truncated}"
    )

    content = _call_llm(STAGE_1_SYSTEM, user_prompt, max_tokens=1500)
    if not content:
        return None

    data = _parse_json_safe(content)
    if not data:
        return None

    data["source_url"] = source_url
    logger.debug(f"[llm-s1] Extracted: {data.get('company_name', 'Unknown')} | "
                 f"Phones:{len(data.get('phone_numbers', []))} | "
                 f"GST:{len(data.get('gst_numbers', []))}")
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


@retry_with_backoff(max_retries=3, base_delay=2.0, exceptions=(requests.RequestException, ValueError))
def format_schema(validated_data: dict) -> Optional[dict]:
    """
    STAGE 2: Map strictly validated data into the final JSON schema using LLM.

    Input comes from data_cleaner.validate_and_normalize() — all values are
    guaranteed real (not hallucinated). LLM only does structural mapping here.
    """
    # Remove internal flags before sending to LLM
    clean_input = {k: v for k, v in validated_data.items() if not k.startswith("_")}

    user_prompt = (
        f"Map the following validated business data to the required schema.\n\n"
        f"VALIDATED INPUT:\n{json.dumps(clean_input, indent=2, ensure_ascii=False)}\n\n"
        f"Return EXACT final schema JSON only."
    )

    content = _call_llm(STAGE_2_SYSTEM, user_prompt, max_tokens=2048)
    if not content:
        return None

    data = _parse_json_safe(content)
    if not data:
        return None

    # Always override the source URL from Python side (never trust LLM for this)
    data["primary_source_url"] = validated_data.get("source_url", "")

    logger.debug(f"[llm-s2] Formatted: {data.get('company_name', 'Unknown')}")
    return data
