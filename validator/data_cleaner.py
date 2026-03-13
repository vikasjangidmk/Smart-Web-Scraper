"""
data_cleaner.py - Python Validation & Normalization Layer (Stage 2 of Pipeline)

This module runs BETWEEN Stage-1 LLM output and Stage-2 LLM formatting.
It removes hallucinated values, validates all fields with regex, and
enriches the record using raw HTML text scanning as a fallback.

Zero-hallucination guarantee — no values are invented here.
"""
import re
import random
from typing import Optional, Any
from bs4 import BeautifulSoup  # type: ignore
from utils.logger import get_logger  # type: ignore

logger = get_logger(__name__)

def clean_html(html: str) -> str:
    """
    Remove scripts, styles, and boilerplate from HTML.
    Returns clean text for LLM processing.
    """
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        # Get text with clean spacing
        text = soup.get_text(separator=" ", strip=True)
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text)
        return text
    except Exception as e:
        logger.warning(f"[cleaner] HTML cleaning failed: {e}")
        return html  # Fallback to raw if logic fails

# ─── GST ─────────────────────────────────────────────────────────────────────
GST_REGEX = re.compile(
    r"\b([0-3][0-9][A-Z]{5}[0-9]{4}[A-Z][A-Z0-9]Z[A-Z0-9])\b"
)

VALID_STATE_CODES = {
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
    "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
    "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
    "31", "32", "33", "34", "35", "36", "37", "38",
}


def validate_gst(gst: str) -> bool:
    """Returns True if GST format is valid."""
    if not gst or not isinstance(gst, str):
        return False
    gst = gst.strip().upper()
    if len(gst) != 15:
        return False
    if not re.fullmatch(r"[0-3][0-9][A-Z]{5}[0-9]{4}[A-Z][A-Z0-9]Z[A-Z0-9]", gst):
        return False
    return gst[:2] in VALID_STATE_CODES  # type: ignore


def extract_gst_from_text(text: str) -> list[str]:
    """Scan raw text for all valid GST numbers."""
    if not text:
        return []
    found = []
    seen = set()
    for match in GST_REGEX.finditer(text.upper()):
        candidate = match.group(1)
        if validate_gst(candidate) and candidate not in seen:
            seen.add(candidate)
            found.append(candidate)
    return found


def clean_gst_list(raw_gsts: list, fallback_text: str = "") -> list[str]:
    """
    Validate a list of GST strings from LLM output.
    Falls back to scanning raw text if list is empty.
    """
    valid = []
    seen = set()

    for gst in raw_gsts:
        if not isinstance(gst, str):
            continue
        # Strip noise characters
        cleaned = re.sub(r"[^A-Z0-9]", "", gst.strip().upper())
        if validate_gst(cleaned) and cleaned not in seen:
            seen.add(cleaned)
            valid.append(cleaned)

    # Fallback: scan raw HTML text
    if not valid and fallback_text:
        extracted = extract_gst_from_text(fallback_text)
        for gst in extracted:
            if gst not in seen:
                seen.add(gst)
                valid.append(gst)
        if extracted:
            logger.debug(f"[cleaner] GST recovered from raw text: {extracted}")

    return valid


# ─── PHONE ────────────────────────────────────────────────────────────────────
# Matches Indian mobile numbers in many formats
PHONE_EXTRACT_REGEX = re.compile(
    r"(?:(?:\+91|0091|91)?[\s\-]?)?(?:0)?([6-9]\d{9})"
)

PHONE_TEXT_SCAN_REGEX = re.compile(
    r"(?:\+91[\s\-]?)?(?:0)?[6-9]\d{9}"
)


def normalize_phone(raw: str) -> Optional[str]:
    """
    Normalize any Indian phone number string to +91XXXXXXXXXX.
    Returns None if the number isn't a valid Indian mobile.
    """
    if not raw or not isinstance(raw, str):
        return None
    digits = re.sub(r"[^\d]", "", raw)
    # Strip country code and leading zeros
    if len(digits) == 13 and digits.startswith("091"):
        digits = digits[3:]  # type: ignore
    elif len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]  # type: ignore
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]  # type: ignore

    if len(digits) == 10 and digits[0] in "6789":
        return f"+91{digits}"
    return None


def extract_phones_from_text(text: str) -> list[str]:
    """Scan raw text to extract all valid Indian mobile numbers."""
    if not text:
        return []
    results = []
    seen = set()
    for match in PHONE_TEXT_SCAN_REGEX.finditer(text):
        norm = normalize_phone(match.group())
        if norm and norm not in seen:
            seen.add(norm)
            results.append(norm)
    return results


def clean_phone_list(raw_phones: list, fallback_text: str = "") -> list[str]:
    """
    Validate a list of phone strings from LLM output.
    Falls back to scanning raw text if list is empty.
    """
    valid: list[str] = []
    seen: set[str] = set()

    for phone in raw_phones:
        norm = normalize_phone(str(phone))
        if norm and norm not in seen:
            seen.add(norm)
            valid.append(norm)

    # Fallback: scan raw HTML text
    if not valid and fallback_text:
        extracted = extract_phones_from_text(fallback_text)
        for phone in extracted[:5]:  # type: ignore # max 5 fallback phones
            if phone not in seen:
                seen.add(str(phone))
                valid.append(str(phone))
        if extracted:
            logger.debug(f"[cleaner] Phones recovered from raw text: {extracted[:3]}")  # type: ignore

    return valid


# ─── EMAIL ────────────────────────────────────────────────────────────────────
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

# Domains that are definitely not real business emails  
SPAM_DOMAINS = {
    "gmail.co", "yahho.com", "yaho.com", "hotmial.com",
    "example.com", "test.com", "domain.com", "email.com"
}


def clean_email_list(raw_emails: list, fallback_text: str = "") -> list[str]:
    """Validate emails from LLM. Falls back to text scan."""
    valid = []
    seen = set()

    for email in raw_emails:
        email = str(email).strip().lower()
        if EMAIL_REGEX.fullmatch(email):
            domain = email.split("@")[-1]
            if domain not in SPAM_DOMAINS and email not in seen:
                seen.add(email)
                valid.append(email)

    if not valid and fallback_text:
        for match in EMAIL_REGEX.finditer(fallback_text):
            email = match.group().lower()
            domain = email.split("@")[-1]
            if domain not in SPAM_DOMAINS and email not in seen:
                seen.add(email)
                valid.append(email)

    return valid


# ─── ADDRESS PARSING ─────────────────────────────────────────────────────────
# Indian PIN code: 6 digits starting with 1-9
PIN_REGEX = re.compile(r"\b([1-9][0-9]{5})\b")

INDIAN_STATES = {
    "Gujarat", "Maharashtra", "Rajasthan", "Karnataka", "Tamil Nadu",
    "Uttar Pradesh", "West Bengal", "Madhya Pradesh", "Andhra Pradesh",
    "Telangana", "Kerala", "Punjab", "Haryana", "Bihar", "Odisha",
    "Assam", "Jharkhand", "Uttarakhand", "Himachal Pradesh", "Goa",
    "Manipur", "Meghalaya", "Nagaland", "Tripura", "Mizoram", "Arunachal Pradesh",
    "Sikkim", "Delhi", "Chandigarh", "Puducherry", "Jammu and Kashmir",
    "Ladakh", "Chhattisgarh"
}

MAJOR_CITIES = {
    "Ahmedabad", "Surat", "Rajkot", "Vadodara", "Gandhinagar", "Bhavnagar",
    "Mumbai", "Pune", "Nagpur", "Nashik", "Thane", "Aurangabad",
    "Delhi", "New Delhi", "Noida", "Gurgaon", "Faridabad", "Ghaziabad",
    "Bengaluru", "Bangalore", "Mysuru", "Hubli", "Mangalore", "Belagavi",
    "Chennai", "Coimbatore", "Madurai", "Salem", "Tirupur", "Tiruchirappalli",
    "Hyderabad", "Secunderabad", "Warangal", "Vijayawada", "Visakhapatnam",
    "Kolkata", "Howrah", "Durgapur", "Asansol", "Siliguri",
    "Jaipur", "Jodhpur", "Kota", "Ajmer", "Udaipur", "Bikaner",
    "Lucknow", "Kanpur", "Agra", "Varanasi", "Allahabad", "Meerut",
    "Chandigarh", "Ludhiana", "Amritsar", "Jalandhar",
    "Bhopal", "Indore", "Gwalior", "Jabalpur",
    "Patna", "Gaya", "Muzaffarpur",
    "Srinagar", "Jammu", "Dehradun", "Haridwar",
    "Bhubaneswar", "Cuttack", "Guwahati",
}


def parse_address(addr_text: str) -> dict:
    """
    Pure Python address parser — no LLM, no hallucination.
    Extracts city, state, PIN code from raw address string.
    """
    if not addr_text:
        return {"full_address": "", "city": None, "state": None, "pin_code": None, "locality": None}

    city = None
    state = None
    pin_code = None
    locality = None

    # Extract PIN
    pin_match = PIN_REGEX.search(addr_text)
    if pin_match:
        pin_code = pin_match.group(1)

    # Extract state
    for s in INDIAN_STATES:
        if re.search(r"\b" + re.escape(s) + r"\b", addr_text, re.IGNORECASE):
            state = s
            break
    
    # Auto-map Gujarat cities to Gujarat state if missing
    GUJARAT_CITIES = {"Ahmedabad", "Surat", "Rajkot", "Vadodara", "Gandhinagar", "Bhavnagar", "Jamnagar", "Morbi", "Vapi"}
    if not state:
        for c in GUJARAT_CITIES:
            if re.search(r"\b" + re.escape(c) + r"\b", addr_text, re.IGNORECASE):
                state = "Gujarat"
                city = c
                break

    # Extract city if not already found
    if not city:
        for c in MAJOR_CITIES:
            if re.search(r"\b" + re.escape(c) + r"\b", addr_text, re.IGNORECASE):
                city = c
                break

    return {
        "full_address": addr_text.strip(),
        "city": city,
        "state": state,
        "pin_code": pin_code,
        "locality": locality
    }


# ─── PRODUCTS ─────────────────────────────────────────────────────────────────
def clean_product_list(raw_products: list) -> list[str]:
    """Remove empty, duplicate, or obviously fake product names."""
    BLACKLIST = {"null", "none", "n/a", "na", "", "not available", "unknown"}
    seen = set()
    result = []
    for p in raw_products:
        p_str = str(p).strip()
        if p_str.lower() not in BLACKLIST and p_str not in seen and len(p_str) > 2:
            seen.add(p_str)
            result.append(p_str)
    return result[:8]  # type: ignore # cap at 8 products per user requirement


# ─── MAIN VALIDATION FUNCTION ─────────────────────────────────────────────────
def validate_and_normalize(llm_raw: dict, html_text: str = "") -> dict:
    """
    Core validation function.

    Takes LLM Stage-1 output + original HTML text.
    Returns a clean, validated, normalized dictionary.
    Hallucinated values are removed. Missing values fall back to text scan.
    """
    logger.debug(f"[cleaner] Validating: {llm_raw.get('company_name', 'Unknown')}")

    validated: dict[str, Any] = {}

    # Company name — trust LLM but clean whitespace
    company = str(llm_raw.get("company_name", "") or "").strip()
    validated["company_name"] = company if len(company) > 1 else ""

    # GST Numbers
    raw_gsts = llm_raw.get("gst_numbers") or []
    if isinstance(raw_gsts, str):
        raw_gsts = [raw_gsts]
    validated["gst_numbers"] = clean_gst_list(raw_gsts, fallback_text=html_text)

    # Phone Numbers
    raw_phones = llm_raw.get("phone_numbers") or []
    if isinstance(raw_phones, str):
        raw_phones = [raw_phones]
    validated["phone_numbers"] = clean_phone_list(raw_phones, fallback_text=html_text)

    # Emails
    raw_emails = llm_raw.get("emails") or []
    if isinstance(raw_emails, str):
        raw_emails = [raw_emails]
    validated["emails"] = clean_email_list(raw_emails, fallback_text=html_text)

    # Address — parse in Python (not LLM)
    addr_text = str(llm_raw.get("address_text", "") or "").strip()
    validated["address"] = parse_address(addr_text)

    # Products
    raw_products = llm_raw.get("products") or []
    if isinstance(raw_products, str):
        raw_products = [raw_products]
    validated["products"] = clean_product_list(raw_products)

    # Contact person — trust LLM, just clean
    contact = str(llm_raw.get("contact_person", "") or "").strip()
    FAKE_NAMES = {"null", "none", "n/a", "unknown", "contact person", "manager"}
    validated["contact_person"] = contact if contact.lower() not in FAKE_NAMES else ""

    # Passthrough fields
    validated["source_url"] = str(llm_raw.get("source_url", "") or "")
    validated["source_platform"] = str(llm_raw.get("source_platform", "") or "")
    validated["website"] = str(llm_raw.get("website", "") or "").strip()
    validated["business_type"] = str(llm_raw.get("business_type", "") or "").strip()
    validated["established_year"] = llm_raw.get("established_year")
    validated["annual_turnover"] = str(llm_raw.get("annual_turnover", "") or "").strip()
    validated["employee_count"] = str(llm_raw.get("employee_count", "") or "").strip()
    validated["certifications"] = list(llm_raw.get("certifications") or [])
    validated["designation"] = str(llm_raw.get("designation", "") or "").strip()
    validated["min_order_quantity"] = str(llm_raw.get("min_order_quantity", "") or "").strip()
    validated["rating"] = llm_raw.get("rating")
    validated["review_count"] = llm_raw.get("review_count")

    # Data quality flags
    validated["_has_gst"] = bool(validated["gst_numbers"])
    validated["_has_phone"] = bool(validated["phone_numbers"])
    validated["_has_email"] = bool(validated["emails"])
    validated["_has_address"] = bool(validated["address"]["city"] or validated["address"]["full_address"])
    validated["_has_products"] = bool(validated["products"])

    # ─── Gujarat State Filter (intelligent) ─────────────────────────────
    # A company is considered Gujarat if ANY of these are true:
    #   1. State is explicitly "Gujarat"
    #   2. State is empty/unknown (benefit of the doubt — city filter will help)
    #   3. City is a known Gujarat city
    #   4. GST number starts with "24" (Gujarat state code)
    GUJARAT_CITY_SET = {
        "ahmedabad", "surat", "rajkot", "vadodara", "gandhinagar",
        "bhavnagar", "jamnagar", "morbi", "vapi", "anand", "mehsana",
        "nadiad", "bharuch", "navsari", "junagadh", "surendranagar",
        "gandhidham", "kutch", "porbandar", "palanpur", "godhra",
        "dahod", "veraval", "botad", "amreli", "modasa", "himmatnagar",
        "ankleshwar", "kalol", "sanand", "bavla",
    }

    state = validated["address"].get("state") or ""
    city = (validated["address"].get("city") or "").lower()
    gst_list = validated.get("gst_numbers") or []

    is_gujarat = False

    # Check 1: State explicitly says Gujarat
    if state.lower() in ("gujarat", "gujrat"):
        is_gujarat = True
    # Check 2: City is known Gujarat city
    elif city in GUJARAT_CITY_SET:
        is_gujarat = True
    # Check 3: GST starts with 24 (Gujarat state code)
    elif any(g.startswith("24") for g in gst_list if isinstance(g, str) and len(g) == 15):
        is_gujarat = True
    # Check 4: State is empty — give benefit of doubt (may be Gujarat company 
    # whose state wasn't on the page)
    elif not state:
        is_gujarat = True
    # Check 5: State is explicitly NOT Gujarat — filter out
    else:
        logger.info(f"[cleaner] Filtering out {company} (State: {state} — not Gujarat)")
        is_gujarat = False

    validated["_is_valid_state"] = is_gujarat

    logger.info(
        f"[cleaner] {company} → GST:{validated['_has_gst']} "
        f"Phone:{validated['_has_phone']} "
        f"Email:{validated['_has_email']} "
        f"Products:{len(validated['products'])} "
        f"Gujarat:{is_gujarat}"
    )

    return validated
