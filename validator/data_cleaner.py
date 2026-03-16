"""
data_cleaner.py - Python Validation & Normalization Layer (Stage 2 of Pipeline)

Optimized for:
  1. MANDATORY phone + GST validation
  2. Invalid company name filtering
  3. Improved Gujarat city detection
  4. Cross-record deduplication support
  5. PIN code validation
  6. Contact number extraction from company section only (not global footer)
"""
import re
from typing import Optional, Any
from bs4 import BeautifulSoup  # type: ignore
from utils.logger import get_logger  # type: ignore

logger = get_logger(__name__)


def clean_html(html: str) -> str:
    """Remove scripts, styles, and boilerplate from HTML. Returns clean text."""
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        # Remove completely useless tags
        for tag in soup(["script", "style", "nav", "footer", "header",
                         "svg", "noscript", "iframe", "form",
                         "meta", "link", "img", "picture"]):
            tag.decompose()
        # Get text with clean spacing
        text = soup.get_text(separator=" ", strip=True)
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text)
        return text
    except Exception as e:
        logger.warning(f"[cleaner] HTML cleaning failed: {e}")
        return html


def extract_company_section_html(html: str) -> str:
    """
    Extract only the company information section from HTML.
    This avoids pulling global website contact numbers from header/footer.
    """
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        # Remove global header, footer, nav — these contain website-wide contacts
        for tag in soup(["header", "footer", "nav", "script", "style",
                         "svg", "noscript", "iframe", "meta", "link",
                         "img", "picture"]):
            tag.decompose()

        # Try to find company-specific sections
        company_sections = []
        company_selectors = [
            # IndiaMART
            {"class": re.compile(r"company|contact|about|profile|supplier|detail", re.I)},
            {"id": re.compile(r"company|contact|about|profile|supplier|detail", re.I)},
        ]
        for sel in company_selectors:
            found = soup.find_all(attrs=sel)
            for section in found:
                company_sections.append(section.get_text(separator=" ", strip=True))

        if company_sections:
            return " ".join(company_sections)

        # Fallback: return full cleaned text
        return soup.get_text(separator=" ", strip=True)
    except Exception:
        return html


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

# Gujarat GST state code
GUJARAT_GST_CODE = "24"


def validate_gst(gst: str) -> bool:
    """Returns True if GST format is valid (15-char with valid state code)."""
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
    """Validate GST strings. Falls back to raw text scan."""
    valid = []
    seen = set()

    for gst in raw_gsts:
        if not isinstance(gst, str):
            continue
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
PHONE_EXTRACT_REGEX = re.compile(
    r"(?:(?:\+91|0091|91)?[\s\-]?)?(?:0)?([6-9]\d{9})"
)

PHONE_TEXT_SCAN_REGEX = re.compile(
    r"(?:\+91[\s\-]?)?(?:0)?[6-9]\d{9}"
)

# Phone numbers to exclude (common website/toll-free numbers)
EXCLUDE_PHONE_PATTERNS = [
    "1800",     # Toll-free
    "1860",     # Premium
    "0120",     # Noida (IndiaMART HQ)
    "0124",     # Gurgaon
]


def normalize_phone(raw: str) -> Optional[str]:
    """Normalize Indian phone number to +91XXXXXXXXXX."""
    if not raw or not isinstance(raw, str):
        return None

    # Skip toll-free / corporate numbers
    for pattern in EXCLUDE_PHONE_PATTERNS:
        if pattern in raw:
            return None

    digits = re.sub(r"[^\d]", "", raw)
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
    """Extract valid Indian mobile numbers from text."""
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
    """Validate phone list. Falls back to text scan."""
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
        for phone in extracted[:5]:  # type: ignore
            if phone not in seen:
                seen.add(str(phone))
                valid.append(str(phone))

    return valid


# ─── EMAIL ────────────────────────────────────────────────────────────────────
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

SPAM_DOMAINS = {
    "gmail.co", "yahho.com", "yaho.com", "hotmial.com",
    "example.com", "test.com", "domain.com", "email.com",
    "sentry.io", "indiamart.com", "tradeindia.com",
    "exportersindia.com",  # Platform emails, not company emails
}


def clean_email_list(raw_emails: list, fallback_text: str = "") -> list[str]:
    """Validate emails. Falls back to text scan."""
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
PIN_REGEX = re.compile(r"\b([1-9][0-9]{5})\b")

# Gujarat PIN codes start with 36, 37, 38, 39
GUJARAT_PIN_PREFIXES = {"36", "37", "38", "39"}

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

# Extended Gujarat cities list
GUJARAT_CITIES = {
    "ahmedabad", "surat", "rajkot", "vadodara", "gandhinagar",
    "bhavnagar", "jamnagar", "morbi", "vapi", "anand", "mehsana",
    "nadiad", "bharuch", "navsari", "junagadh", "surendranagar",
    "gandhidham", "kutch", "porbandar", "palanpur", "godhra",
    "dahod", "veraval", "botad", "amreli", "modasa", "himmatnagar",
    "ankleshwar", "kalol", "sanand", "bavla", "mundra", "halvad",
    "gondal", "wankaner", "jetpur", "dhoraji", "upleta", "mahuva",
    "sihor", "palitana", "khambhat", "petlad", "borsad", "umreth",
    "visnagar", "patan", "unjha", "deesa", "dhanera", "radhanpur",
    "idar", "shamlaji", "viramgam", "dholka", "mandal", "balasinor",
    "lunawada", "devgadh baria", "chhota udepur", "halol", "kalol",
    "savli", "dabhoi", "padra", "karjan", "sinor", "waghodia",
    "olpad", "kamrej", "choryasi", "bardoli", "vyara", "mandvi",
    "songadh", "valod", "nizar", "mahuva", "una", "kodinar",
    "talala", "visavadar", "manavadar", "keshod", "mangrol",
    "valsad", "pardi", "umbergaon", "dharampur", "chikhli",
    "bilimora", "gandevi", "khambhalia", "dwarka", "okha",
    "lalpur", "jam jodhpur", "kalavad", "jodia",
}


def validate_pin_code(pin: str) -> bool:
    """Validate Indian PIN code (6 digits, starts 1-9)."""
    if not pin or not isinstance(pin, str):
        return False
    pin = pin.strip()
    return bool(re.fullmatch(r"[1-9][0-9]{5}", pin))


def is_gujarat_pin(pin: str) -> bool:
    """Check if PIN code belongs to Gujarat (starts with 36-39)."""
    if not validate_pin_code(pin):
        return False
    return pin[:2] in GUJARAT_PIN_PREFIXES  # type: ignore


def parse_address(addr_text: str) -> dict:
    """Pure Python address parser — extracts city, state, PIN code."""
    if not addr_text:
        return {"full_address": "", "city": None, "state": None, "pin_code": None, "locality": None}

    city = None
    state = None
    pin_code = None
    locality = None

    # Extract PIN
    pin_match = PIN_REGEX.search(addr_text)
    if pin_match:
        candidate = pin_match.group(1)
        if validate_pin_code(candidate):
            pin_code = candidate

    # Extract state
    for s in INDIAN_STATES:
        if re.search(r"\b" + re.escape(s) + r"\b", addr_text, re.IGNORECASE):
            state = s
            break

    # Auto-map Gujarat cities to Gujarat state if missing
    if not state:
        for c in GUJARAT_CITIES:
            if re.search(r"\b" + re.escape(c) + r"\b", addr_text, re.IGNORECASE):
                state = "Gujarat"
                city = c.title()
                break

    # If we have a Gujarat PIN code, set state
    if not state and pin_code and is_gujarat_pin(pin_code):
        state = "Gujarat"

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
    BLACKLIST = {"null", "none", "n/a", "na", "", "not available", "unknown",
                 "product", "products", "service", "services", "more"}
    seen = set()
    result = []
    for p in raw_products:
        p_str = str(p).strip()
        if p_str.lower() not in BLACKLIST and p_str not in seen and len(p_str) > 2:
            seen.add(p_str)
            result.append(p_str)
    return result[:8]  # type: ignore


# ─── COMPANY NAME VALIDATION ─────────────────────────────────────────────────
INVALID_COMPANY_NAMES = {
    "ajax", "demo", "test", "login", "register", "signup", "sign up",
    "contact us", "about us", "home", "index", "undefined", "null",
    "none", "n/a", "unknown", "company name", "your company",
    "sample", "example", "placeholder", "template", "untitled",
    "loading", "error", "not found", "page not found", "404",
    "indiamart", "tradeindia", "exportersindia", "justdial",
}


def is_valid_company_name(name: str) -> bool:
    """Check if company name is valid and not a placeholder/system text."""
    if not name or not isinstance(name, str):
        return False
    name_clean = name.strip()
    if len(name_clean) < 3:
        return False
    if name_clean.lower() in INVALID_COMPANY_NAMES:
        return False
    # Reject names that are just numbers or symbols
    if re.fullmatch(r"[\d\s\-_.,]+", name_clean):
        return False
    # Reject names with too many special characters
    alpha_ratio = len(re.findall(r"[a-zA-Z]", name_clean)) / max(len(name_clean), 1)
    if alpha_ratio < 0.3:
        return False
    return True


# ─── PACKAGING KEYWORD CHECK ─────────────────────────────────────────────────
PACKAGING_KEYWORDS = [
    "bubble", "packaging", "packing", "wrap", "roll", "sheet",
    "corrugated", "stretch film", "foam", "cushion", "poly bag",
    "shrink", "lamination", "carton", "box", "tape", "polythene",
    "ldpe", "hdpe", "bopp", "thermocol", "styrofoam", "epe",
    "air column", "void fill", "dunnage", "padded", "envelope",
]


def has_packaging_keywords(text: str) -> bool:
    """Check if text contains any packaging-related keywords."""
    if not text:
        return False
    text_lower = text.lower()
    return any(kw in text_lower for kw in PACKAGING_KEYWORDS)


# ─── MAIN VALIDATION FUNCTION ─────────────────────────────────────────────────
def validate_and_normalize(llm_raw: dict, html_text: str = "") -> dict:
    """
    Core validation function with MANDATORY phone + GST enforcement.

    Takes LLM Stage-1 output + original HTML text.
    Returns clean, validated dictionary.
    """
    validated: dict[str, Any] = {}

    # Company name validation
    company = str(llm_raw.get("company_name", "") or "").strip()
    if not is_valid_company_name(company):
        company = ""
    validated["company_name"] = company

    # Extract company-section-specific text for phone/GST
    company_section_text = extract_company_section_html(html_text) if html_text else ""

    # GST Numbers — scan company section first, then full HTML
    raw_gsts = llm_raw.get("gst_numbers") or []
    if isinstance(raw_gsts, str):
        raw_gsts = [raw_gsts]
    validated["gst_numbers"] = clean_gst_list(raw_gsts, fallback_text=html_text)

    # Phone Numbers — prefer company section contacts over global contacts
    raw_phones = llm_raw.get("phone_numbers") or []
    if isinstance(raw_phones, str):
        raw_phones = [raw_phones]
    # First try company section phones
    company_phones = extract_phones_from_text(company_section_text) if company_section_text else []
    if company_phones:
        raw_phones = list(set(raw_phones) | set(company_phones))
    validated["phone_numbers"] = clean_phone_list(raw_phones, fallback_text=html_text)

    # Emails
    raw_emails = llm_raw.get("emails") or []
    if isinstance(raw_emails, str):
        raw_emails = [raw_emails]
    validated["emails"] = clean_email_list(raw_emails, fallback_text=html_text)

    # Address parsing
    addr_text = str(llm_raw.get("address_text", "") or "").strip()
    validated["address"] = parse_address(addr_text)

    # PIN code validation
    if validated["address"]["pin_code"] and not validate_pin_code(validated["address"]["pin_code"]):
        validated["address"]["pin_code"] = None

    # Products
    raw_products = llm_raw.get("products") or []
    if isinstance(raw_products, str):
        raw_products = [raw_products]
    validated["products"] = clean_product_list(raw_products)

    # Contact person
    contact = str(llm_raw.get("contact_person", "") or "").strip()
    FAKE_NAMES = {"null", "none", "n/a", "unknown", "contact person", "manager",
                  "admin", "administrator", "webmaster", "info"}
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

    # ─── Gujarat State Filter ────────────────────────────────────────────
    state = validated["address"].get("state") or ""
    city = (validated["address"].get("city") or "").lower()
    gst_list = validated.get("gst_numbers") or []
    pin_code = validated["address"].get("pin_code") or ""

    is_gujarat = False

    # Check 1: State explicitly says Gujarat
    if state.lower() in ("gujarat", "gujrat"):
        is_gujarat = True
    # Check 2: City is known Gujarat city
    elif city in GUJARAT_CITIES:
        is_gujarat = True
    # Check 3: GST starts with 24 (Gujarat state code)
    elif any(g.startswith("24") for g in gst_list if isinstance(g, str) and len(g) == 15):
        is_gujarat = True
    # Check 4: PIN code is Gujarat
    elif pin_code and is_gujarat_pin(pin_code):
        is_gujarat = True
    # Check 5: State is empty — give benefit of doubt
    elif not state:
        is_gujarat = True
    # Check 6: State is explicitly NOT Gujarat — filter out
    else:
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
