
"""
phone_validator.py - Phone number normalization and validation
"""
import re
from utils.logger import get_logger  # type: ignore

logger = get_logger(__name__)

# Matches Indian phone numbers in various formats
PHONE_REGEX = re.compile(
    r"(?:\+91[\-\s]?)?(?:0)?[6-9]\d{9}"
)


def normalize_phone(raw: str) -> str | None:
    """
    Normalize a phone number string to +91XXXXXXXXXX format.
    Returns None if no valid number found.
    """
    if not raw or not isinstance(raw, str):
        return None
    # Strip everything except digits and leading +
    digits_only = re.sub(r"[^\d]", "", raw)
    if len(digits_only) == 10 and digits_only[0] in "6789":
        return f"+91{digits_only}"
    elif len(digits_only) == 11 and digits_only.startswith("0"):
        return f"+91{digits_only[1:]}"  # type: ignore
    elif len(digits_only) == 12 and digits_only.startswith("91"):
        num = digits_only[2:]  # type: ignore
        if num[0] in "6789":
            return f"+91{num}"
    elif len(digits_only) == 13 and digits_only.startswith("091"):
        num = digits_only[3:]  # type: ignore
        if num[0] in "6789":
            return f"+91{num}"
    return None


def validate_phone(phone: str) -> bool:
    """
    Returns True if the string is a valid Indian mobile number.
    """
    normalized = normalize_phone(phone)
    return normalized is not None


def extract_phones_from_text(text: str) -> list[str]:
    """
    Extract all valid phone numbers from raw text.
    """
    if not text:
        return []
    results = []
    seen = set()
    # Find all candidate phone strings
    candidates = re.findall(r"[\+\d][\d\s\-\.]{8,14}\d", text)
    for candidate in candidates:
        norm = normalize_phone(candidate)
        if norm and norm not in seen:
            seen.add(norm)
            results.append(norm)
    return results


def sanitize_phone_list(phones: list) -> list[str]:
    """
    Clean a list of phone strings. Remove invalid entries and normalize.
    """
    if not phones:
        return []
    result = []
    seen = set()
    for phone in phones:
        norm = normalize_phone(str(phone))
        if norm and norm not in seen:
            seen.add(norm)
            result.append(norm)
    return result
