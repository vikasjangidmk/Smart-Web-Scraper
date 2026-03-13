
"""
gst_validator.py - GST number format validation
"""
import re
from utils.logger import get_logger  # type: ignore

logger = get_logger(__name__)

# Official GST regex:
# 2 digits (state code) + 5 uppercase letters (PAN chars 1-5) +
# 4 digits (PAN chars 6-9) + 1 letter (PAN char 10) +
# 1 alphanumeric (entity number) + Z (always Z) + 1 alphanumeric (check digit)
GST_REGEX = re.compile(
    r"^[0-3][0-9][A-Z]{5}[0-9]{4}[A-Z][A-Z0-9]Z[A-Z0-9]$"
)

VALID_STATE_CODES = {
    "01","02","03","04","05","06","07","08","09","10",
    "11","12","13","14","15","16","17","18","19","20",
    "21","22","23","24","25","26","27","28","29","30",
    "31","32","33","34","35","36","37","38",
}


def validate_gst(gst: str) -> bool:
    """
    Returns True if the provided string is a valid GST number format.
    """
    if not gst or not isinstance(gst, str):
        return False
    gst = gst.strip()
    if len(gst) != 15:
        return False
    if not GST_REGEX.match(gst):
        return False
    state_code = gst[:2]  # type: ignore
    if state_code not in VALID_STATE_CODES:
        logger.debug(f"[gst] Unknown state code: {state_code} in {gst}")
    return True


def extract_gst_from_text(text: str) -> str | None:
    """
    Scan raw text for a GST-formatted number and return the first valid match.
    """
    if not text:
        return None
    # Find all 15-char candidates that look like GST
    candidates = re.findall(r"\b[0-3][0-9][A-Z]{5}[0-9]{4}[A-Z][A-Z0-9]Z[A-Z0-9]\b", text.upper())
    for candidate in candidates:
        if validate_gst(candidate):
            logger.debug(f"[gst] Found valid GST: {candidate}")
            return candidate
    return None


def sanitize_gst(gst: str | None) -> str | None:
    """
    Clean and validate a GST string. Returns sanitized GST or None if invalid.
    """
    if not gst:
        return None
    cleaned = re.sub(r"[^A-Z0-9]", "", gst.strip().upper())
    if validate_gst(cleaned):
        return cleaned
    # Try extracting from messy string
    extracted = extract_gst_from_text(gst)
    return extracted
