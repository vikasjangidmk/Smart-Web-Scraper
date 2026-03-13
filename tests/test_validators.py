"""
test_validators.py - Unit tests for GST and Phone validators
Run: python -m pytest tests/ -v
"""
import sys
from pathlib import Path

# Allow imports from lead_scraper root
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from validator.gst_validator import validate_gst, sanitize_gst, extract_gst_from_text
from validator.phone_validator import normalize_phone, validate_phone, sanitize_phone_list, extract_phones_from_text


# ─────────────────────────── GST Validator Tests ───────────────────────────

class TestGSTValidator:

    def test_valid_gst_gujarat(self):
        assert validate_gst("24ABCDE1234F1Z5") is True

    def test_valid_gst_maharashtra(self):
        assert validate_gst("27AABCU9603R1ZM") is True

    def test_valid_gst_delhi(self):
        assert validate_gst("07AAACR5055K1Z4") is True

    def test_invalid_gst_too_short(self):
        assert validate_gst("24ABCDE1234F1Z") is False

    def test_invalid_gst_too_long(self):
        assert validate_gst("24ABCDE1234F1Z55") is False

    def test_invalid_gst_lowercase(self):
        assert validate_gst("24abcde1234f1z5") is False

    def test_invalid_gst_none(self):
        assert validate_gst(None) is False

    def test_invalid_gst_empty(self):
        assert validate_gst("") is False

    def test_invalid_gst_random_string(self):
        assert validate_gst("NOTVALIDGST1234") is False

    def test_sanitize_gst_with_spaces(self):
        result = sanitize_gst("24 ABCDE 1234 F1Z5")
        assert result == "24ABCDE1234F1Z5"

    def test_sanitize_gst_with_hyphens(self):
        result = sanitize_gst("24-ABCDE-1234-F1Z5")
        assert result == "24ABCDE1234F1Z5"

    def test_sanitize_gst_invalid_returns_none(self):
        result = sanitize_gst("NOT-A-GST")
        assert result is None

    def test_extract_gst_from_text(self):
        text = "Company GST: 24ABCDE1234F1Z5 | Registered in Gujarat"
        result = extract_gst_from_text(text)
        assert result == "24ABCDE1234F1Z5"

    def test_extract_gst_from_text_not_found(self):
        text = "No GST number here"
        result = extract_gst_from_text(text)
        assert result is None

    def test_extract_gst_from_mixed_text(self):
        text = "GSTIN Number: 27AABCU9603R1ZM, Contact: 9876543210"
        result = extract_gst_from_text(text)
        assert result == "27AABCU9603R1ZM"


# ─────────────────────────── Phone Validator Tests ─────────────────────────

class TestPhoneValidator:

    def test_normalize_10_digit(self):
        assert normalize_phone("9876543210") == "+919876543210"

    def test_normalize_11_digit_with_zero(self):
        assert normalize_phone("09876543210") == "+919876543210"

    def test_normalize_12_digit_with_91(self):
        assert normalize_phone("919876543210") == "+919876543210"

    def test_normalize_with_plus_91(self):
        assert normalize_phone("+919876543210") == "+919876543210"

    def test_normalize_with_spaces(self):
        assert normalize_phone("98765 43210") == "+919876543210"

    def test_normalize_with_hyphens(self):
        assert normalize_phone("98765-43210") == "+919876543210"

    def test_invalid_landline_short(self):
        # 7-digit number — too short
        assert normalize_phone("1234567") is None

    def test_invalid_number_starting_with_5(self):
        # Indian mobile starts 6-9
        assert normalize_phone("5876543210") is None

    def test_validate_phone_valid(self):
        assert validate_phone("9876543210") is True

    def test_validate_phone_invalid(self):
        assert validate_phone("12345") is False

    def test_sanitize_phone_list(self):
        phones = ["9876543210", "invalid", "+919123456789", "09876543210"]
        result = sanitize_phone_list(phones)
        assert "+919876543210" in result
        assert "+919123456789" in result
        # duplicates removed
        assert len(result) == len(set(result))

    def test_sanitize_phone_list_deduplication(self):
        phones = ["9876543210", "09876543210", "+919876543210"]
        result = sanitize_phone_list(phones)
        assert len(result) == 1
        assert result[0] == "+919876543210"

    def test_extract_phones_from_text(self):
        text = "Call us at 9876543210 or +91-9123456789. Office: 079-26583000"
        result = extract_phones_from_text(text)
        assert any("9876543210" in p for p in result)

    def test_sanitize_empty_list(self):
        assert sanitize_phone_list([]) == []

    def test_sanitize_none_list(self):
        assert sanitize_phone_list(None) == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
