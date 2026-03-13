"""
test_llm_extractor.py - Unit tests for the LLM extractor JSON parsing logic
These tests mock the OpenRouter API call so no real network request is made.
Run: python -m pytest tests/ -v
"""
import sys
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from parser.llm_extractor import extract_raw_data, format_schema, clean_html


MOCK_RAW_RESPONSE = {
    "company_name": "ABC Bubble Roll Packaging",
    "phone_numbers": ["+919876543210", "+919876543211"],
    "emails": ["info@abcpackaging.com"],
    "gst_numbers": ["24ABCDE1234F1Z5"],
    "address_text": "123 GIDC Estate, Vatva, Ahmedabad, Gujarat 382445",
    "products": ["Bubble Roll", "Air Bubble Sheet"],
    "contact_person": "Ramesh Patel"
}

MOCK_SCHEMA_RESPONSE = {
    "company_name": "ABC Bubble Roll Packaging",
    "business_type": "",
    "phone": ["+919876543210"],
    "email": ["info@abcpackaging.com"],
    "website": "",
    "gst_number": "24ABCDE1234F1Z5",
    "address": {
        "full_address": "123 GIDC Estate, Vatva, Ahmedabad, Gujarat 382445"
    },
    "products": ["Bubble Roll", "Air Bubble Sheet"],
    "source_url": "https://example.com"
}


def _make_mock_response(content: dict | str) -> MagicMock:
    """Build a mock requests.Response from a dict or string."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    body_str = json.dumps(content) if isinstance(content, dict) else content
    mock_resp.json.return_value = {
        "choices": [{"message": {"content": body_str}}]
    }
    return mock_resp


class TestLLMExtractor:

    @patch("parser.llm_extractor.requests.post")
    def test_raw_extraction(self, mock_post):
        mock_post.return_value = _make_mock_response(MOCK_RAW_RESPONSE)
        result = extract_raw_data("<html>test</html>", "https://example.com")
        assert result is not None
        assert result["company_name"] == "ABC Bubble Roll Packaging"
        assert result["gst_numbers"] == ["24ABCDE1234F1Z5"]
        assert len(result["phone_numbers"]) == 2

    @patch("parser.llm_extractor.requests.post")
    def test_strips_markdown_fences(self, mock_post):
        """LLM sometimes wraps JSON in ```json ... ``` — must be stripped."""
        raw_with_fences = "```json\n" + json.dumps(MOCK_RAW_RESPONSE) + "\n```"
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": raw_with_fences}}]
        }
        mock_post.return_value = mock_resp
        result = extract_raw_data("<html>test</html>", "https://example.com")
        assert result is not None
        assert result["company_name"] == "ABC Bubble Roll Packaging"

    @patch("parser.llm_extractor.requests.post")
    def test_api_error_returns_none(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_post.return_value = mock_resp
        result = extract_raw_data("<html>test</html>", "https://example.com")
        assert result is None

    @patch("parser.llm_extractor.requests.post")
    def test_invalid_json_returns_none(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "not valid json {"}}]
        }
        mock_post.return_value = mock_resp
        result = extract_raw_data("<html>test</html>", "https://example.com")
        assert result is None

    @patch("parser.llm_extractor.requests.post")
    def test_source_url_injected(self, mock_post):
        """source_url must always be set from the argument, not just LLM response."""
        mock_post.return_value = _make_mock_response(MOCK_RAW_RESPONSE)
        url = "https://www.indiamart.com/test-company/"
        result = extract_raw_data("<html>test</html>", url)
        assert result["source_url"] == url

    @patch("parser.llm_extractor.requests.post")
    def test_html_truncation(self, mock_post):
        """HTML longer than max_chars must be truncated before sending."""
        mock_post.return_value = _make_mock_response(MOCK_RAW_RESPONSE)
        long_html = "A" * 20000
        result = extract_raw_data(long_html, "https://example.com", max_chars=5000)
        assert mock_post.called
        call_args = mock_post.call_args
        payload = call_args[1]["json"]
        user_msg = payload["messages"][1]["content"]
        assert len(user_msg) < 5000 + 500

    @patch("parser.llm_extractor.requests.post")
    def test_format_schema(self, mock_post):
        """Test the second stage LLM formatting."""
        mock_post.return_value = _make_mock_response(MOCK_SCHEMA_RESPONSE)
        result = format_schema(MOCK_RAW_RESPONSE)
        assert result is not None
        assert result["company_name"] == "ABC Bubble Roll Packaging"
        assert result["gst_number"] == "24ABCDE1234F1Z5"
        assert "address" in result

    def test_clean_html(self):
        """Test HTML purification."""
        raw_html = '<html><head><script>alert(1)</script><style>body {color: red;}</style></head><body><h1>Company</h1><nav>Menu</nav></body></html>'
        cleaned = clean_html(raw_html)
        assert "Company" in cleaned
        assert "alert(1)" not in cleaned
        assert "Menu" not in cleaned

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
