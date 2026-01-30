import json
import re
from typing import Any, Dict


class JsonCleanupUtils:
    """Utilities for JSON cleanup and normalization."""
    
    @staticmethod
    def normalize_spaces(input_str: str) -> str:
        """Normalize multiple spaces in a string."""
        if not input_str:
            return ""
        return re.sub(r"\s+", " ", input_str.strip())
    
    @staticmethod
    def clean_json_spaces(json_content: str) -> str:
        """Clean excessive spaces in JSON text fields."""
        try:
            data = json.loads(json_content)
            cleaned = JsonCleanupUtils._clean_element(data)
            return json.dumps(cleaned, ensure_ascii=False, separators=(',', ':'))
        except Exception:
            return json_content
    
    @staticmethod
    def _clean_element(element: Any) -> Any:
        """Recursively clean spaces in JSON elements."""
        if isinstance(element, dict):
            return {
                key: JsonCleanupUtils._clean_element(value)
                for key, value in element.items()
            }
        elif isinstance(element, list):
            return [JsonCleanupUtils._clean_element(item) for item in element]
        elif isinstance(element, str):
            return JsonCleanupUtils.normalize_spaces(element)
        else:
            return element
