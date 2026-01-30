"""Tests for JsonCleanupUtils module."""
import json
from ETL_Python.utils.json_cleanup_utils import JsonCleanupUtils


def test_normalize_spaces():
    """Test space normalization."""
    assert JsonCleanupUtils.normalize_spaces("  hello   world  ") == "hello world"
    assert JsonCleanupUtils.normalize_spaces("single") == "single"
    assert JsonCleanupUtils.normalize_spaces("") == ""


def test_clean_json_spaces():
    """Test JSON space cleaning."""
    # Test with nested structure
    input_json = json.dumps({
        "name": "  Test   Company  ",
        "address": {
            "street": "   Main  Street   "
        },
        "items": ["  item1  ", "  item2  "]
    })
    
    output = JsonCleanupUtils.clean_json_spaces(input_json)
    data = json.loads(output)
    
    assert data["name"] == "Test Company"
    assert data["address"]["street"] == "Main Street"
    assert data["items"][0] == "item1"
    assert data["items"][1] == "item2"


def test_clean_json_spaces_preserves_numbers():
    """Test that numbers are preserved."""
    input_json = json.dumps({
        "name": "  Test  ",
        "count": 42,
        "value": 3.14
    })
    
    output = JsonCleanupUtils.clean_json_spaces(input_json)
    data = json.loads(output)
    
    assert data["name"] == "Test"
    assert data["count"] == 42
    assert data["value"] == 3.14


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
