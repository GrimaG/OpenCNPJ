"""Tests for CnpjUtils module."""
import pytest
from ETL_Python.utils.cnpj_utils import CnpjUtils


def test_remove_mask():
    """Test CNPJ mask removal."""
    assert CnpjUtils.remove_mask("12.345.678/0001-90") == "12345678000190"
    assert CnpjUtils.remove_mask("12345678000190") == "12345678000190"
    assert CnpjUtils.remove_mask("") == ""
    assert CnpjUtils.remove_mask(None) == ""


def test_is_valid_format():
    """Test CNPJ format validation."""
    # Valid formats
    assert CnpjUtils.is_valid_format("12345678000190")
    assert CnpjUtils.is_valid_format("12.345.678/0001-90")
    
    # Invalid formats
    assert not CnpjUtils.is_valid_format("")
    assert not CnpjUtils.is_valid_format(None)
    assert not CnpjUtils.is_valid_format("123")
    assert not CnpjUtils.is_valid_format("11111111111111")  # Repeated sequence
    assert not CnpjUtils.is_valid_format("AAAAAAAAAAAAAA")  # Repeated sequence


def test_parse_cnpj():
    """Test CNPJ parsing."""
    basico, ordem, dv = CnpjUtils.parse_cnpj("12345678000190")
    assert basico == "12345678"
    assert ordem == "0001"
    assert dv == "90"
    
    # Test with mask
    basico, ordem, dv = CnpjUtils.parse_cnpj("12.345.678/0001-90")
    assert basico == "12345678"
    assert ordem == "0001"
    assert dv == "90"
    
    # Test invalid length
    with pytest.raises(ValueError):
        CnpjUtils.parse_cnpj("123")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
