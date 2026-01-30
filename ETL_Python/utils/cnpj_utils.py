import re
from typing import Optional, Tuple


class CnpjUtils:
    """Utilities for CNPJ validation and normalization (including alphanumeric)."""
    
    MASK_CHARACTERS = re.compile(r"[./-]")
    INVALID_CHARACTERS = re.compile(r"[^A-Z\d./-]", re.IGNORECASE)
    BASE_CNPJ_PATTERN = re.compile(r"^[A-Z\d]{12}$")
    FULL_CNPJ_PATTERN = re.compile(r"^[A-Z\d]{12}\d{2}$")
    BASE_LENGTH = 12
    
    @staticmethod
    def remove_mask(cnpj: Optional[str]) -> str:
        """Remove mask from CNPJ (dots, slashes, hyphens) and convert to uppercase."""
        if not cnpj or not cnpj.strip():
            return ""
        return CnpjUtils.MASK_CHARACTERS.sub("", cnpj).upper()
    
    @staticmethod
    def is_valid_format(cnpj: Optional[str]) -> bool:
        """Validate if CNPJ has valid format (alphanumeric: 12 alphanumeric chars + 2 digits)."""
        if not cnpj or not cnpj.strip():
            return False
        
        if CnpjUtils.INVALID_CHARACTERS.search(cnpj):
            return False
        
        raw = CnpjUtils.remove_mask(cnpj)
        
        if len(raw) != 14:
            return False
        
        if not CnpjUtils.FULL_CNPJ_PATTERN.match(raw):
            return False
        
        if CnpjUtils._is_repeated_sequence(raw):
            return False
        
        return True
    
    @staticmethod
    def _is_repeated_sequence(cnpj: str) -> bool:
        """Check if it's a repeated sequence (e.g., 11111111111111 or AAAAAAAAAAAAAA)."""
        if not cnpj or len(cnpj) < 2:
            return False
        
        first_char = cnpj[0]
        return all(c == first_char for c in cnpj)
    
    @staticmethod
    def parse_cnpj(cnpj: str) -> Tuple[str, str, str]:
        """
        Extract CNPJ parts: basico (8), ordem (4), dv (2).
        Removes mask automatically before extracting.
        """
        raw = CnpjUtils.remove_mask(cnpj)
        
        if len(raw) != 14:
            raise ValueError(f"CNPJ must have 14 characters after removing mask. Received: {len(raw)}")
        
        basico = raw[:8]
        ordem = raw[8:12]
        dv = raw[12:14]
        
        return basico, ordem, dv
