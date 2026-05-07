"""
FII Filter: whitelist approach for tickers ending in '11'.
Any ticker ending in '11' NOT in the whitelist is treated as FII and excluded.
"""
from pathlib import Path
from typing import Set

ALLOWED_11_PATH = Path("data/allowed_11.txt")

def load_allowed_11() -> Set[str]:
    """Load whitelist of tickers ending in 11 that should be KEPT."""
    allowed = set()
    
    if not ALLOWED_11_PATH.exists():
        return allowed
    
    with open(ALLOWED_11_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            ticker = line.split()[0].upper().strip()
            if ticker:
                allowed.add(ticker)
    
    return allowed

def is_fii(ticker: str, allowed_set: Set[str]) -> bool:
    """
    Check if a ticker should be excluded as FII.
    Rule: if ticker ends with '11' and is NOT in the allowed whitelist, it's a FII.
    """
    t = ticker.upper()
    if not t.endswith("11"):
        return False
    return t not in allowed_set
