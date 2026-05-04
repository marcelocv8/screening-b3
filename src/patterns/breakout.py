import pandas as pd
import numpy as np
from typing import Tuple

def detect_breakout(df: pd.DataFrame, n_resistance: int = 20, m_volume: int = 10, vol_mult: float = 1.5) -> Tuple[bool, float, dict]:
    """
    Detect breakout above N-day resistance with volume confirmation.
    Returns (is_breakout, volume_ratio, details)
    """
    if len(df) < max(n_resistance, m_volume) + 5:
        return False, 0.0, {}
    
    # Resistance = highest high in last N days (excluding today)
    resistance = df["high"].iloc[-(n_resistance+1):-1].max()
    avg_volume = df["volume"].iloc[-(m_volume+1):-1].mean()
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # Breakout conditions
    price_breakout = last["close"] > resistance
    volume_confirm = last["volume"] >= vol_mult * avg_volume if avg_volume > 0 else False
    
    # Also check if high touched resistance and closed above
    high_breakout = last["high"] > resistance
    
    is_breakout = price_breakout and volume_confirm and high_breakout
    
    volume_ratio = last["volume"] / avg_volume if avg_volume > 0 else 0
    
    details = {
        "resistance_level": round(resistance, 2),
        "volume_ratio": round(volume_ratio, 2),
        "close_above_resistance": price_breakout,
        "volume_confirmed": volume_confirm,
    }
    
    return is_breakout, volume_ratio, details
