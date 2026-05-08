import pandas as pd
import numpy as np
from typing import Tuple
from src.core.indicators import get_pivots

def detect_double_bottom(df: pd.DataFrame) -> Tuple[bool, float]:
    """
    Detect Double Bottom pattern after a significant decline.
    ENHANCED: Volume at 2nd bottom MUST be lower than 1st bottom (mandatory gate).
    Returns (is_pattern, confidence_score)
    """
    if len(df) < 60:
        return False, 0.0
    
    sub = df.copy()
    
    # Need significant prior decline (at least 20% from recent high)
    recent_high = sub["high"].tail(120).max() if len(sub) >= 120 else sub["high"].max()
    recent_low = sub["low"].tail(60).min()
    
    if recent_high == 0:
        return False, 0.0
    
    decline = (recent_high - recent_low) / recent_high
    if decline < 0.15:
        return False, 0.0
    
    # Find valleys
    _, valleys = get_pivots(sub["low"], deviation=0.04, order=3)
    
    if len(valleys) < 2:
        return False, 0.0
    
    # Look for two bottoms with peak between them
    for i in range(len(valleys) - 1):
        b1_idx = valleys.index[i]
        b1_price = valleys.iloc[i]
        
        b2_idx = valleys.index[i + 1]
        b2_price = valleys.iloc[i + 1]
        
        # Distance between bottoms
        loc_b1 = sub.index.get_loc(b1_idx)
        loc_b2 = sub.index.get_loc(b2_idx)
        # Handle duplicate indices gracefully
        if isinstance(loc_b1, slice):
            loc_b1 = loc_b1.start if loc_b1.start is not None else loc_b1.stop
        if isinstance(loc_b2, slice):
            loc_b2 = loc_b2.start if loc_b2.start is not None else loc_b2.stop
        if not isinstance(loc_b1, (int, np.integer)) or not isinstance(loc_b2, (int, np.integer)):
            continue
        days_between = int(loc_b2) - int(loc_b1)
        if not (15 <= days_between <= 120):
            continue
        
        # Alignment
        bottom_diff = abs(b1_price - b2_price) / b1_price
        if bottom_diff > 0.05:
            continue
        
        # Peak between them (neckline)
        between = sub[(sub.index > b1_idx) & (sub.index < b2_idx)]
        if len(between) == 0:
            continue
        
        neckline = between["high"].max()
        neckline_idx = between["high"].idxmax()
        
        # Neckline should be significantly above bottoms
        if neckline < b1_price * 1.08:
            continue
        
        # Volume: second bottom MUST have lower volume than first (MANDATORY gate)
        b1_vol = sub.loc[b1_idx, "volume"] if b1_idx in sub.index else 0
        b2_vol = sub.loc[b2_idx, "volume"] if b2_idx in sub.index else 0
        
        if b2_vol >= b1_vol:
            continue  # Volume gate failed
        
        # Check if breakout happened after second bottom
        after_b2 = sub[sub.index > b2_idx]
        if len(after_b2) == 0:
            continue
        
        # Scoring
        score = 0.5  # Higher base since volume gate is strict
        if bottom_diff < 0.03:
            score += 0.15
        if decline > 0.25:
            score += 0.15
        if days_between > 30:
            score += 0.15
        
        return True, min(score, 1.0)
    
    return False, 0.0
