import pandas as pd
import numpy as np
from typing import Tuple
from src.core.indicators import get_pivots

def detect_inverse_head_shoulders(df: pd.DataFrame) -> Tuple[bool, float]:
    """
    Detect Inverse Head and Shoulders pattern after a significant decline.
    Returns (is_pattern, confidence_score)
    """
    if len(df) < 80:
        return False, 0.0
    
    sub = df.copy()
    
    # Prior decline context
    recent_high = sub["high"].tail(120).max() if len(sub) >= 120 else sub["high"].max()
    recent_low = sub["low"].tail(80).min()
    decline = (recent_high - recent_low) / recent_high if recent_high > 0 else 0
    
    if decline < 0.12:
        return False, 0.0
    
    # Find valleys (shoulders and head)
    _, valleys = get_pivots(sub["low"], deviation=0.04, order=3)
    
    if len(valleys) < 3:
        return False, 0.0
    
    # Need 3 consecutive valleys: left shoulder, head, right shoulder
    for i in range(len(valleys) - 2):
        ls_idx = valleys.index[i]
        ls_price = valleys.iloc[i]
        
        head_idx = valleys.index[i + 1]
        head_price = valleys.iloc[i + 1]
        
        rs_idx = valleys.index[i + 2]
        rs_price = valleys.iloc[i + 2]
        
        # Head must be deepest
        if not (head_price < ls_price and head_price < rs_price):
            continue
        
        # Shoulders alignment
        shoulder_diff = abs(ls_price - rs_price) / ((ls_price + rs_price) / 2)
        if shoulder_diff > 0.10:
            continue
        
        # Head depth
        head_depth_vs_shoulder = min(ls_price, rs_price) - head_price
        head_depth_pct = head_depth_vs_shoulder / min(ls_price, rs_price)
        if head_depth_pct < 0.03:
            continue
        
        # Time symmetry
        loc_ls = sub.index.get_loc(ls_idx)
        loc_head = sub.index.get_loc(head_idx)
        loc_rs = sub.index.get_loc(rs_idx)
        # Handle duplicate indices gracefully
        for loc in [loc_ls, loc_head, loc_rs]:
            if isinstance(loc, slice):
                loc = loc.start if loc.start is not None else loc.stop
        if not all(isinstance(loc, (int, np.integer)) for loc in [loc_ls, loc_head, loc_rs]):
            continue
        t1 = int(loc_head) - int(loc_ls)
        t2 = int(loc_rs) - int(loc_head)
        if t1 == 0 or t2 == 0:
            continue
        time_symmetry = abs(t1 - t2) / ((t1 + t2) / 2)
        if time_symmetry > 0.60:
            continue
        
        # Neckline: peaks between shoulders and head
        between_ls_head = sub[(sub.index > ls_idx) & (sub.index < head_idx)]
        between_head_rs = sub[(sub.index > head_idx) & (sub.index < rs_idx)]
        
        if len(between_ls_head) == 0 or len(between_head_rs) == 0:
            continue
        
        peak1 = between_ls_head["high"].max()
        peak2 = between_head_rs["high"].max()
        
        # Neckline should be horizontal or ascending
        if peak2 < peak1 * 0.95:
            continue
        
        # Volume analysis
        ls_vol = sub.loc[ls_idx, "volume"] if ls_idx in sub.index else 0
        head_vol = sub.loc[head_idx, "volume"] if head_idx in sub.index else 0
        rs_vol = sub.loc[rs_idx, "volume"] if rs_idx in sub.index else 0
        
        # Score
        score = 0.4
        if shoulder_diff < 0.05:
            score += 0.15
        if head_depth_pct > 0.05:
            score += 0.10
        if time_symmetry < 0.30:
            score += 0.15
        if head_vol < ls_vol:
            score += 0.10
        if rs_vol < ls_vol:
            score += 0.10
        
        return True, min(score, 1.0)
    
    return False, 0.0
