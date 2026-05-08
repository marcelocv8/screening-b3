import pandas as pd
import numpy as np
from scipy.stats import linregress
from typing import Tuple, Optional
from src.core.indicators import get_pivots

def detect_cup_and_handle(df_weekly: pd.DataFrame) -> Tuple[bool, float]:
    """
    Detect Cup and Handle pattern (O'Neil style) on weekly data.
    Returns (is_pattern, confidence_score)
    """
    if len(df_weekly) < 30:
        return False, 0.0
    
    sub = df_weekly.copy()
    sub["range"] = sub["high"] - sub["low"]
    
    # Need at least 7 weeks for cup
    peaks, valleys = get_pivots(sub["high"], deviation=0.08, order=2)
    _, valleys_low = get_pivots(sub["low"], deviation=0.08, order=2)
    
    if len(peaks) < 2 or len(valleys_low) < 1:
        return False, 0.0
    
    # Look for left peak and subsequent valley (cup bottom)
    for i in range(len(peaks) - 1):
        left_peak_idx = peaks.index[i]
        left_peak_price = peaks.iloc[i]
        
        # Find valley after left peak
        candidate_valleys = valleys_low[valleys_low.index > left_peak_idx]
        if len(candidate_valleys) == 0:
            continue
        
        cup_bottom_idx = candidate_valleys.index[0]
        cup_bottom_price = candidate_valleys.iloc[0]
        
        # Cup duration
        cup_weeks = sub.index.get_loc(cup_bottom_idx) - sub.index.get_loc(left_peak_idx)
        if not (4 <= cup_weeks <= 65):
            continue
        
        # Cup depth
        depth = (left_peak_price - cup_bottom_price) / left_peak_price
        if not (0.10 <= depth <= 0.50):
            continue
        
        # Find right peak after cup bottom
        right_peaks = peaks[peaks.index > cup_bottom_idx]
        if len(right_peaks) == 0:
            continue
        
        right_peak_idx = right_peaks.index[0]
        right_peak_price = right_peaks.iloc[0]
        
        # Right peak should be near left peak level
        peak_alignment = abs(right_peak_price - left_peak_price) / left_peak_price
        if peak_alignment > 0.10:
            continue
        
        # Check for handle (pullback after right peak)
        handle_data = sub[sub.index > right_peak_idx]
        if len(handle_data) < 1 or len(handle_data) > 6:
            # Handle should be 1-4 weeks
            continue
        
        if len(handle_data) > 0:
            handle_low = handle_data["low"].min()
            handle_pullback = (right_peak_price - handle_low) / right_peak_price
            if not (0.03 <= handle_pullback <= 0.15):
                continue
        
        # Context: prior uptrend
        prior_data = sub[sub.index < left_peak_idx]
        if len(prior_data) > 10:
            prior_low = prior_data["low"].tail(20).min()
            if left_peak_price < prior_low * 1.20:
                continue  # Not enough prior uptrend
        
        # Score confidence
        score = 0.5
        if 0.15 <= depth <= 0.33:
            score += 0.2
        if peak_alignment < 0.05:
            score += 0.15
        if len(handle_data) >= 2:
            score += 0.15
        
        return True, min(score, 1.0)
    
    return False, 0.0
