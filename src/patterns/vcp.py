import pandas as pd
import numpy as np
from typing import Tuple, Optional
from src.core.indicators import get_pivots

def detect_vcp(df: pd.DataFrame, lookback: int = 60, min_contractions: int = 3) -> Tuple[bool, float]:
    """
    Detect Volatility Contraction Pattern (Minervini style).
    Returns (is_vcp, confidence_score)
    """
    if len(df) < lookback + 10:
        return False, 0.0
    
    sub = df.tail(lookback).copy()
    sub["range"] = sub["high"] - sub["low"]
    
    # Get pivots for segmentation
    smooth_high = sub["high"].rolling(3, center=True).mean().fillna(sub["high"])
    smooth_low = sub["low"].rolling(3, center=True).mean().fillna(sub["low"])
    
    peaks, valleys = get_pivots(smooth_high, deviation=0.03, order=3)
    _, valleys_low = get_pivots(smooth_low, deviation=0.03, order=3)
    
    if len(peaks) < 2 or len(valleys_low) < 2:
        # Fallback: use rolling windows
        window_size = len(sub) // (min_contractions + 1)
        if window_size < 5:
            return False, 0.0
        
        ranges = []
        volumes = []
        for i in range(min_contractions + 1):
            start = i * window_size
            end = (i + 1) * window_size if i < min_contractions else len(sub)
            segment = sub.iloc[start:end]
            ranges.append(segment["range"].mean())
            volumes.append(segment["volume"].mean())
        
        # Check contraction
        contraction_count = sum(1 for i in range(len(ranges)-1) if ranges[i+1] < ranges[i])
        volume_falling = sum(1 for i in range(len(volumes)-1) if volumes[i+1] < volumes[i])
        
        if contraction_count >= min_contractions - 1 and volume_falling >= min_contractions - 2:
            score = (contraction_count / (len(ranges)-1)) * 0.5 + (volume_falling / max(len(volumes)-1, 1)) * 0.5
            # Check if price is above SMA50 for context
            if sub.iloc[-1]["close"] > sub.iloc[-1].get("sma50", 0):
                score += 0.2
            return True, min(score, 1.0)
        return False, 0.0
    
    # Pivot-based approach
    # Create segments between alternating pivots
    all_pivots = sorted(list(peaks.index) + list(valleys_low.index))
    if len(all_pivots) < min_contractions + 1:
        return False, 0.0
    
    ranges = []
    volumes = []
    for i in range(len(all_pivots)-1):
        mask = (sub.index >= all_pivots[i]) & (sub.index <= all_pivots[i+1])
        segment = sub.loc[mask]
        if len(segment) > 0:
            ranges.append(segment["range"].mean())
            volumes.append(segment["volume"].mean())
    
    if len(ranges) < min_contractions:
        return False, 0.0
    
    # Find consecutive contractions
    contraction_streaks = []
    current_streak = 1
    for i in range(len(ranges)-1):
        if ranges[i+1] < ranges[i] * 1.05:  # Allow 5% tolerance
            current_streak += 1
        else:
            contraction_streaks.append(current_streak)
            current_streak = 1
    contraction_streaks.append(current_streak)
    
    best_streak = max(contraction_streaks) if contraction_streaks else 0
    
    if best_streak >= min_contractions:
        # Calculate confidence
        score = min(best_streak / (min_contractions + 1), 1.0) * 0.6
        
        # Volume should be decreasing
        vol_trend = np.polyfit(range(len(volumes)), volumes, 1)[0] if len(volumes) > 1 else 0
        if vol_trend < 0:
            score += 0.2
        
        # Price context
        last_close = sub.iloc[-1]["close"]
        if last_close > sub.iloc[-1].get("sma50", 0):
            score += 0.2
        
        return True, min(score, 1.0)
    
    return False, 0.0
