import pandas as pd
import numpy as np
from typing import Tuple
from src.core.indicators import get_pivots

def detect_vcp(df: pd.DataFrame, lookback: int = 60, min_contractions: int = 4) -> Tuple[bool, float]:
    """
    Detect Volatility Contraction Pattern (Minervini style) - ENHANCED VERSION.
    
    Rules (STRICTER on volume + price range):
    - At least 4 consecutive contractions of range (high-low)
    - Each contraction range < 90% of the previous one
    - Volume in last contraction < 60% of the first one (MANDATORY gate)
    - Price range (high-low) of last contraction < 70% of first (MANDATORY band shrink)
    - Price above SMA20 and SMA20 > SMA50 (momentum preserved)
    - Price within 10% of 52-week high (consolidation near highs)
    - Prefer ascending contractions (each bottom higher than previous)
    
    Returns (is_vcp, confidence_score)
    """
    if len(df) < lookback + 20:
        return False, 0.0
    
    sub = df.tail(lookback).copy()
    sub["range"] = sub["high"] - sub["low"]
    
    # Need SMAs
    if "sma20" not in sub.columns:
        sub["sma20"] = sub["close"].rolling(20).mean()
    if "sma50" not in sub.columns:
        sub["sma50"] = sub["close"].rolling(50).mean()
    
    last = sub.iloc[-1]
    
    # Context check: price must be near 52-week high
    high_52w = df["high"].tail(252).max() if len(df) >= 252 else df["high"].max()
    if last["close"] < high_52w * 0.90:
        return False, 0.0
    
    # Momentum check: SMA20 > SMA50 and price > SMA20
    if not (last["sma20"] > last["sma50"] and last["close"] > last["sma20"]):
        return False, 0.0
    
    # Get pivots for segmentation
    smooth_high = sub["high"].rolling(3, center=True).mean().fillna(sub["high"])
    smooth_low = sub["low"].rolling(3, center=True).mean().fillna(sub["low"])
    
    peaks, _ = get_pivots(smooth_high, deviation=0.03, order=3)
    _, valleys_low = get_pivots(smooth_low, deviation=0.03, order=3)
    
    if len(peaks) < 2 or len(valleys_low) < 2:
        # Fallback: use fixed window segmentation
        return _detect_vcp_fixed_window(sub, min_contractions)
    
    # Pivot-based segmentation
    all_pivots = sorted(list(peaks.index) + list(valleys_low.index))
    if len(all_pivots) < min_contractions + 1:
        return False, 0.0
    
    ranges = []
    volumes = []
    lows = []
    for i in range(len(all_pivots)-1):
        mask = (sub.index >= all_pivots[i]) & (sub.index <= all_pivots[i+1])
        segment = sub.loc[mask]
        if len(segment) > 0:
            ranges.append(segment["range"].mean())
            volumes.append(segment["volume"].mean())
            lows.append(segment["low"].min())
    
    if len(ranges) < min_contractions:
        return False, 0.0
    
    # STRICT: Check consecutive contractions (each < 90% of previous)
    contraction_streak = 1
    best_streak = 1
    streak_start = 0
    best_start = 0
    for i in range(len(ranges)-1):
        if ranges[i+1] < ranges[i] * 0.90:  # Strict: must shrink by at least 10%
            if contraction_streak == 1:
                streak_start = i
            contraction_streak += 1
            if contraction_streak > best_streak:
                best_streak = contraction_streak
                best_start = streak_start
        else:
            contraction_streak = 1
    
    if best_streak < min_contractions:
        return False, 0.0
    
    # Get the best streak segments
    streak_ranges = ranges[best_start:best_start+best_streak]
    streak_volumes = volumes[best_start:best_start+best_streak]
    streak_lows = lows[best_start:best_start+best_streak]
    
    # MANDATORY volume gate: last contraction volume < 60% of first
    vol_ok = streak_volumes[-1] < streak_volumes[0] * 0.60 if len(streak_volumes) >= 2 else False
    if not vol_ok:
        return False, 0.0
    
    # MANDATORY price band gate: last range < 70% of first
    band_ok = streak_ranges[-1] < streak_ranges[0] * 0.70 if len(streak_ranges) >= 2 else False
    if not band_ok:
        return False, 0.0
    
    # Ascending bottoms check (each low higher than previous = constructive)
    ascending_lows = all(streak_lows[i+1] > streak_lows[i] for i in range(len(streak_lows)-1))
    
    # Score confidence
    score = 0.4  # Base for passing all mandatory gates
    
    # More contractions = higher confidence
    score += min((best_streak - min_contractions) * 0.15, 0.3)
    
    # Ascending lows (very constructive)
    if ascending_lows:
        score += 0.15
    
    # Near 52w high (closer = better)
    proximity_to_high = last["close"] / high_52w
    score += (proximity_to_high - 0.90) * 0.5  # Up to +0.05
    
    # Strong momentum
    if last["close"] > last.get("sma50", 0) * 1.10:
        score += 0.1
    
    return True, min(score, 1.0)


def _detect_vcp_fixed_window(sub: pd.DataFrame, min_contractions: int) -> Tuple[bool, float]:
    """Fallback VCP detection using fixed windows."""
    window_size = len(sub) // (min_contractions + 2)
    if window_size < 5:
        return False, 0.0
    
    ranges = []
    volumes = []
    lows = []
    for i in range(min_contractions + 2):
        start = i * window_size
        end = (i + 1) * window_size if i < min_contractions + 1 else len(sub)
        segment = sub.iloc[start:end]
        ranges.append(segment["range"].mean())
        volumes.append(segment["volume"].mean())
        lows.append(segment["low"].min())
    
    # Check strict consecutive contractions
    contraction_streak = 1
    best_streak = 1
    streak_start = 0
    best_start = 0
    for i in range(len(ranges)-1):
        if ranges[i+1] < ranges[i] * 0.90:
            if contraction_streak == 1:
                streak_start = i
            contraction_streak += 1
            if contraction_streak > best_streak:
                best_streak = contraction_streak
                best_start = streak_start
        else:
            contraction_streak = 1
    
    if best_streak < min_contractions:
        return False, 0.0
    
    streak_ranges = ranges[best_start:best_start+best_streak]
    streak_volumes = volumes[best_start:best_start+best_streak]
    streak_lows = lows[best_start:best_start+best_streak]
    
    # MANDATORY volume gate
    vol_ok = streak_volumes[-1] < streak_volumes[0] * 0.60 if len(streak_volumes) >= 2 else False
    if not vol_ok:
        return False, 0.0
    
    # MANDATORY band gate
    band_ok = streak_ranges[-1] < streak_ranges[0] * 0.70 if len(streak_ranges) >= 2 else False
    if not band_ok:
        return False, 0.0
    
    ascending_lows = all(streak_lows[i+1] > streak_lows[i] for i in range(len(streak_lows)-1))
    
    score = 0.4
    score += min((best_streak - min_contractions) * 0.15, 0.3)
    if ascending_lows:
        score += 0.15
    
    return True, min(score, 1.0)
