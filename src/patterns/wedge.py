import pandas as pd
import numpy as np
from scipy.stats import linregress
from typing import Tuple
from src.core.indicators import get_pivots

def detect_wedge_momentum(df: pd.DataFrame, lookback: int = 40) -> Tuple[bool, float]:
    """
    Detect Wedge Momentum pattern (Qullamaggie/Chew style).
    Tops descending + bottoms ascending (symmetric wedge) OR descending channel
    with preserved momentum (SMA20 > SMA50 or price > SMA50).
    
    ENHANCED: Volume check uses rolling 5-candle average for smoother detection.
    Returns (is_wedge, confidence_score)
    """
    if len(df) < lookback + 20:
        return False, 0.0
    
    sub = df.tail(lookback).copy()
    
    # Need SMAs calculated
    if "sma20" not in sub.columns or "sma50" not in sub.columns:
        sub["sma20"] = sub["close"].rolling(20).mean()
        sub["sma50"] = sub["close"].rolling(50).mean()
    
    # Detect pivots
    peaks, valleys = get_pivots(sub["high"], deviation=0.03, order=3)
    valleys_low, _ = get_pivots(sub["low"], deviation=0.03, order=3)
    
    if len(peaks) < 3 or len(valleys_low) < 3:
        return False, 0.0
    
    # Use last 3-4 pivots
    recent_peaks = peaks.tail(4)
    recent_valleys = valleys_low.tail(4)
    
    if len(recent_peaks) < 3 or len(recent_valleys) < 3:
        return False, 0.0
    
    # Fit trendlines
    x_peaks = np.arange(len(recent_peaks))
    x_valleys = np.arange(len(recent_valleys))
    
    slope_peaks, _, _, _, _ = linregress(x_peaks, recent_peaks.values)
    slope_valleys, _, _, _, _ = linregress(x_valleys, recent_valleys.values)
    
    # Check wedge conditions
    tops_descending = slope_peaks < -0.01
    
    wedge_symmetric = tops_descending and (slope_valleys > 0.01)
    descending_channel = tops_descending and (slope_valleys < 0) and (abs(slope_valleys) < abs(slope_peaks))
    
    is_wedge_shape = wedge_symmetric or descending_channel
    
    if not is_wedge_shape:
        return False, 0.0
    
    # Check momentum preserved
    last = sub.iloc[-1]
    momentum_ok = (last.get("sma20", 0) > last.get("sma50", 0)) or (last["close"] > last.get("sma50", 0))
    
    if not momentum_ok:
        return False, 0.0
    
    # ENHANCED: Volume check using rolling 5-candle averages
    # Compare last 5-candle avg vs prior 5-candle avg (smoother, less noisy)
    vol_ma5 = sub["volume"].rolling(5).mean()
    recent_vol_ma = vol_ma5.tail(5).mean()
    prior_vol_ma = vol_ma5.tail(15).head(10).mean()  # candles 6-15 from end
    
    vol_drying = False
    if prior_vol_ma > 0:
        vol_drying = recent_vol_ma < prior_vol_ma * 0.85
    
    # Confidence scoring (base score increased since volume is smoother)
    score = 0.4  # base for shape
    
    # Stronger wedge if converging
    if wedge_symmetric:
        score += 0.25
    
    # Pre-breakout proximity (price near apex)
    apex_price = (recent_peaks.iloc[-1] + recent_valleys.iloc[-1]) / 2
    price_proximity = 1 - abs(last["close"] - apex_price) / apex_price
    if price_proximity > 0.8:
        score += 0.15
    
    # Volume drying up (using 5-candle MA - higher weight since it's more reliable)
    if vol_drying:
        score += 0.20
    else:
        # Partial credit if volume is at least flat to slightly down
        if prior_vol_ma > 0 and recent_vol_ma < prior_vol_ma * 1.0:
            score += 0.05
    
    return True, min(score, 1.0)
