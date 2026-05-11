import pandas as pd
import numpy as np
from typing import Tuple

def detect_rasb_trend(df: pd.DataFrame) -> Tuple[bool, float]:
    """
    Detect RASB + Trend Momentum strategy (backtest-validated).
    
    Two legs (OR logic):
    1. RASB: close > sma50 AND close > high_10d[1] AND volume >= 1.2 * vol_20d AND rs_ratio > rs_30d
    2. Trend momentum: close > SMA200 AND SMA50 > SMA150 > SMA200 AND ret_63d > 0 AND ret_126d > 0
    
    Returns (is_detected, confidence_score)
    """
    if len(df) < 130:
        return False, 0.0
    
    last = df.iloc[-1]
    close = last.get("close", np.nan)
    
    if pd.isna(close):
        return False, 0.0
    
    # Ensure required MAs exist
    has_ma = all(c in df.columns for c in ["sma50", "sma150", "sma200"])
    if not has_ma:
        return False, 0.0
    
    sma50 = last.get("sma50", np.nan)
    sma150 = last.get("sma150", np.nan)
    sma200 = last.get("sma200", np.nan)
    
    if pd.isna(sma50) or pd.isna(sma150) or pd.isna(sma200):
        return False, 0.0
    
    # Calculate returns if not present
    if "ret_63d" in df.columns:
        ret_63d = last.get("ret_63d", np.nan)
    else:
        price_63d = df["close"].iloc[-63] if len(df) >= 63 else np.nan
        ret_63d = (close / price_63d - 1) if price_63d and price_63d > 0 else np.nan
    
    if "ret_126d" in df.columns:
        ret_126d = last.get("ret_126d", np.nan)
    else:
        price_126d = df["close"].iloc[-126] if len(df) >= 126 else np.nan
        ret_126d = (close / price_126d - 1) if price_126d and price_126d > 0 else np.nan
    
    # Leg 1: RASB
    leg_rasb = False
    if len(df) >= 11:
        high_10d = df["high"].iloc[-11:-1].max()  # high of last 10 days excluding today
        vol_20d = df["volume"].tail(20).mean()
        volume = last.get("volume", np.nan)
        
        rs_ok = False
        if "rs_ratio" in df.columns and "rs_30d" in df.columns:
            rs_ratio = last.get("rs_ratio", np.nan)
            rs_30d = last.get("rs_30d", np.nan)
            if pd.notna(rs_ratio) and pd.notna(rs_30d):
                rs_ok = rs_ratio > rs_30d
        
        price_ok = close > sma50 and close > high_10d
        vol_ok = pd.notna(volume) and pd.notna(vol_20d) and vol_20d > 0 and volume >= vol_20d * 1.2
        
        leg_rasb = price_ok and vol_ok and rs_ok
    
    # Leg 2: Trend momentum (same as wedge_or_trend)
    leg_trend = (close > sma200) and (sma50 > sma150 > sma200)
    if pd.notna(ret_63d):
        leg_trend = leg_trend and (ret_63d > 0)
    if pd.notna(ret_126d):
        leg_trend = leg_trend and (ret_126d > 0)
    
    is_detected = leg_rasb or leg_trend
    
    if not is_detected:
        return False, 0.0
    
    # Confidence scoring
    score = 0.5  # Base
    
    # Both legs active
    if leg_rasb and leg_trend:
        score += 0.25
    
    # Volume strength (for RASB leg)
    if leg_rasb and pd.notna(last.get("volume")) and pd.notna(df["volume"].tail(20).mean()):
        vol_ratio = last.get("volume") / df["volume"].tail(20).mean()
        if vol_ratio > 1.5:
            score += 0.10
        elif vol_ratio > 1.2:
            score += 0.05
    
    # Trend strength (for trend leg)
    if leg_trend:
        if close > sma200 * 1.10:
            score += 0.10
        if sma50 > sma150 * 1.05:
            score += 0.10
    
    # Returns momentum
    if pd.notna(ret_63d) and ret_63d > 0.15:
        score += 0.05
    if pd.notna(ret_126d) and ret_126d > 0.30:
        score += 0.05
    
    return True, min(score, 1.0)
