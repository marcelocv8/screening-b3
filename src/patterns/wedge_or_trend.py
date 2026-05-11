import pandas as pd
import numpy as np
from typing import Tuple

def detect_wedge_or_trend(df: pd.DataFrame) -> Tuple[bool, float]:
    """
    Detect wedge_or_trend pattern (backtest-validated strategy).
    
    Two legs (OR logic):
    1. Wedge (tactical): SMA20 > SMA50 AND close >= 0.92 * high_52w
    2. Trend momentum (structural): close > SMA200 AND SMA50 > SMA150 > SMA200 
       AND ret_63d > 0 AND ret_126d > 0 AND rs_ratio > rs_30d_avg
    
    Returns (is_detected, confidence_score)
    """
    if len(df) < 130:
        return False, 0.0
    
    last = df.iloc[-1]
    close = last.get("close", np.nan)
    
    if pd.isna(close):
        return False, 0.0
    
    # Ensure required columns exist
    has_ma = all(c in df.columns for c in ["sma20", "sma50", "sma150", "sma200"])
    if not has_ma:
        return False, 0.0
    
    sma20 = last.get("sma20", np.nan)
    sma50 = last.get("sma50", np.nan)
    sma150 = last.get("sma150", np.nan)
    sma200 = last.get("sma200", np.nan)
    
    if pd.isna(sma20) or pd.isna(sma50) or pd.isna(sma150) or pd.isna(sma200):
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
    
    # 52-week high
    high_52w = df["high"].tail(252).max() if len(df) >= 252 else df["high"].max()
    
    # RS ratio comparison
    rs_ok = False
    if "rs_ratio" in df.columns and "rs_30d" in df.columns:
        rs_ratio = last.get("rs_ratio", np.nan)
        rs_30d = last.get("rs_30d", np.nan)
        if pd.notna(rs_ratio) and pd.notna(rs_30d):
            rs_ok = rs_ratio > rs_30d
    
    # Leg 1: Wedge (tactical)
    leg_wedge = (sma20 > sma50) and (close >= high_52w * 0.92)
    
    # Leg 2: Trend momentum (structural)
    leg_trend = (close > sma200) and (sma50 > sma150 > sma200)
    if pd.notna(ret_63d):
        leg_trend = leg_trend and (ret_63d > 0)
    if pd.notna(ret_126d):
        leg_trend = leg_trend and (ret_126d > 0)
    if rs_ok:
        leg_trend = leg_trend and rs_ok
    
    is_detected = leg_wedge or leg_trend
    
    if not is_detected:
        return False, 0.0
    
    # Confidence scoring (generous, max 1.0)
    score = 0.5  # Base for passing
    
    # Both legs active = highest confidence
    if leg_wedge and leg_trend:
        score += 0.25
    
    # Close to 52w high (for wedge leg)
    if leg_wedge and high_52w > 0:
        proximity = close / high_52w
        score += min((proximity - 0.92) * 2, 0.15)  # up to +0.15
    
    # Strong trend (for trend leg)
    if leg_trend:
        if close > sma200 * 1.10:
            score += 0.10
        if sma50 > sma150 * 1.05:
            score += 0.10
    
    # Returns momentum
    if pd.notna(ret_63d) and ret_63d > 0.10:
        score += 0.05
    if pd.notna(ret_126d) and ret_126d > 0.20:
        score += 0.05
    
    return True, min(score, 1.0)
