import pandas as pd
import numpy as np
from typing import Dict, Any
from src.core.indicators import is_uptrend, calculate_volume_metrics
from src.patterns.vcp import detect_vcp
from src.patterns.wedge import detect_wedge_momentum
from src.patterns.cup_handle import detect_cup_and_handle
from src.patterns.double_bottom import detect_double_bottom
from src.patterns.inverse_hs import detect_inverse_head_shoulders
from src.patterns.breakout import detect_breakout

# Weights configuration
WEIGHTS = {
    # Trend Template
    "price_above_sma50": 1.0,
    "price_above_sma150": 1.0,
    "price_above_sma200": 1.0,
    "sma50_above_sma150": 1.0,
    "sma150_above_sma200": 1.0,
    "sma200_rising": 1.0,
    "price_double_sma200": 0.5,
    "near_52w_high": 1.5,
    "rs_52w_high": 1.5,
    "rs_uptrend_6m": 1.0,
    "liquidity_ok": 0.5,
    # Patterns
    "vcp": 3.0,
    "wedge": 2.5,
    "cup_handle": 2.5,
    "double_bottom": 2.0,
    "inverse_hs": 2.0,
    "pre_breakout": 1.5,
    # Fundamentals
    "revenue_growth": 1.5,
    "profit_growth": 1.5,
    "margin_expansion": 1.0,
    "roe_good": 1.0,
    "low_debt": 0.5,
}

def classify_tier(score: float) -> str:
    if score >= 13.0:
        return "S"
    elif score >= 9.5:
        return "A"
    elif score >= 6.5:
        return "B"
    else:
        return "C"

def score_stock(df_daily: pd.DataFrame, df_weekly: pd.DataFrame,
                fundamentals: Dict[str, Any] = None,
                avg_volume_financeiro: float = 0) -> Dict[str, Any]:
    """
    Calculate full Minervini + patterns score for a single stock.
    """
    result = {
        "score": 0.0,
        "tier": "C",
        "breakout": False,
        "breakout_details": {},
        "patterns": {},
        "trend": {},
        "fundamentals": {}
    }
    
    if df_daily.empty or len(df_daily) < 50:
        return result
    
    score = 0.0
    
    # 1. Trend Template
    trend = is_uptrend(df_daily)
    result["trend"] = trend
    
    for key, val in trend.items():
        if val and key in WEIGHTS:
            score += WEIGHTS[key]
    
    # Liquidity check
    liquidity_ok = avg_volume_financeiro >= 50000
    if liquidity_ok:
        score += WEIGHTS["liquidity_ok"]
    result["trend"]["liquidity_ok"] = liquidity_ok
    
    # 2. Patterns on daily
    vcp_ok, vcp_score = detect_vcp(df_daily)
    wedge_ok, wedge_score = detect_wedge_momentum(df_daily)
    db_ok, db_score = detect_double_bottom(df_daily)
    ihs_ok, ihs_score = detect_inverse_head_shoulders(df_daily)
    
    if vcp_ok:
        score += WEIGHTS["vcp"] * vcp_score
    if wedge_ok:
        score += WEIGHTS["wedge"] * wedge_score
    if db_ok:
        score += WEIGHTS["double_bottom"] * db_score
    if ihs_ok:
        score += WEIGHTS["inverse_hs"] * ihs_score
    
    result["patterns"]["vcp"] = {"detected": vcp_ok, "confidence": round(vcp_score, 2)}
    result["patterns"]["wedge"] = {"detected": wedge_ok, "confidence": round(wedge_score, 2)}
    result["patterns"]["double_bottom"] = {"detected": db_ok, "confidence": round(db_score, 2)}
    result["patterns"]["inverse_hs"] = {"detected": ihs_ok, "confidence": round(ihs_score, 2)}
    
    # 3. Cup & Handle on weekly
    ch_ok, ch_score = False, 0.0
    if df_weekly is not None and len(df_weekly) >= 30:
        ch_ok, ch_score = detect_cup_and_handle(df_weekly)
        if ch_ok:
            score += WEIGHTS["cup_handle"] * ch_score
    result["patterns"]["cup_handle"] = {"detected": ch_ok, "confidence": round(ch_score, 2)}
    
    # 4. Breakout detection
    breakout_ok, vol_ratio, breakout_details = detect_breakout(df_daily)
    result["breakout"] = breakout_ok
    result["breakout_details"] = breakout_details
    
    # 5. Pre-breakout proximity (within 5% of recent high)
    recent_high = df_daily["high"].tail(20).max()
    last_close = df_daily.iloc[-1]["close"]
    if last_close >= recent_high * 0.95 and last_close < recent_high * 1.02:
        score += WEIGHTS["pre_breakout"]
        result["patterns"]["pre_breakout"] = True
    else:
        result["patterns"]["pre_breakout"] = False
    
    # 6. Fundamentals
    if fundamentals:
        rev_g = fundamentals.get("revenue_growth_yoy", 0)
        prof_g = fundamentals.get("profit_growth_yoy", 0)
        roe = fundamentals.get("roe", 0)
        debt = fundamentals.get("div_bruta_patrim", 999)
        
        if rev_g > 0:
            score += WEIGHTS["revenue_growth"]
        if prof_g > 0:
            score += WEIGHTS["profit_growth"]
        if roe > 15:
            score += WEIGHTS["roe_good"]
        if debt < 1.0:
            score += WEIGHTS["low_debt"]
        
        result["fundamentals"] = {
            "revenue_growth_yoy": rev_g,
            "profit_growth_yoy": prof_g,
            "roe": roe,
            "div_bruta_patrim": debt,
        }
    
    result["score"] = round(score, 2)
    result["tier"] = classify_tier(score)
    
    return result
