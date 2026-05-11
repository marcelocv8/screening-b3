import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple
from src.core.indicators import is_uptrend
from src.patterns.vcp import detect_vcp
from src.patterns.wedge import detect_wedge_momentum
from src.patterns.cup_handle import detect_cup_and_handle
from src.patterns.wedge_or_trend import detect_wedge_or_trend
from src.patterns.rasb_trend import detect_rasb_trend
from src.patterns.breakout import detect_breakout

# Weights for TECHNICAL score (max ~17 points)
TECH_WEIGHTS = {
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
    "vcp": 3.0,
    "wedge_or_trend": 4.0,
    "rasb_trend": 3.5,
    "wedge": 2.5,
    "cup_handle": 2.5,
    "pre_breakout": 1.5,
}

# Weights for FUNDAMENTAL score (max 5.0 points)
FUND_WEIGHTS = {
    "roe_good": 1.0,
    "pl_reasonable": 1.0,
    "revenue_growth": 1.0,
    "profit_growth": 1.0,
    "low_debt": 1.0,
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


def classify_fund_tag(score: float) -> str:
    if score >= 4.0:
        return "Forte"
    elif score >= 2.5:
        return "OK"
    else:
        return "Fraco"


def score_technical(df_daily: pd.DataFrame, df_weekly: pd.DataFrame,
                    avg_volume_financeiro: float = 0) -> Tuple[float, Dict]:
    """Calculate technical score and return detailed breakdown."""
    if df_daily.empty or len(df_daily) < 50:
        return 0.0, {}
    
    score = 0.0
    details = {}
    
    # 1. Trend Template
    trend = is_uptrend(df_daily)
    details["trend"] = trend
    
    for key, val in trend.items():
        if val and key in TECH_WEIGHTS:
            score += TECH_WEIGHTS[key]
    
    # Liquidity
    liquidity_ok = avg_volume_financeiro >= 50000
    if liquidity_ok:
        score += TECH_WEIGHTS["liquidity_ok"]
    details["trend"]["liquidity_ok"] = liquidity_ok
    
    # 2. Patterns on daily
    vcp_ok, vcp_score = detect_vcp(df_daily)
    wedge_or_trend_ok, wedge_or_trend_score = detect_wedge_or_trend(df_daily)
    rasb_trend_ok, rasb_trend_score = detect_rasb_trend(df_daily)
    wedge_ok, wedge_score = detect_wedge_momentum(df_daily)
    
    if vcp_ok:
        score += TECH_WEIGHTS["vcp"] * vcp_score
    if wedge_or_trend_ok:
        score += TECH_WEIGHTS["wedge_or_trend"] * wedge_or_trend_score
    if rasb_trend_ok:
        score += TECH_WEIGHTS["rasb_trend"] * rasb_trend_score
    if wedge_ok:
        score += TECH_WEIGHTS["wedge"] * wedge_score
    
    details["patterns"] = {
        "vcp": {"detected": vcp_ok, "confidence": round(vcp_score, 2)},
        "wedge_or_trend": {"detected": wedge_or_trend_ok, "confidence": round(wedge_or_trend_score, 2)},
        "rasb_trend": {"detected": rasb_trend_ok, "confidence": round(rasb_trend_score, 2)},
        "wedge": {"detected": wedge_ok, "confidence": round(wedge_score, 2)},
    }
    
    # 3. Cup & Handle on weekly
    ch_ok, ch_score = False, 0.0
    if df_weekly is not None and len(df_weekly) >= 30:
        ch_ok, ch_score = detect_cup_and_handle(df_weekly)
        if ch_ok:
            score += TECH_WEIGHTS["cup_handle"] * ch_score
    details["patterns"]["cup_handle"] = {"detected": ch_ok, "confidence": round(ch_score, 2)}
    
    # 4. Pre-breakout proximity
    recent_high = df_daily["high"].tail(20).max()
    last_close = df_daily.iloc[-1]["close"]
    if pd.notna(recent_high) and pd.notna(last_close) and last_close >= recent_high * 0.95 and last_close < recent_high * 1.02:
        score += TECH_WEIGHTS["pre_breakout"]
        details["patterns"]["pre_breakout"] = True
    else:
        details["patterns"]["pre_breakout"] = False
    
    # 5. Breakout
    breakout_ok, _, breakout_details = detect_breakout(df_daily)
    details["breakout"] = breakout_ok
    details["breakout_details"] = breakout_details
    
    return round(score, 2), details


def score_fundamental(fundamentals: Dict[str, Any]) -> Tuple[float, Dict]:
    """Calculate fundamental score (0-5) and return breakdown."""
    if not fundamentals:
        return 0.0, {}
    
    score = 0.0
    details = {}
    
    # ROE > 15%
    roe = fundamentals.get("roe", 0)
    if roe > 15:
        score += FUND_WEIGHTS["roe_good"]
        details["roe_good"] = True
    else:
        details["roe_good"] = False
    details["roe_value"] = roe
    
    # P/L < 20 (not expensive)
    pl = fundamentals.get("pl", 999)
    if 0 < pl < 20:
        score += FUND_WEIGHTS["pl_reasonable"]
        details["pl_reasonable"] = True
    else:
        details["pl_reasonable"] = False
    details["pl_value"] = pl
    
    # Revenue growth > 0
    rev_g = fundamentals.get("revenue_growth_yoy", 0)
    if rev_g > 0:
        score += FUND_WEIGHTS["revenue_growth"]
        details["revenue_growth"] = True
    else:
        details["revenue_growth"] = False
    details["revenue_growth_value"] = rev_g
    
    # Profit growth > 0
    prof_g = fundamentals.get("profit_growth_yoy", 0)
    if prof_g > 0:
        score += FUND_WEIGHTS["profit_growth"]
        details["profit_growth"] = True
    else:
        details["profit_growth"] = False
    details["profit_growth_value"] = prof_g
    
    # Low debt (DL/PL < 1 or not available)
    debt = fundamentals.get("div_bruta_patrim", 999)
    if debt < 1.0:
        score += FUND_WEIGHTS["low_debt"]
        details["low_debt"] = True
    else:
        details["low_debt"] = False
    details["debt_value"] = debt
    
    return round(score, 2), details


def score_stock(df_daily: pd.DataFrame, df_weekly: pd.DataFrame,
                fundamentals: Dict[str, Any] = None,
                avg_volume_financeiro: float = 0) -> Dict[str, Any]:
    """
    Calculate complete score with technical and fundamental separated.
    
    Returns:
        {
            "technical_score": float,
            "technical_tier": str,
            "fundamental_score": float,
            "fundamental_tag": str,
            "patterns": dict,
            "breakout": bool,
            "breakout_details": dict,
            "fundamentals_detail": dict,
        }
    """
    tech_score, tech_details = score_technical(df_daily, df_weekly, avg_volume_financeiro)
    fund_score, fund_details = score_fundamental(fundamentals or {})
    
    return {
        "technical_score": tech_score,
        "technical_tier": classify_tier(tech_score),
        "fundamental_score": fund_score,
        "fundamental_tag": classify_fund_tag(fund_score),
        "patterns": tech_details.get("patterns", {}),
        "trend": tech_details.get("trend", {}),
        "breakout": tech_details.get("breakout", False),
        "breakout_details": tech_details.get("breakout_details", {}),
        "fundamentals_detail": fund_details,
    }
