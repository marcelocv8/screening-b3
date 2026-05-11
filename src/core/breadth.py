"""
Market Breadth calculation for Brazil + USA.
Generates an allocation score (1-5) based on market conditions.
Includes historical signal tracking.
"""
import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, Any

RESULTS_DIR = Path("results")
HISTORY_PATH = RESULTS_DIR / "signal_history.json"

def _load_signal_history() -> list:
    """Load historical signal counts for averaging."""
    if HISTORY_PATH.exists():
        try:
            with open(HISTORY_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []

def _save_signal_history(history: list):
    """Save signal history, keeping last 30 days."""
    HISTORY_PATH.parent.mkdir(exist_ok=True)
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history[-30:], f, indent=2)

def calculate_breadth_indicators(br_df: pd.DataFrame, us_df: pd.DataFrame = None, date_str: str = None) -> Dict[str, Any]:
    """
    Calculate market breadth indicators.
    
    Args:
        br_df: DataFrame with Brazilian stock screening results
        us_df: Optional DataFrame with US stock data
        date_str: Current date string for history tracking
    
    Returns:
        dict with breadth metrics and allocation score (1-5)
    """
    if br_df is None or br_df.empty:
        return _default_breadth()
    
    total = len(br_df)
    if total == 0:
        return _default_breadth()
    
    # Brazilian indicators
    above_sma50 = (br_df["price"] > br_df["sma50"]).sum() if "sma50" in br_df.columns else 0
    above_sma200 = (br_df["price"] > br_df["sma200"]).sum() if "sma200" in br_df.columns else 0
    
    pct_above_sma50 = above_sma50 / total * 100
    pct_above_sma200 = above_sma200 / total * 100
    
    # New highs vs lows (using 20-day window approximation)
    new_highs = br_df["breakout"].sum() if "breakout" in br_df.columns else 0
    
    # Tier distribution
    tier_s = (br_df["technical_tier"] == "S").sum() if "technical_tier" in br_df.columns else 0
    tier_a = (br_df["technical_tier"] == "A").sum() if "technical_tier" in br_df.columns else 0
    
    # Count patterns as proxy for momentum
    wot_count = int(br_df["wedge_or_trend"].sum()) if "wedge_or_trend" in br_df.columns else 0
    rasb_count = int(br_df["rasb_trend"].sum()) if "rasb_trend" in br_df.columns else 0
    vcp_count = int(br_df["vcp"].sum()) if "vcp" in br_df.columns else 0
    wedge_count = int(br_df["wedge"].sum()) if "wedge" in br_df.columns else 0
    cup_count = int(br_df["cup_handle"].sum()) if "cup_handle" in br_df.columns else 0
    prebo_count = int(br_df["pre_breakout"].sum()) if "pre_breakout" in br_df.columns else 0
    breakout_count = int(br_df["breakout"].sum()) if "breakout" in br_df.columns else 0
    
    total_signals = wot_count + rasb_count + vcp_count + wedge_count + cup_count + prebo_count + breakout_count
    
    # Historical average
    history = _load_signal_history()
    avg_signals = np.mean([h["total_signals"] for h in history]) if history else total_signals
    signal_vs_avg = total_signals / avg_signals if avg_signals > 0 else 1.0
    
    # Update history
    if date_str:
        history.append({
            "date": date_str,
            "total_signals": int(total_signals),
            "wedge_or_trends": int(wot_count),
            "rasb_trends": int(rasb_count),
            "breakouts": int(breakout_count),
            "vcps": int(vcp_count),
        })
        _save_signal_history(history)
    
    # Scoring (each factor contributes to the final 1-5 score)
    score = 0
    details = {
        "pct_above_sma50": round(pct_above_sma50, 1),
        "pct_above_sma200": round(pct_above_sma200, 1),
        "tier_s": int(tier_s),
        "tier_a": int(tier_a),
        "new_highs": int(new_highs),
        "wedge_or_trend_count": int(wot_count),
        "rasb_trend_count": int(rasb_count),
        "vcp_count": int(vcp_count),
        "breakout_count": int(breakout_count),
        "total_signals": int(total_signals),
        "avg_signals": round(avg_signals, 1),
        "signal_vs_avg": round(signal_vs_avg, 2),
        "total_stocks": int(total),
    }
    
    # Factor 1: % above SMA50 (weight 25%)
    if pct_above_sma50 >= 70:
        score += 1.25
    elif pct_above_sma50 >= 50:
        score += 0.75
    elif pct_above_sma50 >= 30:
        score += 0.25
    
    # Factor 2: % above SMA200 (weight 15%)
    if pct_above_sma200 >= 65:
        score += 0.75
    elif pct_above_sma200 >= 45:
        score += 0.45
    elif pct_above_sma200 >= 25:
        score += 0.15
    
    # Factor 3: Quality of setups (S + A tier count) (weight 20%)
    quality_pct = (tier_s + tier_a) / total * 100 if total > 0 else 0
    if quality_pct >= 40:
        score += 1.0
    elif quality_pct >= 25:
        score += 0.6
    elif quality_pct >= 15:
        score += 0.2
    
    # Factor 4: Breakout momentum (weight 15%)
    breakout_pct = breakout_count / total * 100 if total > 0 else 0
    if breakout_pct >= 5:
        score += 0.75
    elif breakout_pct >= 2:
        score += 0.45
    elif breakout_pct >= 0.5:
        score += 0.15
    
    # Factor 5: Signals vs average (weight 15%)
    if signal_vs_avg >= 1.3:
        score += 0.75
    elif signal_vs_avg >= 1.0:
        score += 0.45
    elif signal_vs_avg >= 0.7:
        score += 0.15
    
    # Factor 6: New highs (weight 10%)
    if new_highs >= 20:
        score += 0.5
    elif new_highs >= 10:
        score += 0.3
    elif new_highs >= 3:
        score += 0.1
    
    # Convert to 1-5 scale
    allocation_score = max(1, min(5, round(score)))
    
    # Determine regime label
    regime_labels = {
        5: "Mercado Forte — Full Exposure",
        4: "Mercado Positivo — Moderado",
        3: "Neutro — Cauteloso",
        2: "Mercado Fraco — Defensivo",
        1: "Mercado em Queda — Cash/Hedge"
    }
    
    details["allocation_score"] = allocation_score
    details["regime"] = regime_labels.get(allocation_score, "Indefinido")
    details["allocation_pct"] = _allocation_pct(allocation_score)
    
    return details

def _default_breadth() -> Dict[str, Any]:
    return {
        "pct_above_sma50": 0.0,
        "pct_above_sma200": 0.0,
        "tier_s": 0,
        "tier_a": 0,
        "new_highs": 0,
        "vcp_count": 0,
        "breakout_count": 0,
        "total_signals": 0,
        "avg_signals": 0.0,
        "signal_vs_avg": 1.0,
        "total_stocks": 0,
        "allocation_score": 3,
        "regime": "Neutro — Cauteloso",
        "allocation_pct": "40-60%",
    }

def _allocation_pct(score: int) -> str:
    mapping = {5: "80-100%", 4: "60-80%", 3: "40-60%", 2: "20-40%", 1: "0-20%"}
    return mapping.get(score, "40-60%")
