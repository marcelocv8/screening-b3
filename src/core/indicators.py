import pandas as pd
import numpy as np
from typing import Tuple, Optional

def calculate_ma(df: pd.DataFrame, periods: list = [50, 150, 200], col: str = "close") -> pd.DataFrame:
    """Calculate simple moving averages and momentum returns."""
    # Ensure column is numeric before rolling (yfinance may return strings)
    df[col] = pd.to_numeric(df[col], errors="coerce")
    for p in periods:
        df[f"sma{p}"] = df[col].rolling(window=p, min_periods=p//2).mean()
    
    # Returns for wedge_or_trend strategy
    df["ret_63d"] = df[col].pct_change(63)
    df["ret_126d"] = df[col].pct_change(126)
    
    return df

def calculate_rsi(df: pd.DataFrame, period: int = 14, col: str = "close") -> pd.Series:
    """Calculate Relative Strength Index."""
    delta = df[col].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Average True Range."""
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(window=period, min_periods=period).mean()
    return atr

def relative_strength(prices: pd.Series, benchmark: pd.Series) -> pd.Series:
    """Calculate relative strength ratio (stock / benchmark)."""
    # Strip timezone to avoid mismatch (safely handle RangeIndex from _clean_df)
    prices_tz = prices.copy()
    benchmark_tz = benchmark.copy()
    if isinstance(prices_tz.index, pd.DatetimeIndex) and prices_tz.index.tz is not None:
        prices_tz.index = prices_tz.index.tz_localize(None)
    if isinstance(benchmark_tz.index, pd.DatetimeIndex) and benchmark_tz.index.tz is not None:
        benchmark_tz.index = benchmark_tz.index.tz_localize(None)
    return prices_tz / benchmark_tz.reindex(prices_tz.index).ffill()

def is_uptrend(df: pd.DataFrame) -> dict:
    """Check Minervini Trend Template conditions."""
    if len(df) < 200:
        return {k: False for k in [
            "price_above_sma50", "price_above_sma150", "price_above_sma200",
            "sma50_above_sma150", "sma150_above_sma200", "sma200_rising",
            "price_double_sma200", "near_52w_high", "rs_52w_high", "rs_uptrend_6m"
        ]}
    
    last = df.iloc[-1]
    
    # Ensure numeric values — cast EVERYTHING to float to avoid int/str comparison errors
    close = float(last["close"]) if pd.notna(last["close"]) else 0.0
    sma50 = float(last.get("sma50", 0)) if pd.notna(last.get("sma50", 0)) else 0.0
    sma150 = float(last.get("sma150", 0)) if pd.notna(last.get("sma150", 0)) else 0.0
    sma200 = float(last.get("sma200", 0)) if pd.notna(last.get("sma200", 0)) else 0.0
    high = float(last["high"]) if pd.notna(last["high"]) else 0.0
    
    # Basic conditions
    price_above_sma50 = close > sma50
    price_above_sma150 = close > sma150
    price_above_sma200 = close > sma200
    sma50_above_sma150 = sma50 > sma150
    sma150_above_sma200 = sma150 > sma200
    
    # SMA200 rising (last 20 days) — cast to float series
    if len(df) >= 220:
        sma200_series = pd.to_numeric(df["sma200"], errors="coerce")
        sma200_rising = float(sma200_series.iloc[-1]) > float(sma200_series.iloc[-20])
    else:
        sma200_rising = False
    
    # Price > 2x SMA200
    price_double_sma200 = close > 2 * sma200 if sma200 > 0 else False
    
    # Near 52-week high (within 10%)
    high_52w = float(df["high"].tail(252).max()) if len(df) >= 252 else float(df["high"].max())
    near_52w_high = close >= high_52w * 0.90
    
    # RS vs benchmark
    rs = df.get("rs_ratio", pd.Series(np.nan, index=df.index))
    rs_52w_high = False
    rs_uptrend_6m = False
    
    if not rs.isna().all():
        rs_last = float(last["rs_ratio"]) if pd.notna(last["rs_ratio"]) else 0.0
        rs_52w_high = rs_last >= float(rs.tail(252).max()) * 0.98 if len(rs) >= 252 else False
        rs_6m = rs.tail(126) if len(rs) >= 126 else rs
        if len(rs_6m) > 20:
            rs_uptrend_6m = float(rs_6m.iloc[-1]) > float(rs_6m.iloc[0])
    
    return {
        "price_above_sma50": price_above_sma50,
        "price_above_sma150": price_above_sma150,
        "price_above_sma200": price_above_sma200,
        "sma50_above_sma150": sma50_above_sma150,
        "sma150_above_sma200": sma150_above_sma200,
        "sma200_rising": sma200_rising,
        "price_double_sma200": price_double_sma200,
        "near_52w_high": near_52w_high,
        "rs_52w_high": rs_52w_high,
        "rs_uptrend_6m": rs_uptrend_6m,
    }

def calculate_volume_metrics(df: pd.DataFrame) -> dict:
    """Calculate volume metrics for breakout detection."""
    if len(df) < 20:
        return {"avg_volume_10d": 0, "avg_volume_20d": 0}
    
    last = df.iloc[-1]
    avg_volume_10d = df["volume"].tail(10).mean()
    avg_volume_20d = df["volume"].tail(20).mean()
    
    return {
        "volume_today": last["volume"],
        "avg_volume_10d": avg_volume_10d,
        "avg_volume_20d": avg_volume_20d,
        "volume_ratio_10d": last["volume"] / avg_volume_10d if avg_volume_10d > 0 else 0,
        "volume_ratio_20d": last["volume"] / avg_volume_20d if avg_volume_20d > 0 else 0,
    }

def get_pivots(series: pd.Series, deviation: float = 0.05, order: int = 3) -> Tuple[pd.Series, pd.Series]:
    """Simple local extrema based pivot detection using scipy if available, else rolling."""
    try:
        from scipy.signal import argrelextrema
        idx_max = argrelextrema(series.values, np.greater, order=order)[0]
        idx_min = argrelextrema(series.values, np.less, order=order)[0]
        peaks = pd.Series(series.values[idx_max], index=series.index[idx_max])
        valleys = pd.Series(series.values[idx_min], index=series.index[idx_min])
        return peaks, valleys
    except ImportError:
        # Fallback: rolling window approach
        rolling_max = series.rolling(window=2*order+1, center=True).max()
        rolling_min = series.rolling(window=2*order+1, center=True).min()
        peaks = series[(series == rolling_max) & (series.shift(1) < series) & (series.shift(-1) < series)].dropna()
        valleys = series[(series == rolling_min) & (series.shift(1) > series) & (series.shift(-1) > series)].dropna()
        return peaks, valleys
