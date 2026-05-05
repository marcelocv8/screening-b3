#!/usr/bin/env python3
"""
Main screening runner.
Executes full pipeline: fetch data, calculate indicators, detect patterns, score, save results.
"""
import os
import sys
import json
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Ensure src is in path
sys.path.insert(0, str(Path(__file__).parent))

from src.data.brapi_client import BrapiClient
from src.data.yfinance_client import YFinanceClient
from src.data.fundamentus_client import FundamentusClient
from src.data.bdr_mapper import BDRMapper
from src.core.indicators import calculate_ma, relative_strength, calculate_volume_metrics
from src.core.scorer import score_stock

# Constants
RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)
MIN_LIQUIDEZ = 50000  # R$

def is_business_day() -> bool:
    """Check if today is a business day (Mon-Fri) and not a known Brazilian holiday (simplified)."""
    today = datetime.now()
    if today.weekday() >= 5:  # Sat or Sun
        return False
    return True

def fetch_universe(brapi: BrapiClient, yf_client: YFinanceClient, bdr_mapper: BDRMapper) -> pd.DataFrame:
    """Fetch full universe: Brazilian stocks + ETFs + BDRs mapped to underlyings."""
    print("[1/6] Fetching universe from Brapi...")
    
    universe = []
    
    # Stocks
    stocks = brapi.list_stocks(type_="stock")
    if not stocks.empty:
        stocks["category"] = "BR_STOCK"
        stocks["analysis_ticker"] = stocks["stock"]
        universe.append(stocks[["stock", "name", "category", "analysis_ticker"]])
    
    # ETFs
    etfs = brapi.list_stocks(type_="fund")
    if not etfs.empty:
        etfs["category"] = "ETF"
        etfs["analysis_ticker"] = etfs["stock"]
        universe.append(etfs[["stock", "name", "category", "analysis_ticker"]])
    
    # BDRs -> map to underlyings
    bdrs = brapi.list_stocks(type_="bdr")
    if not bdrs.empty:
        bdrs["category"] = "BDR"
        bdrs["underlying"] = bdrs["stock"].apply(lambda x: bdr_mapper.get_underlying(x))
        # Drop BDRs where we couldn't determine the underlying
        mapped_count = bdrs["underlying"].notna().sum()
        skipped_count = bdrs["underlying"].isna().sum()
        bdrs = bdrs[bdrs["underlying"].notna()].copy()
        bdrs["analysis_ticker"] = bdrs["underlying"]
        universe.append(bdrs[["stock", "name", "category", "analysis_ticker"]])
        print(f"[1/6] BDRs mapped: {mapped_count} | skipped (no underlying): {skipped_count}")
    
    df = pd.concat(universe, ignore_index=True)
    df = df.drop_duplicates(subset=["stock"])
    print(f"[1/6] Universe size: {len(df)} tickers")
    return df

def fetch_data_for_ticker(row: pd.Series, yf_client: YFinanceClient, brapi: BrapiClient,
                          fundamentus_data: pd.DataFrame = None, bdr_mapper: BDRMapper = None) -> dict:
    """Fetch daily and weekly data for a single ticker."""
    ticker = row["stock"]
    analysis_ticker = row["analysis_ticker"]
    category = row["category"]
    
    result = {
        "ticker": ticker,
        "name": row.get("name", ticker),
        "category": category,
        "analysis_ticker": analysis_ticker,
        "bdr_display": None,
        "daily": pd.DataFrame(),
        "weekly": pd.DataFrame(),
        "fundamentals": {},
        "volume_financeiro": 0,
        "error": None
    }
    
    # For BDRs, analysis is on underlying but display BDR ticker
    if category == "BDR":
        result["bdr_display"] = f"{analysis_ticker} (BDR: {ticker})"
        yf_symbol = analysis_ticker
    else:
        yf_symbol = f"{ticker}.SA"
    
    try:
        # Daily data from yfinance
        df_daily = yf_client.get_history(yf_symbol, period="2y", interval="1d")
        if df_daily.empty:
            # Fallback: try without .SA for some edge cases
            if category != "BDR":
                df_daily = yf_client.get_history(ticker, period="2y", interval="1d")
        
        if df_daily.empty or len(df_daily) < 50:
            result["error"] = "Insufficient daily data"
            return result
        
        # Calculate indicators
        df_daily = calculate_ma(df_daily, [50, 150, 200])
        
        # Weekly data
        df_weekly = yf_client.get_history(yf_symbol, period="3y", interval="1wk")
        if df_weekly.empty and category != "BDR":
            df_weekly = yf_client.get_history(ticker, period="3y", interval="1wk")
        
        # Volume financeiro estimation
        last_close = df_daily.iloc[-1]["close"]
        last_volume = df_daily.iloc[-1]["volume"]
        result["volume_financeiro"] = last_close * last_volume
        
        # Fetch fundamentals for Brazilian stocks
        if category in ("BR_STOCK", "ETF") and fundamentus_data is not None:
            row_fund = fundamentus_data[fundamentus_data["ticker"] == ticker]
            if not row_fund.empty:
                r = row_fund.iloc[0]
                result["fundamentals"] = {
                    "revenue_growth_yoy": _parse_pct(r.get("cresc_rec_5a", "0")),
                    "profit_growth_yoy": 0,
                    "roe": _parse_pct(r.get("roe", "0")),
                    "div_bruta_patrim": _parse_float(r.get("div_bruta_patrim", "99")),
                    "pl": _parse_float(r.get("pl", "0")),
                    "pvp": _parse_float(r.get("pvp", "0")),
                    "mrg_liq": _parse_pct(r.get("mrg_liq", "0")),
                    "liq_corrente": _parse_float(r.get("liq_corrente", "0")),
                }
        elif category == "BDR":
            # Use yfinance info for US fundamentals
            info = yf_client.get_info(yf_symbol)
            if info:
                result["fundamentals"] = {
                    "revenue_growth_yoy": info.get("revenueGrowth", 0) * 100 if info.get("revenueGrowth") else 0,
                    "profit_growth_yoy": info.get("earningsGrowth", 0) * 100 if info.get("earningsGrowth") else 0,
                    "roe": info.get("returnOnEquity", 0) * 100 if info.get("returnOnEquity") else 0,
                    "div_bruta_patrim": 0,  # Not easily available
                }
        
        result["daily"] = df_daily
        result["weekly"] = df_weekly
        
    except Exception as e:
        result["error"] = str(e)
    
    return result

def _parse_pct(val) -> float:
    if pd.isna(val):
        return 0.0
    s = str(val).replace("%", "").replace(",", ".").strip()
    try:
        return float(s)
    except:
        return 0.0

def _parse_float(val) -> float:
    if pd.isna(val):
        return 0.0
    s = str(val).replace(",", ".").replace("%", "").strip()
    try:
        return float(s)
    except:
        return 0.0

def run_screening():
    print("=" * 60)
    print(f" Screening B3 - Minervini + Patterns")
    print(f" Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    if not is_business_day():
        print("Today is not a business day. Skipping.")
        return
    
    brapi = BrapiClient()
    yf_client = YFinanceClient(delay=0.3)
    fclient = FundamentusClient()
    bdr_mapper = BDRMapper()
    
    universe = fetch_universe(brapi, yf_client, bdr_mapper)
    if universe.empty:
        print("No tickers found. Exiting.")
        return
    
    # Fetch IBOV for relative strength
    print("[2/6] Fetching IBOV benchmark...")
    ibov = yf_client.get_history("^BVSP", period="2y", interval="1d")
    ibov_weekly = yf_client.get_history("^BVSP", period="3y", interval="1wk")
    
    # Pre-fetch Fundamentus data once
    print("[2.5/6] Fetching Fundamentus data...")
    fundamentus_data = fclient.get_stock_list()
    
    # Process tickers
    print("[3/6] Processing tickers...")
    results = []
    total = len(universe)
    
    for idx, row in universe.iterrows():
        if idx % 50 == 0:
            print(f"  Progress: {idx}/{total} ({idx/total*100:.1f}%)")
        
        try:
            data = fetch_data_for_ticker(row, yf_client, brapi, fundamentus_data, bdr_mapper)
            
            if data["error"] or data["daily"].empty:
                continue
            
            # Liquidity filter
            if data["volume_financeiro"] < MIN_LIQUIDEZ:
                continue
            
            # Calculate relative strength vs IBOV
            if not ibov.empty:
                data["daily"]["rs_ratio"] = relative_strength(
                    data["daily"]["close"], ibov["close"]
                )
            
            # Score
            score_result = score_stock(
                data["daily"],
                data["weekly"],
                fundamentals=data["fundamentals"],
                avg_volume_financeiro=data["volume_financeiro"]
            )
            
            # Build result record
            last = data["daily"].iloc[-1]
            record = {
                "ticker": data["ticker"],
                "name": data["name"],
                "category": data["category"],
                "display": data["bdr_display"] or data["ticker"],
                "score": score_result["score"],
                "tier": score_result["tier"],
                "price": round(last["close"], 2),
                "sma50": round(last.get("sma50", 0), 2),
                "sma150": round(last.get("sma150", 0), 2),
                "sma200": round(last.get("sma200", 0), 2),
                "volume_financeiro": round(data["volume_financeiro"], 2),
                "vcp": score_result["patterns"].get("vcp", {}).get("detected", False),
                "vcp_conf": score_result["patterns"].get("vcp", {}).get("confidence", 0),
                "wedge": score_result["patterns"].get("wedge", {}).get("detected", False),
                "wedge_conf": score_result["patterns"].get("wedge", {}).get("confidence", 0),
                "cup_handle": score_result["patterns"].get("cup_handle", {}).get("detected", False),
                "cup_handle_conf": score_result["patterns"].get("cup_handle", {}).get("confidence", 0),
                "double_bottom": score_result["patterns"].get("double_bottom", {}).get("detected", False),
                "double_bottom_conf": score_result["patterns"].get("double_bottom", {}).get("confidence", 0),
                "inverse_hs": score_result["patterns"].get("inverse_hs", {}).get("detected", False),
                "inverse_hs_conf": score_result["patterns"].get("inverse_hs", {}).get("confidence", 0),
                "pre_breakout": score_result["patterns"].get("pre_breakout", False),
                "breakout": score_result["breakout"],
                "breakout_vol_ratio": score_result["breakout_details"].get("volume_ratio", 0),
                "breakout_resistance": score_result["breakout_details"].get("resistance_level", 0),
                "roe": data["fundamentals"].get("roe", 0),
                "pl": data["fundamentals"].get("pl", 0),
                "pvp": data["fundamentals"].get("pvp", 0),
            }
            results.append(record)
        except Exception as e:
            print(f"  [SKIP] {row.get('stock', '?')}: {type(e).__name__}: {e}")
            continue
    
    print("[4/6] Building ranking...")
    df_results = pd.DataFrame(results)
    if df_results.empty:
        print("No results after processing. Exiting.")
        return
    
    df_results = df_results.sort_values("score", ascending=False).reset_index(drop=True)
    df_results["rank"] = df_results.index + 1
    
    # Save results
    today_str = datetime.now().strftime("%Y-%m-%d")
    print("[5/6] Saving results...")
    
    # JSON with full details
    json_path = RESULTS_DIR / f"screening_{today_str}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    # Parquet for fast loading in Streamlit
    parquet_path = RESULTS_DIR / "latest.parquet"
    df_results.to_parquet(parquet_path, index=False)
    
    # CSV for easy viewing
    csv_path = RESULTS_DIR / f"screening_{today_str}.csv"
    df_results.to_csv(csv_path, index=False, encoding="utf-8-sig")
    
    # Summary
    summary = {
        "date": today_str,
        "total_processed": total,
        "qualified": len(df_results),
        "tier_s": int((df_results["tier"] == "S").sum()),
        "tier_a": int((df_results["tier"] == "A").sum()),
        "tier_b": int((df_results["tier"] == "B").sum()),
        "breakouts": int(df_results["breakout"].sum()),
        "vcps": int(df_results["vcp"].sum()),
        "wedges": int(df_results["wedge"].sum()),
    }
    
    summary_path = RESULTS_DIR / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    
    print("[6/6] Done!")
    print(f"  Results: {len(df_results)} qualified stocks")
    print(f"  Tier S: {summary['tier_s']} | Tier A: {summary['tier_a']} | Tier B: {summary['tier_b']}")
    print(f"  Breakouts today: {summary['breakouts']}")
    print(f"  VCPs: {summary['vcps']} | Wedges: {summary['wedges']}")
    print(f"  Files saved to: {RESULTS_DIR}")

if __name__ == "__main__":
    run_screening()
