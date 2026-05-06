#!/usr/bin/env python3
"""
Main screening runner - OPTIMIZED VERSION.
Uses batch download, aggressive filtering, and preflight validation.
"""
import os
import sys
import json
import time
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.data.brapi_client import BrapiClient
from src.data.yfinance_client import YFinanceClient
from src.data.fundamentus_client import FundamentusClient
from src.data.cvm_client import CVMClient
from src.data.bdr_mapper import BDRMapper
from src.core.indicators import calculate_ma, relative_strength
from src.core.scorer import score_stock

RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)
MIN_LIQUIDEZ = 50000


def is_business_day() -> bool:
    today = datetime.now()
    return today.weekday() < 5


def is_fractional(ticker: str) -> bool:
    """Check if ticker is a fractional share (ends with F)."""
    return ticker.upper().endswith("F")


def fetch_universe(brapi: BrapiClient, bdr_mapper: BDRMapper) -> pd.DataFrame:
    """Fetch and aggressively filter universe."""
    print("[1/6] Fetching universe from Brapi...")
    
    universe = []
    
    # Stocks
    stocks = brapi.list_stocks(type_="stock")
    if not stocks.empty:
        stocks = stocks[~stocks["stock"].apply(is_fractional)]
        stocks["category"] = "BR_STOCK"
        stocks["analysis_ticker"] = stocks["stock"] + ".SA"
        universe.append(stocks[["stock", "name", "category", "analysis_ticker"]])
    
    # ETFs
    etfs = brapi.list_stocks(type_="fund")
    if not etfs.empty:
        etfs = etfs[~etfs["stock"].apply(is_fractional)]
        etfs["category"] = "ETF"
        etfs["analysis_ticker"] = etfs["stock"] + ".SA"
        universe.append(etfs[["stock", "name", "category", "analysis_ticker"]])
    
    # BDRs
    bdrs = brapi.list_stocks(type_="bdr")
    if not bdrs.empty:
        bdrs = bdrs[~bdrs["stock"].apply(is_fractional)]
        bdrs["category"] = "BDR"
        bdrs["underlying"] = bdrs["stock"].apply(lambda x: bdr_mapper.get_underlying(x))
        mapped = bdrs["underlying"].notna().sum()
        skipped = bdrs["underlying"].isna().sum()
        bdrs = bdrs[bdrs["underlying"].notna()].copy()
        bdrs["analysis_ticker"] = bdrs["underlying"]
        universe.append(bdrs[["stock", "name", "category", "analysis_ticker"]])
        print(f"[1/6] BDRs mapped: {mapped} | skipped: {skipped}")
    
    df = pd.concat(universe, ignore_index=True)
    df = df.drop_duplicates(subset=["stock"])
    print(f"[1/6] Universe after filtering: {len(df)} tickers")
    return df


def preflight_filter(universe: pd.DataFrame, yf_client: YFinanceClient) -> pd.DataFrame:
    """Quick validation: remove tickers that yfinance can't find at all."""
    print("[2/6] Preflight validation (quick existence check)...")
    
    # Sample tickers for validation - check first 10 from each category
    to_check = []
    for cat in ["BR_STOCK", "ETF", "BDR"]:
        sample = universe[universe["category"] == cat]["analysis_ticker"].head(10).tolist()
        to_check.extend(sample)
    
    # Actually, let's validate all BDRs since they're most likely to fail
    bdr_tickers = universe[universe["category"] == "BDR"]["analysis_ticker"].tolist()
    if bdr_tickers:
        print(f"  Validating {len(bdr_tickers)} BDRs...")
        valid_bdrs = yf_client.validate_tickers(bdr_tickers)
        invalid_bdrs = set(bdr_tickers) - set(valid_bdrs)
        if invalid_bdrs:
            print(f"  Removing {len(invalid_bdrs)} invalid BDRs: {list(invalid_bdrs)[:5]}...")
            universe = universe[~(
                (universe["category"] == "BDR") & 
                universe["analysis_ticker"].isin(invalid_bdrs)
            )]
    
    print(f"[2/6] Universe after preflight: {len(universe)} tickers")
    return universe


def process_batch(universe: pd.DataFrame, yf_client: YFinanceClient, 
                  ibov: pd.DataFrame, fundamentus_data: pd.DataFrame) -> list:
    """Process all tickers using batch downloads."""
    print("[3/6] Batch downloading data...")
    
    # Separate by category for appropriate handling
    br_tickers = universe[universe["category"].isin(["BR_STOCK", "ETF"])]["analysis_ticker"].tolist()
    bdr_df = universe[universe["category"] == "BDR"]
    
    # Download BR stocks/ETFs in batches
    br_data = yf_client.batch_download(br_tickers, period="2y", interval="1d")
    print(f"  Downloaded {len(br_data)} Brazilian tickers")
    
    # Download BDRs (underlyings) in batches
    bdr_tickers = bdr_df["analysis_ticker"].tolist()
    bdr_data = yf_client.batch_download(bdr_tickers, period="2y", interval="1d")
    print(f"  Downloaded {len(bdr_data)} BDR underlyings")
    
    # Download weekly data for patterns
    print("[3.5/6] Downloading weekly data for patterns...")
    br_weekly = yf_client.batch_download(br_tickers, period="3y", interval="1wk")
    bdr_weekly = yf_client.batch_download(bdr_tickers, period="3y", interval="1wk")
    
    # Process results
    print("[4/6] Processing and scoring...")
    results = []
    total = len(universe)
    
    for idx, row in universe.iterrows():
        if idx % 100 == 0:
            print(f"  Progress: {idx}/{total}")
        
        ticker = row["stock"]
        category = row["category"]
        analysis_ticker = row["analysis_ticker"]
        
        # Get data from batch results
        df_daily = br_data.get(analysis_ticker, pd.DataFrame()) if category != "BDR" else bdr_data.get(analysis_ticker, pd.DataFrame())
        df_weekly = br_weekly.get(analysis_ticker, pd.DataFrame()) if category != "BDR" else bdr_weekly.get(analysis_ticker, pd.DataFrame())
        
        if df_daily.empty or len(df_daily) < 50:
            continue
        
        # Calculate indicators
        df_daily = calculate_ma(df_daily, [50, 150, 200])
        
        # Volume financeiro
        last_close = df_daily.iloc[-1]["close"]
        last_volume = df_daily.iloc[-1]["volume"]
        volume_financeiro = last_close * last_volume
        
        if volume_financeiro < MIN_LIQUIDEZ:
            continue
        
        # RS vs IBOV
        if not ibov.empty:
            df_daily["rs_ratio"] = relative_strength(df_daily["close"], ibov["close"])
        
        # Fundamentals - Yahoo Finance as primary for ALL tickers
        fundamentals = {}
        yf_symbol = analysis_ticker if category == "BDR" else f"{ticker}.SA"
        info = yf_client.get_info(yf_symbol)
        if info:
            fundamentals = {
                "revenue_growth_yoy": info.get("revenueGrowth", 0) * 100 if info.get("revenueGrowth") else 0,
                "profit_growth_yoy": info.get("earningsGrowth", 0) * 100 if info.get("earningsGrowth") else 0,
                "roe": info.get("returnOnEquity", 0) * 100 if info.get("returnOnEquity") else 0,
                "pl": info.get("trailingPE", 999),
                "pvp": info.get("priceToBook", 999),
                "div_bruta_patrim": info.get("debtToEquity", 999) / 100 if info.get("debtToEquity") else 999,
            }
        
        # Fallback: Fundamentus for Brazilian stocks only
        if not fundamentals and category in ("BR_STOCK", "ETF") and fundamentus_data is not None and not fundamentus_data.empty and "ticker" in fundamentus_data.columns:
            row_fund = fundamentus_data[fundamentus_data["ticker"] == ticker]
            if not row_fund.empty:
                r = row_fund.iloc[0]
                fundamentals = {
                    "revenue_growth_yoy": _parse_pct(r.get("cresc_rec_5a", "0")),
                    "profit_growth_yoy": 0,
                    "roe": _parse_pct(r.get("roe", "0")),
                    "div_bruta_patrim": _parse_float(r.get("div_bruta_patrim", "99")),
                    "pl": _parse_float(r.get("pl", "0")),
                    "pvp": _parse_float(r.get("pvp", "0")),
                }
        
        # Score
        try:
            score_result = score_stock(
                df_daily, df_weekly,
                fundamentals=fundamentals,
                avg_volume_financeiro=volume_financeiro
            )
        except Exception as e:
            print(f"  [SKIP] {ticker}: scoring error: {e}")
            continue
        
        display = f"{analysis_ticker} (BDR: {ticker})" if category == "BDR" else ticker
        
        record = {
            "ticker": ticker,
            "name": row.get("name", ticker),
            "category": category,
            "display": display,
            "technical_score": score_result["technical_score"],
            "technical_tier": score_result["technical_tier"],
            "fundamental_score": score_result["fundamental_score"],
            "fundamental_tag": score_result["fundamental_tag"],
            "price": round(last_close, 2),
            "sma50": round(df_daily.iloc[-1].get("sma50", 0), 2),
            "sma150": round(df_daily.iloc[-1].get("sma150", 0), 2),
            "sma200": round(df_daily.iloc[-1].get("sma200", 0), 2),
            "volume_financeiro": round(volume_financeiro, 2),
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
            "roe": fundamentals.get("roe", 0),
            "pl": fundamentals.get("pl", 0),
            "pvp": fundamentals.get("pvp", 0),
        }
        results.append(record)
    
    return results


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
    print(" Screening B3 - Minervini + Patterns [OPTIMIZED]")
    print(f" Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    if not is_business_day():
        print("Not a business day. Skipping.")
        return
    
    brapi = BrapiClient()
    yf_client = YFinanceClient(delay=0.1)
    fclient = FundamentusClient()
    bdr_mapper = BDRMapper()
    
    # 1. Fetch universe
    universe = fetch_universe(brapi, bdr_mapper)
    if universe.empty:
        print("No tickers found. Exiting.")
        return
    
    # 2. Preflight filter
    universe = preflight_filter(universe, yf_client)
    
    # 3. Fetch IBOV
    print("[2.5/6] Fetching IBOV...")
    ibov = yf_client.get_history("^BVSP", period="2y", interval="1d")
    
    # 4. Fundamentus
    print("[2.8/6] Fetching Fundamentus...")
    fundamentus_data = fclient.get_stock_list()
    
    # 5. Process everything
    results = process_batch(universe, yf_client, ibov, fundamentus_data)
    
    if not results:
        print("No results. Exiting.")
        return
    
    # 6. Build ranking
    print("[5/6] Building ranking...")
    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values("technical_score", ascending=False).reset_index(drop=True)
    df_results["rank"] = df_results.index + 1
    
    # 7. Save
    today_str = datetime.now().strftime("%Y-%m-%d")
    print("[6/6] Saving...")
    
    json_path = RESULTS_DIR / f"screening_{today_str}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    df_results.to_parquet(RESULTS_DIR / "latest.parquet", index=False)
    df_results.to_csv(RESULTS_DIR / f"screening_{today_str}.csv", index=False, encoding="utf-8-sig")
    
    summary = {
        "date": today_str,
        "qualified": len(df_results),
        "tier_s": int((df_results["technical_tier"] == "S").sum()),
        "tier_a": int((df_results["technical_tier"] == "A").sum()),
        "tier_b": int((df_results["technical_tier"] == "B").sum()),
        "breakouts": int(df_results["breakout"].sum()),
        "vcps": int(df_results["vcp"].sum()),
        "wedges": int(df_results["wedge"].sum()),
        "fund_strong": int((df_results["fundamental_tag"] == "Forte").sum()),
        "fund_ok": int((df_results["fundamental_tag"] == "OK").sum()),
        "fund_weak": int((df_results["fundamental_tag"] == "Fraco").sum()),
    }
    
    with open(RESULTS_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n DONE! {len(df_results)} stocks | S:{summary['tier_s']} A:{summary['tier_a']} B:{summary['tier_b']}")
    print(f" Breakouts: {summary['breakouts']} | VCPs: {summary['vcps']} | Wedges: {summary['wedges']}")
    print(f" Fundamentals: Forte:{summary['fund_strong']} OK:{summary['fund_ok']} Fraco:{summary['fund_weak']}")


if __name__ == "__main__":
    run_screening()
