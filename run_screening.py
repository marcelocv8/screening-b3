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
from src.data.fii_filter import load_allowed_11, is_fii
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


# Fallback list from lista de ativos br.xlsx + backup ProfitChart (368 tickers)
FALLBACK_STOCKS = [
    "PETR4", "VALE3", "ITUB4", "PRIO3", "PETR3", "B3SA3", "BBDC4", "AXIA3", "SBSP3",
    "BBAS3", "BPAC11", "EMBJ3", "RENT3", "ITSA4", "ABEV3", "ENEV3", "CPLE3", "SUZB3",
    "WEGE3", "EQTL3", "RDOR3", "VBBR3", "BRAV3", "LREN3", "GGBR4", "MBRF3", "RADL3",
    "RAIL3", "CMIG4", "CSMG3", "ALOS3", "VIVT3", "ENGI11", "CYRE3", "BBSE3", "CSAN3",
    "UGPA3", "MOTV3", "MGLU3", "TOTS3", "CURY3", "TIMS3", "AURA33", "HAPV3", "BBDC3",
    "SMFT3", "ASAI3", "AXIA6", "MULT3", "DIRR3", "CPFE3", "CEAB3", "SAPR11", "KLBN11",
    "AZZA3", "ISAE4", "USIM5", "SANB11", "CSNA3", "COGN3", "TAEE11", "PSSA3", "VIVA3",
    "GOAU4", "NATU3", "CXSE3", "MOVI3", "ITUB3", "TEND3", "MRVE3", "EGIE3", "HYPE3",
    "ECOR3", "VAMO3", "POMO4", "NEOE3", "BEEF3", "BRKM5", "SLCE3", "ODPV3", "GMAT3",
    "PLPL3", "JHSF3", "IGTI11", "SMTO3", "ORVR3", "YDUQ3", "BRAP4", "RECV3", "GGPS3",
    "SIMH3", "AURE3", "FLRY3", "CMIN3", "IRBR3", "MDNE3", "PGMN3", "INTB3", "BRSR6",
    "ALUP11", "CBAV3", "CVCB3", "EZTC3", "ANIM3", "ALPA4", "RAPT4", "VULC3", "SBFG3",
    "TTEN3", "GRND3", "PCAR3", "UNIP6", "CYRE4", "AMER3", "TUPY3", "KEPL3", "ABCB4",
    "KLBN4", "RIAA3", "PINE4", "ONCO3", "LWSA3", "DESK3", "RAIZ4", "TGMA3", "LEVE3",
    "SEER3", "BHIA3", "VTRU3", "DXCO3", "BMOB3", "MILS3", "AUAU3", "SAPR4", "MYPK3",
    "LAVV3", "MDIA3", "PNVL3", "TFCO4", "PRNR3", "JSLG3", "LOGG3", "SOJA3", "QUAL3",
    "BIOM3", "RANI3", "BMGB4", "HBSA3", "FIQE3", "FRAS3", "LIGT3", "LJQQ3", "CASH3",
    "RCSL4", "VLID3", "CAML3", "FESA4", "DASA3", "POSI3", "ARML3", "AGRO3", "POMO3",
    "EVEN3", "WIZC3", "OPCT3", "JALL3", "BRBI11", "USIM3", "KLBN3", "HBOR3", "CSED3",
    "ALLD3", "CMIG3", "BLAU3", "TAEE4", "TRIS3", "ITSA3", "MELK3", "ROMI3", "SHUL4",
    "VVEO3", "SYNE3", "BMEB4", "SAPR3", "OBTC3", "PFRM3", "GFSA3", "TASA4", "MTRE3",
    "SANB4", "PMAM3", "SANB3", "AZTE3", "AMBP3", "TAEE3", "VITT3", "HBRE3", "CSUD3",
    "MLAS3", "BRST3", "ESPA3", "BRAP3", "PTBL3", "AZEV4", "DEXP3", "GGBR3", "MATD3",
    "SCAR3", "AMOB3", "EUCA4", "SEQL3", "NGRD3", "ENJU3", "BPAC3", "ALPK3", "ETER3",
    "UNIP3", "TPIS3", "BMEB3", "GOAU3", "BPAC5", "ALPA3", "EMAE4", "LOGN3", "BRKM3",
    "OFSA3", "DMVF3", "CLSC4", "BAZA3", "LAND3", "TECN3", "AZEV3", "LPSB3", "COCE5",
    "IFCM3", "SHOW3", "PTNT4", "AALR3", "CAMB3", "MEAL3", "WEST3", "CGRA4", "EALT4",
    "CEBR6", "TOKY3", "PDGR3", "AERI3", "CEBR3", "LUPA3", "RSUL4", "RCSL3", "RAPT3",
    "INEP3", "OIBR3", "BSLI4", "ALUP4", "TCSA3", "ENGI4", "BSLI3", "AMAR3", "BRSR3",
    "CGAS5", "PINE3", "BEES3", "TASA3", "WHRL4", "IGTI3", "RNEW4", "ISAE3", "RNEW3",
    "TKNO4", "BGIP4", "MNPR3", "ATED3", "VSTE3", "ENGI3", "ALUP3", "DOTZ3", "MGEL4",
    "EQPA3", "INEP4", "CEBR5", "AGXY3", "MTSA4", "WDCN3", "PDTC3", "BEES4", "CRPG5",
    "AVLL3", "VIVR3", "CTKA4", "CGRA3", "UCAS3", "HOOT4", "BMKS3", "BIED3", "EKTR4",
    "EPAR3", "HAGA4", "PATI3", "WHRL3", "FHER3", "BGIP3", "BNBR3", "DEXP4", "TRAD3",
    # Additional tickers from ProfitChart backup (Jul 2024)
    "ALSO3", "ARZZ3", "ATMP3", "AZUL4", "BOAS3", "BPAN4", "BRFS3", "BRIT3", "CCRO3",
    "CEDO4", "CIEL3", "CLSA3", "CPLE6", "CRFB3", "ELET6", "ELMD3", "EMBR3", "ENAT3",
    "ENBR3", "GOLL4", "GUAR3", "JBSS3", "KRSA3", "LVTC3", "MBLY3", "MODL3", "MRFG3",
    "NINJ3", "NTCO3", "PETZ3", "PORT3", "RDNI3", "RRRP3", "SGPS3", "SOMA3", "SQIA3",
    "STBP3", "TRPL4", "VIIA3", "WIZS3", "ZAMP3",
]

FALLBACK_ETFS = [
    "BOVA11", "SMAL11", "IVVB11", "BRCR11", "KNRI11", "HGLG11", "XPLG11", "VISC11",
    # FIPs B3 ETFs
    "AZIN11", "MINE11", "BSGD11", "BDIV11", "COPN11", "ENDD11", "FINC11", "ESUT11",
    "ESUU11", "ESUD11", "NVRP11", "KNDI11", "KNOX11", "OPHF11", "PICE11", "PLBR11",
    "FPOR11", "RZDL11", "VIGT11", "XPIE11", "BMMT11", "AGRI11", "BBOV11", "BRAZ11",
    "DVER11", "BBOI11", "DOLA11", "CORN11", "BBSD11", "TECX11", "PKIN11", "OURO11",
    "SPYR11", "BREW11", "BCIC11", "BDEF11", "IBOB11", "ESGB11", "GOLB11", "DOLB11",
    "SPXB11", "SPBZ11", "GENB11", "SMAB11", "CMDB11", "AUVP11", "TIRB11", "BRXC11",
    "RICO11", "GDIV11", "XBCI11", "XSPI11", "PIPE11", "QQQQ11", "QQQI11", "COIN11",
    "FIXX11", "AURO11", "CASA11", "IWMI11", "SPYI11", "XBOV11", "GLDX11", "TRIG11",
    "BOVB11", "GXUS11", "BULZ11", "ARGE11", "UTLL11", "PEVC11", "BEST11", "BVBR11",
    "USTK11", "SVAL11", "CHIP11", "VWRA11", "IVWO11", "WRLD11", "GPUS11", "BDOM11",
    "BXPO11", "SCVB11", "ALUG11", "NUCL11", "BIZD11", "CAPE11", "EWBZ11", "BRAX11",
    "ECOO11", "SILK11", "GLDI11", "B3BR11", "BOVV11", "DIVO11", "DIVD11", "FIND11",
    "GOVE11", "MATB11", "ISUS11", "MILL11", "HTEK11", "TECK11", "PIBB11", "REVE11",
    "SPXR11", "SPXI11", "SMAC11", "NSDV11", "HIGH11", "LVOL11", "NBOV11", "NDIV11",
    "QLBR11", "SPUB11", "SPVT11", "BOVS11", "ELAS11", "GOLX11", "ACWI11", "XINA11",
    "UTEC11", "USAL11", "BOVX11", "GOLD11", "SLVR11", "NASD11", "DOLX11", "SPXH11",
]

FALLBACK_BDRS = {
    "AAPL34": "AAPL", "ABEV34": "ABEV", "AMZO34": "AMZN", "BERK34": "BRK-B",
    "BOAC34": "BAC", "COCA34": "KO", "DISB34": "DIS", "EXXO34": "XOM",
    "FDXB34": "FDX", "GEOO34": "GE", "GOGL34": "GOOGL", "IBMB34": "IBM",
    "ITLC34": "IT", "JNJB34": "JNJ", "JPMC34": "JPM", "LMTB34": "LMT",
    "M1TA34": "META", "MCDC34": "MCD", "MSFT34": "MSFT", "NFLX34": "NFLX",
    "NVDC34": "NVDA", "ORCL34": "ORCL", "PEPB34": "PEP", "PFEI34": "PFE",
    "PGCO34": "PG", "QCOM34": "QCOM", "TSLA34": "TSLA", "VISA34": "V",
    "WALM34": "WMT"
}

def fetch_universe(brapi: BrapiClient, bdr_mapper: BDRMapper) -> pd.DataFrame:
    """Fetch and aggressively filter universe. Excludes FIIs via whitelist."""
    print("[1/6] Fetching universe from Brapi...")
    
    allowed_11 = load_allowed_11()
    universe = []
    
    # Stocks
    stocks = brapi.list_stocks(type_="stock")
    if stocks.empty:
        print("[1/6] Brapi stocks failed, using fallback list...")
        stocks = pd.DataFrame({"stock": FALLBACK_STOCKS, "name": FALLBACK_STOCKS})
    if not stocks.empty:
        stocks = stocks[~stocks["stock"].apply(is_fractional)]
        # Exclude FIIs: any ticker ending in 11 not in whitelist
        before_fii = len(stocks)
        stocks = stocks[~stocks["stock"].apply(lambda x: is_fii(x, allowed_11))]
        after_fii = len(stocks)
        if before_fii != after_fii:
            print(f"[1/6] Excluded {before_fii - after_fii} FIIs (ticker 11 not in whitelist)")
        stocks["category"] = "BR_STOCK"
        stocks["analysis_ticker"] = stocks["stock"] + ".SA"
        universe.append(stocks[["stock", "name", "category", "analysis_ticker"]])
    
    # ETFs
    etfs = brapi.list_stocks(type_="fund")
    if etfs.empty:
        print("[1/6] Brapi ETFs failed, using fallback list...")
        etfs = pd.DataFrame({"stock": FALLBACK_ETFS, "name": FALLBACK_ETFS})
    if not etfs.empty:
        etfs = etfs[~etfs["stock"].apply(is_fractional)]
        etfs["category"] = "ETF"
        etfs["analysis_ticker"] = etfs["stock"] + ".SA"
        universe.append(etfs[["stock", "name", "category", "analysis_ticker"]])
    
    # BDRs
    bdrs = brapi.list_stocks(type_="bdr")
    if bdrs.empty:
        print("[1/6] Brapi BDRs failed, using fallback list...")
        bdr_list = [{"stock": k, "name": k, "underlying": v} for k, v in FALLBACK_BDRS.items()]
        bdrs = pd.DataFrame(bdr_list)
    if not bdrs.empty:
        if "underlying" not in bdrs.columns:
            bdrs["underlying"] = bdrs["stock"].apply(lambda x: bdr_mapper.get_underlying(x))
        bdrs = bdrs[~bdrs["stock"].apply(is_fractional)]
        bdrs["category"] = "BDR"
        mapped = bdrs["underlying"].notna().sum()
        skipped = bdrs["underlying"].isna().sum()
        bdrs = bdrs[bdrs["underlying"].notna()].copy()
        bdrs["analysis_ticker"] = bdrs["underlying"]
        universe.append(bdrs[["stock", "name", "category", "analysis_ticker"]])
        print(f"[1/6] BDRs mapped: {mapped} | skipped: {skipped}")
    
    if not universe:
        print("[1/6] CRITICAL: No universe data available from any source!")
        return pd.DataFrame()
    
    df = pd.concat(universe, ignore_index=True)
    df = df.drop_duplicates(subset=["stock"])
    print(f"[1/6] Universe after filtering: {len(df)} tickers")
    return df


def preflight_filter(universe: pd.DataFrame, yf_client: YFinanceClient) -> pd.DataFrame:
    """Quick validation: remove tickers that yfinance can't find at all."""
    print("[2/6] Preflight validation (quick existence check)...")
    
    # Validate BDRs first
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
    
    # Validate BR stocks and ETFs - CRITICAL to avoid batch corruption
    br_etf_tickers = universe[universe["category"].isin(["BR_STOCK", "ETF"])]["analysis_ticker"].tolist()
    if br_etf_tickers:
        print(f"  Validating {len(br_etf_tickers)} BR stocks/ETFs...")
        valid_br = yf_client.validate_tickers(br_etf_tickers)
        invalid_br = set(br_etf_tickers) - set(valid_br)
        if invalid_br:
            print(f"  Removing {len(invalid_br)} invalid BR tickers: {list(invalid_br)[:5]}...")
            universe = universe[~(
                universe["category"].isin(["BR_STOCK", "ETF"]) & 
                universe["analysis_ticker"].isin(invalid_br)
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
    
    # DEBUG: Track category counts
    print("[4/6] Processing and scoring...")
    print(f"  DEBUG: Universe categories: {universe['category'].value_counts().to_dict()}")
    print(f"  DEBUG: br_data keys sample: {list(br_data.keys())[:5]}")
    print(f"  DEBUG: bdr_data keys sample: {list(bdr_data.keys())[:5]}")
    print(f"  DEBUG: br_data has {len(br_data)} entries, bdr_data has {len(bdr_data)} entries")
    
    # Count tickers in br_data by looking up universe tickers
    br_in_universe = universe[universe["category"].isin(["BR_STOCK", "ETF"])]["analysis_ticker"].tolist()
    bdr_in_universe = universe[universe["category"] == "BDR"]["analysis_ticker"].tolist()
    br_found = sum(1 for t in br_in_universe if t in br_data)
    bdr_found = sum(1 for t in bdr_in_universe if t in bdr_data)
    print(f"  DEBUG: BR tickers in universe: {len(br_in_universe)}, found in br_data: {br_found}")
    print(f"  DEBUG: BDR tickers in universe: {len(bdr_in_universe)}, found in bdr_data: {bdr_found}")
    
    results = []
    total = len(universe)
    category_counts = {"processed": 0, "skipped": 0, "by_category": {}}
    
    for idx, row in universe.iterrows():
        if idx % 100 == 0:
            print(f"  Progress: {idx}/{total}")
        
        ticker = row["stock"]
        category = row["category"]
        analysis_ticker = row["analysis_ticker"]
        
        # Get data from batch results
        df_daily = br_data.get(analysis_ticker, pd.DataFrame()) if category != "BDR" else bdr_data.get(analysis_ticker, pd.DataFrame())
        df_weekly = br_weekly.get(analysis_ticker, pd.DataFrame()) if category != "BDR" else bdr_weekly.get(analysis_ticker, pd.DataFrame())
        
        # DEBUG: Show first few skipped tickers
        if df_daily.empty or len(df_daily) < 50:
            category_counts["skipped"] = category_counts.get("skipped", 0) + 1
            category_counts["by_category"][category] = category_counts["by_category"].get(category, 0) + 1
            if len(results) == 0 and idx < 20:
                print(f"  DEBUG SKIP {category} {analysis_ticker}: empty={df_daily.empty}, len={len(df_daily)}")
            continue
        
        category_counts["processed"] = category_counts.get("processed", 0) + 1
        
        # Calculate indicators
        df_daily = calculate_ma(df_daily, [50, 150, 200])
        
        # Volume financeiro - skip if close is NaN
        last_close = df_daily.iloc[-1]["close"]
        if pd.isna(last_close):
            category_counts["skipped"] = category_counts.get("skipped", 0) + 1
            category_counts["by_category"][category] = category_counts["by_category"].get(category, 0) + 1
            continue
        
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
        
        # Score DAILY
        try:
            score_result = score_stock(
                df_daily, df_weekly,
                fundamentals=fundamentals,
                avg_volume_financeiro=volume_financeiro
            )
        except Exception as e:
            print(f"  [SKIP] {ticker}: scoring error: {e}")
            continue
        
        # Score WEEKLY (if data available)
        weekly_score = 0.0
        weekly_tier = "C"
        weekly_patterns = {}
        weekly_breakout = False
        if df_weekly is not None and len(df_weekly) >= 30:
            try:
                weekly_result = score_stock(
                    df_weekly, None,
                    fundamentals=fundamentals,
                    avg_volume_financeiro=volume_financeiro
                )
                weekly_score = weekly_result["technical_score"]
                weekly_tier = weekly_result["technical_tier"]
                weekly_patterns = weekly_result.get("patterns", {})
                weekly_breakout = weekly_result.get("breakout", False)
            except Exception:
                pass
        
        display = f"{analysis_ticker} (BDR: {ticker})" if category == "BDR" else ticker
        
        record = {
            "ticker": ticker,
            "name": row.get("name", ticker),
            "category": category,
            "display": display,
            "technical_score": score_result["technical_score"],
            "technical_tier": score_result["technical_tier"],
            "technical_score_weekly": weekly_score,
            "technical_tier_weekly": weekly_tier,
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
            # Weekly patterns
            "vcp_weekly": weekly_patterns.get("vcp", {}).get("detected", False),
            "wedge_weekly": weekly_patterns.get("wedge", {}).get("detected", False),
            "cup_handle_weekly": weekly_patterns.get("cup_handle", {}).get("detected", False),
            "double_bottom_weekly": weekly_patterns.get("double_bottom", {}).get("detected", False),
            "inverse_hs_weekly": weekly_patterns.get("inverse_hs", {}).get("detected", False),
            "pre_breakout_weekly": weekly_patterns.get("pre_breakout", False),
            "breakout_weekly": weekly_breakout,
            "roe": fundamentals.get("roe", 0),
            "pl": fundamentals.get("pl", 0),
            "pvp": fundamentals.get("pvp", 0),
        }
        results.append(record)
    
    print(f"  DEBUG: Processed {category_counts['processed']}, Skipped {category_counts['skipped']}")
    print(f"  DEBUG: Skip by category: {category_counts['by_category']}")
    
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
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # 6.5 Market Breadth
    print("[5.5/6] Calculating market breadth...")
    from src.core.breadth import calculate_breadth_indicators
    breadth_data = calculate_breadth_indicators(df_results, date_str=today_str)
    
    # 6.6 AI Opinion
    print("[5.6/6] Generating AI opinion...")
    from src.core.ai_opinion import get_ai_opinion
    ai_opinion = get_ai_opinion(breadth_data, today_str)
    
    # 7. Save
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
        "allocation_score": breadth_data.get("allocation_score", 3),
        "regime": breadth_data.get("regime", "Neutro"),
    }
    
    with open(RESULTS_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    
    with open(RESULTS_DIR / "breadth_score.json", "w", encoding="utf-8") as f:
        json.dump(breadth_data, f, indent=2, ensure_ascii=False)
    
    with open(RESULTS_DIR / "ai_opinion.json", "w", encoding="utf-8") as f:
        json.dump(ai_opinion, f, indent=2, ensure_ascii=False)
    
    print(f"\n DONE! {len(df_results)} stocks | S:{summary['tier_s']} A:{summary['tier_a']} B:{summary['tier_b']}")
    print(f" Breakouts: {summary['breakouts']} | VCPs: {summary['vcps']} | Wedges: {summary['wedges']}")
    print(f" Fundamentals: Forte:{summary['fund_strong']} OK:{summary['fund_ok']} Fraco:{summary['fund_weak']}")
    print(f" Market Regime: {summary['regime']} (Score: {summary['allocation_score']}/5)")
    print(f" AI Opinion: {'✅ Gemini' if ai_opinion.get('has_ai') else '⚠️ Fallback'}")


if __name__ == "__main__":
    run_screening()
