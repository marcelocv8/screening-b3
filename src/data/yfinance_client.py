import yfinance as yf
import pandas as pd
import numpy as np
import time
from typing import Optional, List, Dict

class YFinanceClient:
    def __init__(self, delay: float = 0.3):
        self.delay = delay
    
    def validate_tickers(self, symbols: List[str]) -> List[str]:
        """Quick preflight validation: returns only symbols with actual price data."""
        if not symbols:
            return []
        try:
            data = yf.download(
                symbols, period="5d", interval="1d",
                group_by="ticker", auto_adjust=True,
                progress=False, threads=True, timeout=10
            )
            valid = []
            for sym in symbols:
                if len(symbols) == 1:
                    df = data
                else:
                    df = data.get(sym, pd.DataFrame())
                # Check if there's any actual non-NaN close price
                if not df.empty and "Close" in df.columns and df["Close"].notna().any():
                    valid.append(sym)
            return valid
        except Exception:
            return []
    
    def batch_download(self, symbols: List[str], period: str = "1y", interval: str = "1d") -> Dict[str, pd.DataFrame]:
        """Download multiple tickers at once using yf.download. Much faster than individual requests."""
        if not symbols:
            return {}
        
        results = {}
        batch_size = 50
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            try:
                data = yf.download(
                    batch, period=period, interval=interval,
                    group_by="ticker", auto_adjust=True,
                    progress=False, threads=True, timeout=20
                )
                
                if len(batch) == 1:
                    sym = batch[0]
                    df = self._clean_df(data)
                    if not df.empty and df["close"].notna().any():
                        results[sym] = df
                else:
                    for sym in batch:
                        if sym in data.columns.levels[0]:
                            df = self._clean_df(data[sym])
                            if not df.empty and df["close"].notna().any():
                                results[sym] = df
                
                time.sleep(self.delay)
            except Exception as e:
                print(f"[Batch Error] {batch[:3]}...: {e}")
                time.sleep(1)
        
        return results
    
    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize yfinance DataFrame."""
        if df is None or df.empty:
            return pd.DataFrame()
        
        df = df.reset_index()
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        
        rename_map = {
            "open": "open", "high": "high", "low": "low",
            "close": "close", "volume": "volume",
            "adj_close": "adj_close", "date": "date", "datetime": "date"
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        
        if "date" not in df.columns and "datetime" in df.columns:
            df = df.rename(columns={"datetime": "date"})
        
        df = df.sort_values("date").reset_index(drop=True)
        return df
    
    def get_history(self, symbol: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
        """Get historical OHLCV for a single symbol (fallback)."""
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=interval, auto_adjust=True, timeout=10)
            if df.empty:
                return pd.DataFrame()
            return self._clean_df(df)
        except Exception as e:
            print(f"[YFinance Error] {symbol}: {e}")
            return pd.DataFrame()
    
    def get_info(self, symbol: str) -> dict:
        """Get fundamental info for a symbol."""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info or {}
            time.sleep(self.delay)
            return info
        except Exception as e:
            print(f"[YFinance Info Error] {symbol}: {e}")
            return {}
    
    def get_weekly(self, symbol: str, period: str = "2y") -> pd.DataFrame:
        """Get weekly data for pattern detection."""
        return self.get_history(symbol, period=period, interval="1wk")
