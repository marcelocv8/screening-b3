import yfinance as yf
import pandas as pd
import time
from typing import Optional, List

class YFinanceClient:
    def __init__(self, delay: float = 0.3):
        self.delay = delay
    
    def get_ticker(self, symbol: str):
        return yf.Ticker(symbol)
    
    def get_history(self, symbol: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
        """Get historical OHLCV data for a symbol."""
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=interval, auto_adjust=True, timeout=10)
            if df.empty:
                return pd.DataFrame()
            df = df.reset_index()
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]
            # rename common columns
            rename_map = {
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
                "adj_close": "adj_close",
                "date": "date",
                "datetime": "date"
            }
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            if "date" not in df.columns and "datetime" in df.columns:
                df = df.rename(columns={"datetime": "date"})
            df = df.sort_values("date").reset_index(drop=True)
            time.sleep(self.delay)
            return df
        except Exception as e:
            print(f"[YFinance Error] {symbol}: {e}")
            return pd.DataFrame()
    
    def get_info(self, symbol: str) -> dict:
        """Get fundamental info for a symbol (used for US stocks)."""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info or {}
            time.sleep(self.delay)
            return info
        except Exception as e:
            print(f"[YFinance Info Error] {symbol}: {e}")
            return {}
    
    def get_weekly(self, symbol: str, period: str = "2y") -> pd.DataFrame:
        """Get weekly data for pattern detection (e.g. Cup & Handle)."""
        return self.get_history(symbol, period=period, interval="1wk")
