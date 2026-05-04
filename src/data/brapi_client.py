import requests
import pandas as pd
import time
from typing import List, Optional, Dict, Any
import os

class BrapiClient:
    BASE_URL = "https://brapi.dev/api"
    
    def __init__(self, token: Optional[str] = None):
        self.token = token or os.environ.get("BRAPI_TOKEN")
        self.headers = {}
        if self.token:
            self.headers["Authorization"] = f"Bearer {self.token}"
    
    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"[Brapi Error] {endpoint}: {e}")
            return {}
    
    def list_stocks(self, type_: str = "stock", limit: int = 1000) -> pd.DataFrame:
        """List all available tickers of a given type (stock, bdr, fund, etf)."""
        tickers = []
        page = 1
        while True:
            data = self._get("quote/list", params={"type": type_, "limit": limit, "page": page})
            stocks = data.get("stocks", [])
            if not stocks:
                break
            tickers.extend(stocks)
            if data.get("hasNextPage", False):
                page += 1
                time.sleep(0.2)
            else:
                break
        return pd.DataFrame(tickers)
    
    def get_quote(self, tickers: List[str]) -> pd.DataFrame:
        """Get current quote for tickers (comma-separated)."""
        ticker_str = ",".join(tickers)
        data = self._get(f"quote/{ticker_str}")
        results = data.get("results", [])
        return pd.DataFrame(results)
    
    def get_historical(self, ticker: str, range_: str = "1y", interval: str = "1d") -> pd.DataFrame:
        """Get historical OHLCV data."""
        data = self._get(f"quote/{ticker}", params={"range": range_, "interval": interval})
        results = data.get("results", [{}])[0]
        hist = results.get("historicalDataPrice", [])
        if not hist:
            return pd.DataFrame()
        df = pd.DataFrame(hist)
        df["date"] = pd.to_datetime(df["date"], unit="s")
        df = df.sort_values("date")
        return df
