import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
from typing import Optional

class FundamentusClient:
    BASE_URL = "https://www.fundamentus.com.br"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0",
    }
    
    def __init__(self, delay: float = 0.5):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        # First request to get cookies
        try:
            self.session.get(self.BASE_URL, timeout=10)
        except Exception:
            pass
    
    def _get(self, path: str, params: Optional[dict] = None) -> Optional[BeautifulSoup]:
        url = f"{self.BASE_URL}/{path}"
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            time.sleep(self.delay)
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            print(f"[Fundamentus Error] {path}: {e}")
            return None
    
    def get_stock_list(self) -> pd.DataFrame:
        """Scrape the stock list from fundamentus with key indicators."""
        soup = self._get("resultado.php")
        if not soup:
            return pd.DataFrame()
        table = soup.find("table", {"id": "resultado"})
        if not table:
            return pd.DataFrame()
        
        # Read HTML table into DataFrame
        df = pd.read_html(str(table), decimal=",", thousands=".")[0]
        
        # Clean column names
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
        
        # Rename common columns
        rename_map = {
            "papel": "ticker",
            "cotacao": "price",
            "p/l": "pl",
            "p/vp": "pvp",
            "psr": "psr",
            "div.yield": "dividend_yield",
            "p/atvo": "p_ativo",
            "p/cap.giro": "p_cap_giro",
            "p/ebit": "pebit",
            "p/ativ_circ_liq": "p_ativ_circ_liq",
            "ev/ebit": "evebit",
            "ev/ebitda": "evebitda",
            "mrg_ebit": "mrg_ebit",
            "mrg._liq.": "mrg_liq",
            "liq._corr.": "liq_corrente",
            "roic": "roic",
            "roe": "roe",
            "liq._2meses": "liq_2meses",
            "patrim._liq": "patr_liq",
            "div._brut/patrim.": "div_bruta_patrim",
            "cresc._rec._5a": "cresc_rec_5a",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        return df
    
    def get_detailed_info(self, ticker: str) -> dict:
        """Scrape detailed info page for a specific ticker."""
        soup = self._get("detalhes.php", params={"papel": ticker})
        if not soup:
            return {}
        
        data = {}
        # Parse main data table
        tables = soup.find_all("table", {"class": "w728"})
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).lower()
                    val = cells[1].get_text(strip=True)
                    data[key] = val
        
        return data
