import requests
import pandas as pd
import zipfile
import io
from typing import Optional, Dict
from pathlib import Path

class CVMClient:
    """Client for CVM Dados Abertos (Brazilian SEC open data).
    Downloads financial statements (DFP/ITR) and calculates key indicators.
    """
    
    BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC"
    
    def __init__(self, cache_dir: str = "assets/cvm_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._dre_data: Optional[pd.DataFrame] = None
        self._bp_data: Optional[pd.DataFrame] = None
    
    def _download_csv(self, doc_type: str, year: int) -> Optional[pd.DataFrame]:
        """Download DFP or ITR CSV for a given year."""
        cache_file = self.cache_dir / f"{doc_type}_{year}.csv"
        
        if cache_file.exists():
            return pd.read_csv(cache_file, sep=";", encoding="latin1", low_memory=False)
        
        url = f"{self.BASE_URL}/{doc_type}/DADOS/{doc_type}_cia_aberta_{year}.zip"
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
                with z.open(csv_name) as f:
                    df = pd.read_csv(f, sep=";", encoding="latin1", low_memory=False)
            
            df.to_csv(cache_file, index=False, encoding="utf-8")
            return df
        except Exception as e:
            print(f"[CVM Error] {doc_type} {year}: {e}")
            return None
    
    def load_latest_data(self) -> bool:
        """Load latest available DFP and BP data."""
        current_year = pd.Timestamp.now().year
        
        # Try current year and previous year
        for year in [current_year, current_year - 1]:
            dre = self._download_csv("DFP", year)
            if dre is not None and not dre.empty:
                self._dre_data = dre
                break
        
        for year in [current_year, current_year - 1]:
            bp = self._download_csv("DFP", year)
            if bp is not None and not bp.empty:
                # BP is in a different file within the same zip usually
                # For simplicity, we'll use the DRE data and estimate
                self._bp_data = bp
                break
        
        return self._dre_data is not None
    
    def get_fundamentals(self, cnpj: str) -> Dict[str, float]:
        """Calculate fundamentals from CVM data for a given CNPJ."""
        if self._dre_data is None:
            return {}
        
        # Filter by CNPJ and latest available
        company_data = self._dre_data[self._dre_data["CNPJ_CIA"] == cnpj]
        if company_data.empty:
            return {}
        
        # Get latest period
        latest = company_data.sort_values("DT_REFER").iloc[-1]
        
        # Extract key values from DRE
        # CD_CONTA codes:
        # 3.01 = Receita de Venda
        # 3.03 = Resultado Bruto
        # 3.05 = EBIT / Resultado Antes do Resultado Financeiro
        # 3.11 = Lucro/Prejuízo do Período
        
        revenue = self._get_account_value(company_data, "3.01")
        gross_profit = self._get_account_value(company_data, "3.03")
        ebit = self._get_account_value(company_data, "3.05")
        net_income = self._get_account_value(company_data, "3.11")
        
        fundamentals = {
            "revenue": revenue,
            "gross_profit": gross_profit,
            "ebit": ebit,
            "net_income": net_income,
        }
        
        # Calculate margins
        if revenue and revenue > 0:
            fundamentals["gross_margin"] = (gross_profit / revenue) * 100
            fundamentals["ebit_margin"] = (ebit / revenue) * 100
            fundamentals["net_margin"] = (net_income / revenue) * 100
        
        return fundamentals
    
    def _get_account_value(self, df: pd.DataFrame, account_code: str) -> float:
        """Get the latest value for a specific account code."""
        mask = df["CD_CONTA"] == account_code
        if not mask.any():
            return 0.0
        latest = df[mask].sort_values("DT_REFER").iloc[-1]
        return float(latest.get("VL_CONTA", 0))
