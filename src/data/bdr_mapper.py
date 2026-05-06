import pandas as pd
from typing import Dict, Tuple, Optional
import requests

# Fallback manual mapping for common BDRs (in case CSV download fails)
BDR_MANUAL_MAP = {
    "AAPL34": "AAPL", "ABEV34": "ABEV", "ADBE34": "ADBE", "AMZO34": "AMZN",
    "ARMT34": "ARM", "BOAC34": "BAC", "BERK34": "BRK-B", "COCA34": "KO",
    "CRFB34": "CRF", "DISB34": "DIS", "EXXO34": "XOM", "FDXB34": "FDX",
    "GEOO34": "GE", "GGBR34": "GGB", "GOGL34": "GOOGL", "HONB34": "HON",
    "ITLC34": "IT", "JNJB34": "JNJ", "JPMC34": "JPM", "KFCB34": "YUM",
    "LILY34": "LLY", "LMTB34": "LMT", "M1TA34": "META", "MELI34": "MELI",
    "MCDC34": "MCD", "MSFT34": "MSFT", "NFLX34": "NFLX", "NVDC34": "NVDA",
    "ORCL34": "ORCL", "PEPB34": "PEP", "PFEI34": "PFE", "PGCO34": "PG",
    "QCOM34": "QCOM", "TSLA34": "TSLA", "UPSS34": "UPS", "UTDI34": "UTDI",
    "VISA34": "V", "WALM34": "WMT", "WFCO34": "WFC", "XRXB34": "XRX",
    "BMYB34": "BMY", "COWC34": "COST", "DEEC34": "DE", "GDBR34": "GILD",
    "HOME34": "HD", "IBMB34": "IBM", "INTC34": "INTC", "MUTC34": "MU",
    "NOKI34": "NOK", "PFIZ34": "PFE", "RBIH34": "RBGLY", "SLBG34": "SLB",
    "SNEC34": "SNE", "TEXA34": "TXN", "TIET34": "TX", "TMOS34": "TMO",
    "TOYB34": "TYP", "TWTR34": "TWTR", "UOLL34": "UOL", "VIVA34": "VIV",
    "WUNI34": "WU", "MOSC34": "MS", "STBP34": "STT", "SULA34": "SUL",
    "BOEI34": "BA", "CATP34": "CAT", "CHEV34": "CVX", "CMCS34": "CMCSA",
    "CODA34": "CODI", "CSXC34": "CSX", "DHER34": "DHR", "GMCO34": "GM",
    "HALI34": "HAL", "HPQB34": "HPQ", "IHPB34": "IHP", "MSNG34": "MSI",
    "NEMB34": "NEM", "OXYB34": "OXY", "PAAS34": "PAAS", "PAGS34": "PAGS",
    "PRIO34": "PRIO", "RDFN34": "RDFN", "SBUB34": "SBUX", "SPGI34": "SPG",
    "TAKP34": "TAK", "TGTB34": "TGT", "TRPL34": "TRP", "TSMC34": "TSM",
    "UALG34": "UAL", "UBER34": "UBER", "UNHB34": "UNH", "VRTX34": "VRTX",
    "WYNN34": "WYNN", "BIDU34": "BIDU", "BABA34": "BABA", "JDMC34": "JD",
    "PYPL34": "PYPL", "SQIA34": "SQ", "SHOP34": "SHOP", "SNOW34": "SNOW",
    "ZMST34": "ZM", "CRWD34": "CRWD", "DDOG34": "DDOG", "NETE34": "NET",
    "OKTA34": "OKTA", "PLTR34": "PLTR", "RBLX34": "RBLX", "ROKU34": "ROKU",
    "SEYE34": "SE", "SPOT34": "SPOT", "TEAM34": "TEAM", "TWLO34": "TWLO",
    "UBSF34": "UBSFY", "ZMST34": "ZM", "ZTSB34": "ZTS", "AZUL34": "AZUL",
    "GOLL34": "GOL", "CCRO34": "CCRO", "CSNA34": "SID", "ELET34": "ELP",
    "ELPL34": "ELPL", "ENBR34": "EBR", "EQTL34": "EQTL", "GGBR34": "GGB",
    "ITSA34": "ITSA", "ITUB34": "ITUB", "JBSS34": "JBSAY", "KLBN34": "KLBAY",
    "LREN34": "LRENY", "MRVE34": "MRVE", "NATU34": "NU", "PCAR34": "PCAR",
    "PETR34": "PBR", "RDOR34": "RDOR", "RAIL34": "RAIL", "SANB34": "SAN",
    "SBSP34": "SBS", "SULA34": "SULA", "SUZB34": "SUZ", "TAEE34": "TAEEY",
    "TEND34": "TEND", "TIMS34": "TIMS", "TRPL34": "TRP", "UGPA34": "UGP",
    "USIM34": "USIM", "VALE34": "VALE", "VIIA34": "VIIAY", "VIVA34": "VIV",
    "WEGE34": "WEGZY", "YDUQ34": "YDUQ", "AZUL34": "AZUL", "EMBR34": "ERJ",
    "GOLL34": "GOL", "BEEF34": "BSBR", "BPAC34": "BPAC", "BRML34": "BRML",
    "BBDC34": "BBD", "BBAS34": "BBAR", "BBSE34": "BBSEY", "BRFS34": "BRFS",
    "BRKM34": "BRKM", "BRPR34": "BRPR", "BRSR34": "BSRR", "CBEE34": "CBEE",
    "CCRO34": "CCRO", "CESP34": "CESP", "CMIG34": "CIG", "CPFE34": "CPFE",
    "CPLE34": "CPL", "CSMG34": "CSMG", "CSNA34": "SID", "CYRE34": "CYRBY",
    "DTEX34": "DTEX", "ECOR34": "ECOR", "EGIE34": "EGIE", "ELET34": "ELP",
    "ELET34": "ELP", "ELPL34": "ELPL", "EMBR34": "ERJ", "ENBR34": "EBR",
    "ENGI34": "ENGIY", "EQTL34": "EQTL", "EZTC34": "EZTCY", "FLRY34": "FLY",
    "GFSA34": "GFSA", "GGBR34": "GGB", "GOAU34": "GOUA", "GOLL34": "GOL",
    "HGTX34": "HGTX", "HYPE34": "HYPE", "IGTA34": "IGTA", "IRBR34": "IRBR",
    "ITSA34": "ITSA", "ITUB34": "ITUB", "JBSS34": "JBSAY", "JHSF34": "JHSF",
    "KLBN34": "KLBAY", "LAME34": "LAME", "LIGT34": "LIGT", "LINX34": "LINX",
    "LLIS34": "LLIS", "LOGG34": "LOGG", "LREN34": "LRENY", "MDIA34": "MDIA",
    "MEAL34": "MEAL", "MGLU34": "MGLU", "MOVI34": "MOVI", "MRFG34": "MRFG",
    "MRVE34": "MRVE", "MULT34": "MULT", "MYPK34": "MYPK", "NATU34": "NU",
    "ODPV34": "ODPV", "OIBR34": "OIBR", "PARD34": "PARD", "PCAR34": "PCAR",
    "PETR34": "PBR", "POMO34": "POMO", "POSI34": "POSI", "PRIO34": "PRIO",
    "QUAL34": "QUAL", "RADL34": "RADL", "RAIL34": "RAIL", "RAPT34": "RAPT",
    "RDNI34": "RDNI", "RDOR34": "RDOR", "RENT34": "RENT", "SANB34": "SAN",
    "SAPR34": "SAPR", "SBSP34": "SBS", "SEER34": "SEER", "SLCE34": "SLCE",
    "SMLE34": "SMLE", "SMTO34": "SMTO", "SQIA34": "SQ", "STBP34": "STT",
    "SULA34": "SULA", "SUZB34": "SUZ", "TAEE34": "TAEEY", "TASA34": "TASA",
    "TCSL34": "TCSL", "TGMA34": "TGMA", "TIMP34": "TIM", "TOTS34": "TOTS",
    "TPIS34": "TPIS", "TRIS34": "TRIS", "TRPL34": "TRP", "TUPY34": "TUPY",
    "UGPA34": "UGP", "UNIP34": "UNIP", "USIM34": "USIM", "VALE34": "VALE",
    "VIVA34": "VIV", "VIVR34": "VIVR", "VIVT34": "VIVT", "VLID34": "VLID",
    "VULC34": "VULC", "WEGE34": "WEGZY", "WIZS34": "WIZS", "YDUQ34": "YDUQ",
}

BDR_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vQbWlK_G7LsoWhbndYp6JziXblqFMESL7S4aiCtZm_ucnbTHBC7P702SVArGbLFFMqMbAQzka1FmVoE/pub?output=csv"
)

class BDRMapper:
    def __init__(self):
        self._mapping: Dict[str, str] = {}
        self._loaded = False
    
    def load(self) -> Dict[str, str]:
        """Load BDR mapping from public CSV, fallback to manual mapping."""
        if self._loaded:
            return self._mapping
        
        # Try CSV first
        try:
            df = pd.read_csv(BDR_CSV_URL)
            # Expected columns: BDR, COD, Nome, ...
            if "BDR" in df.columns and "COD" in df.columns:
                mapping = dict(zip(df["BDR"].astype(str).str.strip().str.upper(),
                                   df["COD"].astype(str).str.strip().str.upper()))
                self._mapping = mapping
                print(f"[BDRMapper] Loaded {len(mapping)} BDRs from CSV.")
            else:
                raise ValueError("CSV columns not as expected")
        except Exception as e:
            print(f"[BDRMapper] CSV failed ({e}), using manual fallback.")
            self._mapping = BDR_MANUAL_MAP.copy()
        
        self._loaded = True
        return self._mapping
    
    def get_underlying(self, bdr_ticker: str) -> Optional[str]:
        """Get underlying ticker for a BDR ticker."""
        if not self._loaded:
            self.load()
        
        bdr = bdr_ticker.upper().strip()
        
        # 1. Direct mapping from CSV/manual
        if bdr in self._mapping:
            return self._mapping[bdr]
        
        # 2. Heuristic: if ends with 2 digits, strip them (e.g., IBIT39 -> IBIT)
        if len(bdr) >= 3 and bdr[-2:].isdigit():
            candidate = bdr[:-2]
            if candidate and len(candidate) >= 2:
                return candidate
        
        # 3. Heuristic: if contains a dot, it's likely a direct US ticker (e.g., BRK.B)
        if "." in bdr:
            return bdr
        
        # 4. Unknown
        return None
    
    def is_known_bdr(self, bdr_ticker: str) -> bool:
        """Check if we have a valid mapping for this BDR."""
        return self.get_underlying(bdr_ticker) is not None
    
    def get_bdr_for_underlying(self, underlying: str) -> Optional[str]:
        """Reverse lookup: find BDR ticker for an underlying."""
        if not self._loaded:
            self.load()
        underlying = underlying.upper()
        for bdr, und in self._mapping.items():
            if und == underlying:
                return bdr
        return None
    
    def is_bdr(self, ticker: str) -> bool:
        if not self._loaded:
            self.load()
        return ticker.upper() in self._mapping
    
    def all_bdrs(self) -> list:
        if not self._loaded:
            self.load()
        return list(self._mapping.keys())
    
    def all_underlyings(self) -> list:
        if not self._loaded:
            self.load()
        return list(self._mapping.values())
