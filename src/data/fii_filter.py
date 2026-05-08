"""
FII Filter: whitelist approach for tickers ending in '11'.
Any ticker ending in '11' NOT in the whitelist is treated as FII and excluded.
"""
from pathlib import Path
from typing import Set

ALLOWED_11_PATH = Path("data/allowed_11.txt")

# Essential tickers ending in 11 that are NOT FIIs (ETFs, Units, etc)
# Used as fallback if allowed_11.txt is missing
DEFAULT_ALLOWED_11 = {
    "BOVA11", "SMAL11", "IVVB11", "BRCR11", "KNRI11", "HGLG11", "XPLG11", "VISC11",
    "BPAC11", "ITUB11", "SANB11", "BBDC11", "BBAS11", "ABEV11", "PETR11",
    "VALE11", "WEGE11", "RENT11", "LREN11", "MGLU11", "VIVT11", "TOTS11",
    "B3SA11", "ENGI11", "TAEE11", "CPLE11", "SAPR11", "KLBN11", "ALUP11",
    "IGTI11", "BRBI11", "AZIN11", "MINE11", "BSGD11", "BDIV11", "COPN11",
    "ENDD11", "FINC11", "ESUT11", "ESUU11", "ESUD11", "NVRP11", "KNDI11",
    "KNOX11", "OPHF11", "PICE11", "PLBR11", "FPOR11", "RZDL11", "VIGT11",
    "XPIE11", "BMMT11", "AGRI11", "BBOV11", "BRAZ11", "DVER11", "BBOI11",
    "DOLA11", "CORN11", "BBSD11", "TECX11", "PKIN11", "OURO11", "SPYR11",
    "BREW11", "BCIC11", "BDEF11", "IBOB11", "ESGB11", "GOLB11", "DOLB11",
    "SPXB11", "SPBZ11", "GENB11", "SMAB11", "CMDB11", "AUVP11", "TIRB11",
    "BRXC11", "RICO11", "GDIV11", "XBCI11", "XSPI11", "PIPE11", "QQQQ11",
    "QQQI11", "COIN11", "FIXX11", "AURO11", "CASA11", "IWMI11", "SPYI11",
    "XBOV11", "GLDX11", "TRIG11", "BOVB11", "GXUS11", "BULZ11", "ARGE11",
    "UTLL11", "PEVC11", "BEST11", "BVBR11", "USTK11", "SVAL11", "CHIP11",
    "VWRA11", "IVWO11", "WRLD11", "GPUS11", "BDOM11", "BXPO11", "SCVB11",
    "ALUG11", "NUCL11", "BIZD11", "CAPE11", "EWBZ11", "BRAX11", "ECOO11",
    "SILK11", "GLDI11", "B3BR11", "BOVV11", "DIVO11", "DIVD11", "FIND11",
    "GOVE11", "MATB11", "ISUS11", "MILL11", "HTEK11", "TECK11", "PIBB11",
    "REVE11", "SPXR11", "SPXI11", "SMAC11", "NSDV11", "HIGH11", "LVOL11",
    "NBOV11", "NDIV11", "QLBR11", "SPUB11", "SPVT11", "BOVS11", "ELAS11",
    "GOLX11", "ACWI11", "XINA11", "UTEC11", "USAL11", "BOVX11", "GOLD11",
    "SLVR11", "NASD11", "DOLX11", "SPXH11",
}

def load_allowed_11() -> Set[str]:
    """Load whitelist of tickers ending in 11 that should be KEPT."""
    allowed = set(DEFAULT_ALLOWED_11)
    
    if not ALLOWED_11_PATH.exists():
        return allowed
    
    with open(ALLOWED_11_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            ticker = line.split()[0].upper().strip()
            if ticker:
                allowed.add(ticker)
    
    return allowed

def is_fii(ticker: str, allowed_set: Set[str]) -> bool:
    """
    Check if a ticker should be excluded as FII.
    Rule: if ticker ends with '11' and is NOT in the allowed whitelist, it's a FII.
    """
    t = ticker.upper()
    if not t.endswith("11"):
        return False
    return t not in allowed_set
