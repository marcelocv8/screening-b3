import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime

st.set_page_config(
    page_title="Screening B3",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ═══════════════════════════════════════════════════════════════
# TERMINAL BLACK THEME — CSS via st.html (Cloud-compatible)
# ═══════════════════════════════════════════════════════════════
st.html("""
<style>
  /* Force dark background on main container */
  .stApp {
    background-color: #050505 !important;
  }
  .main .block-container {
    background-color: #050505 !important;
    padding-top: 2rem;
    max-width: 1400px;
  }
  /* Metric cards */
  [data-testid="stMetric"] {
    background-color: #0f0f0f;
    border: 1px solid #1a1a1a;
    border-radius: 12px;
    padding: 16px;
  }
  [data-testid="stMetric"] > div {
    color: #e0e0e0;
  }
  [data-testid="stMetricValue"] {
    font-family: 'SF Mono', Monaco, monospace !important;
    font-weight: 800 !important;
    letter-spacing: -1px;
    color: #00e676 !important;
  }
  [data-testid="stMetricLabel"] {
    color: #666 !important;
    font-size: 10px !important;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    font-weight: 700 !important;
  }
  /* Sidebar */
  section[data-testid="stSidebar"] {
    background-color: #0a0a0a !important;
    border-right: 1px solid #1a1a1a;
  }
  section[data-testid="stSidebar"] .stMarkdown {
    color: #888;
  }
  /* Tabs */
  .stTabs [data-baseweb="tab-list"] {
    gap: 8px;
  }
  .stTabs [data-baseweb="tab"] {
    background-color: #0f0f0f;
    border: 1px solid #1a1a1a;
    border-radius: 10px;
    color: #666;
    font-size: 12px;
    font-weight: 700;
  }
  .stTabs [data-baseweb="tab-highlight"] {
    background-color: #00e676;
  }
  .stTabs [aria-selected="true"] {
    background-color: #00e676 !important;
    color: #000 !important;
    border-color: #00e676 !important;
  }
  /* DataFrames */
  .stDataFrame {
    background-color: #0f0f0f;
    border: 1px solid #1a1a1a;
    border-radius: 12px;
  }
  .stDataFrame th {
    background-color: #141414 !important;
    color: #666 !important;
    font-size: 10px !important;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    font-weight: 700 !important;
  }
  .stDataFrame td {
    border-bottom: 1px solid #1a1a1a !important;
    color: #e0e0e0 !important;
    font-size: 13px !important;
  }
  .stDataFrame tr:hover td {
    background-color: rgba(0, 230, 118, 0.04) !important;
  }
  /* Plotly charts */
  .js-plotly-plot .plotly {
    background-color: #0f0f0f !important;
  }
</style>
""")

RESULTS_DIR = Path("results")

def load_data():
    parquet_path = RESULTS_DIR / "latest.parquet"
    summary_path = RESULTS_DIR / "summary.json"
    breadth_path = RESULTS_DIR / "breadth_score.json"
    ai_path = RESULTS_DIR / "ai_opinion.json"
    
    summary = {}
    breadth = {}
    ai_opinion = {}
    
    if summary_path.exists():
        import json
        with open(summary_path, "r") as f:
            summary = json.load(f)
    
    if breadth_path.exists():
        import json
        with open(breadth_path, "r") as f:
            breadth = json.load(f)
    
    if ai_path.exists():
        import json
        with open(ai_path, "r") as f:
            ai_opinion = json.load(f)
    
    if not parquet_path.exists():
        return pd.DataFrame(), summary, breadth, ai_opinion
    
    df = pd.read_parquet(parquet_path)
    return df, summary, breadth, ai_opinion

def get_pattern_badges(row, weekly=False):
    """Return plain-text badges with emojis for st.dataframe compatibility."""
    suffix = "_weekly" if weekly else ""
    badges = []
    if row.get(f"vcp{suffix}"):
        conf = row.get(f"vcp{suffix}_conf", row.get("vcp_conf", 0))
        badges.append(f"🌀 VCP({conf:.0%})")
    if row.get(f"wedge{suffix}"):
        conf = row.get(f"wedge{suffix}_conf", row.get("wedge_conf", 0))
        badges.append(f"🔺 WEDGE({conf:.0%})")
    if row.get(f"cup_handle{suffix}"):
        conf = row.get(f"cup_handle{suffix}_conf", row.get("cup_handle_conf", 0))
        badges.append(f"🏆 C&H({conf:.0%})")
    if row.get(f"double_bottom{suffix}"):
        conf = row.get(f"double_bottom{suffix}_conf", row.get("double_bottom_conf", 0))
        badges.append(f"⬆️ DB({conf:.0%})")
    if row.get(f"inverse_hs{suffix}"):
        conf = row.get(f"inverse_hs{suffix}_conf", row.get("inverse_hs_conf", 0))
        badges.append(f"👤 IH&S({conf:.0%})")
    if row.get(f"pre_breakout{suffix}"):
        badges.append("⏳ PRE-BO")
    if row.get(f"breakout{suffix}"):
        badges.append("🔥 BREAKOUT")
    return " | ".join(badges) if badges else "—"

def color_fund_tag(val):
    colors = {
        "Forte": "background-color: #00e67620; color: #00e676; font-weight: bold",
        "OK": "background-color: #ffd60020; color: #ffd600",
        "Fraco": "background-color: #ff525220; color: #ff5252"
    }
    return colors.get(val, "")

def render_accelerometer(score):
    """Render an accelerometer gauge from red to green for allocation score 1-5."""
    positions = {1: 5, 2: 25, 3: 50, 4: 75, 5: 95}
    left_pct = positions.get(score, 50)
    
    colors = {1: "#ff5252", 2: "#ff9100", 3: "#ffd600", 4: "#69f0ae", 5: "#00e676"}
    score_color = colors.get(score, "#ffd600")
    
    return f"""
    <div style="margin-bottom: 10px;">
        <div style="font-size: 11px; text-transform: uppercase; letter-spacing: 1.5px; color: #666; margin-bottom: 8px; font-weight: 700;">
            Aceleração do Mercado
        </div>
        <div style="position: relative; height: 24px; border-radius: 12px; background: linear-gradient(90deg, #ff5252 0%, #ff9100 25%, #ffd600 50%, #69f0ae 75%, #00e676 100%); border: 1px solid #333;">
            <div style="position: absolute; top: -6px; left: {left_pct}%; transform: translateX(-50%); width: 0; height: 0; border-left: 8px solid transparent; border-right: 8px solid transparent; border-top: 10px solid {score_color};"></div>
            <div style="position: absolute; top: -22px; left: {left_pct}%; transform: translateX(-50%); background: {score_color}; color: #000; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 800; font-family: monospace;">
                {score}
            </div>
        </div>
        <div style="display: flex; justify-content: space-between; margin-top: 4px; font-size: 10px; color: #444; font-weight: 600;">
            <span>1</span><span>2</span><span>3</span><span>4</span><span>5</span>
        </div>
    </div>
    """

# ═══════════════════════════════════════════════════════════════
# TERMINAL BLACK THEME CSS
# ═══════════════════════════════════════════════════════════════
st.markdown("""
<style>
    /* Global */
    .stApp {
        background-color: #050505;
        color: #e0e0e0;
    }
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #0a0a0a;
        border-right: 1px solid #1a1a1a;
    }
    section[data-testid="stSidebar"] .stMarkdown {
        color: #888;
    }
    section[data-testid="stSidebar"] h1, 
    section[data-testid="stSidebar"] h2, 
    section[data-testid="stSidebar"] h3 {
        color: #e0e0e0 !important;
    }
    
    /* Widgets */
    .stMultiSelect [data-baseweb="select"] {
        background-color: #0f0f0f !important;
        border-color: #1a1a1a !important;
    }
    .stSlider [data-testid="stThumbValue"] {
        color: #00e676 !important;
    }
    .stSlider [data-testid="stTickBarMin"],
    .stSlider [data-testid="stTickBarMax"] {
        color: #555 !important;
    }
    
    /* Buttons / Segmented Control */
    .stSegmentedControl [data-baseweb="segmented-control"] {
        background-color: #0f0f0f !important;
        border: 1px solid #1a1a1a !important;
    }
    
    /* DataFrames */
    .stDataFrame {
        background-color: #0f0f0f;
        border: 1px solid #1a1a1a;
        border-radius: 12px;
    }
    .stDataFrame th {
        background-color: #141414 !important;
        color: #666 !important;
        font-size: 10px !important;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 700 !important;
    }
    .stDataFrame td {
        border-bottom: 1px solid #1a1a1a !important;
        color: #e0e0e0 !important;
        font-size: 13px !important;
    }
    .stDataFrame tr:hover td {
        background-color: rgba(0, 230, 118, 0.04) !important;
    }
    
    /* Metric cards */
    [data-testid="stMetric"] {
        background-color: #0f0f0f;
        border: 1px solid #1a1a1a;
        border-radius: 12px;
        padding: 16px;
    }
    [data-testid="stMetric"] label {
        color: #666 !important;
        font-size: 10px !important;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 700 !important;
    }
    [data-testid="stMetricValue"] {
        font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace !important;
        font-weight: 800 !important;
        letter-spacing: -1px;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background-color: transparent;
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #0f0f0f;
        border: 1px solid #1a1a1a;
        border-radius: 10px;
        color: #666;
        font-size: 12px;
        font-weight: 700;
    }
    .stTabs [data-baseweb="tab-highlight"] {
        background-color: #00e676;
    }
    .stTabs [aria-selected="true"] {
        background-color: #00e676 !important;
        color: #000 !important;
        border-color: #00e676 !important;
    }
    
    /* Plotly chart background */
    .js-plotly-plot .plotly {
        background-color: #0f0f0f !important;
    }
</style>
""", unsafe_allow_html=True)

# Load data
df, summary, breadth, ai_opinion = load_data()

# ═══════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════
st.sidebar.title("📊 Screening B3")

if summary:
    st.sidebar.markdown(f"**Data:** {summary.get('date', 'N/A')}")
    st.sidebar.markdown(f"**Ativos:** {summary.get('qualified', 0)}")
    st.sidebar.markdown(f"**Tier S:** {summary.get('tier_s', 0)} | **A:** {summary.get('tier_a', 0)} | **B:** {summary.get('tier_b', 0)}")
    st.sidebar.markdown(f"**Breakouts:** {summary.get('breakouts', 0)} | **VCPs:** {summary.get('vcps', 0)} | **Wedges:** {summary.get('wedges', 0)}")
    st.sidebar.markdown(f"**Fund. Forte:** {summary.get('fund_strong', 0)} | **OK:** {summary.get('fund_ok', 0)}")

st.sidebar.markdown("---")

# Filters
st.sidebar.subheader("Filtros")

tier_filter = st.sidebar.multiselect(
    "Tier Técnico",
    options=["S", "A", "B", "C"],
    default=["S", "A", "B"]
)

fund_filter = st.sidebar.multiselect(
    "Tag Fundamentalista",
    options=["Forte", "OK", "Fraco"],
    default=["Forte", "OK", "Fraco"]
)

category_filter = st.sidebar.multiselect(
    "Categoria",
    options=["BR_STOCK", "ETF", "BDR"],
    default=["BR_STOCK", "ETF", "BDR"]
)

pattern_filters = st.sidebar.multiselect(
    "Padrões",
    options=["VCP", "Wedge", "Cup & Handle", "Double Bottom", "Inverse H&S", "Breakout", "Pre-Breakout"],
    default=[]
)

min_tech_score = st.sidebar.slider("Score Técnico mínimo", 0.0, 20.0, 0.0, 0.5)

# ═══════════════════════════════════════════════════════════════
# MAIN CONTENT
# ═══════════════════════════════════════════════════════════════
st.title("🚀 Screening Momentum")

if df.empty:
    st.warning("Nenhum resultado encontrado. O screening ainda não foi executado ou os arquivos não estão disponíveis.")
    st.info("O screening roda automaticamente às 16:20 (BRT) em dias úteis via GitHub Actions.")
    st.stop()

# ═══════════════════════════════════════════════════════════════
# MARKET BREADTH HEADER
# ═══════════════════════════════════════════════════════════════
st.markdown("---")

if breadth:
    alloc_score = breadth.get("allocation_score", 3)
    regime = breadth.get("regime", "Neutro")
    alloc_pct = breadth.get("allocation_pct", "40-60%")
    total_signals = breadth.get("total_signals", 0)
    avg_signals = breadth.get("avg_signals", 0)
    signal_vs_avg = breadth.get("signal_vs_avg", 1.0)
    
    score_colors = {5: "#00e676", 4: "#69f0ae", 3: "#ffd600", 2: "#ff9100", 1: "#ff5252"}
    score_color = score_colors.get(alloc_score, "#ffd600")
    
    hcol1, hcol2, hcol3 = st.columns([1.2, 1.8, 2.2])
    
    with hcol1:
        # Accelerometer gauge
        st.markdown(render_accelerometer(alloc_score), unsafe_allow_html=True)
        st.markdown(
            f"""
            <div style="text-align: center; margin-top: 8px;">
                <div style="font-size: 28px; font-weight: 800; font-family: monospace; color: {score_color};">{alloc_score}/5</div>
                <div style="font-size: 11px; color: #555; text-transform: uppercase; letter-spacing: 1px; font-weight: 700;">{alloc_pct}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with hcol2:
        st.markdown(f"**Regime:** <span style='color: {score_color}; font-weight: 700;'>{regime}</span>", unsafe_allow_html=True)
        st.markdown(
            f"""
            <div style="font-size: 12px; color: #888; line-height: 1.8;">
                % acima MME50: <strong style="color: #e0e0e0;">{breadth.get('pct_above_sma50', 0)}%</strong><br>
                % acima MME200: <strong style="color: #e0e0e0;">{breadth.get('pct_above_sma200', 0)}%</strong><br>
                Breakouts: <strong style="color: #e0e0e0;">{breadth.get('breakout_count', 0)}</strong> | 
                VCPs: <strong style="color: #e0e0e0;">{breadth.get('vcp_count', 0)}</strong><br>
                Sinais hoje: <strong style="color: {score_color};">{total_signals}</strong> 
                (média: {avg_signals:.1f} | vs média: {signal_vs_avg:.2f}x)
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with hcol3:
        if ai_opinion and ai_opinion.get("opinion"):
            source_icon = "🤖" if ai_opinion.get("has_ai") else "⚠️"
            source_label = "Gemini" if ai_opinion.get("has_ai") else "Fallback"
            opinion_text = ai_opinion["opinion"].replace("\n", "<br>")
            
            st.markdown(
                f"""
                <div style="background-color: #141414; border: 1px solid #2a2a2a; border-left: 3px solid #ffd600; border-radius: 12px; padding: 16px; height: 100%;">
                    <div style="font-size: 11px; text-transform: uppercase; letter-spacing: 1px; color: #ffd600; font-weight: 700; margin-bottom: 10px;">
                        {source_icon} Parecer da IA <span style="opacity: 0.5;">({source_label})</span>
                    </div>
                    <div style="font-size: 13px; color: #bbb; line-height: 1.7; max-height: 180px; overflow-y: auto;">
                        {opinion_text}
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                """
                <div style="background-color: #141414; border: 1px solid #2a2a2a; border-radius: 12px; padding: 16px; height: 100%; display: flex; align-items: center; justify-content: center;">
                    <span style="color: #555; font-size: 13px;">Parecer da IA não disponível.</span>
                </div>
                """,
                unsafe_allow_html=True
            )

# Toggle timeframe
st.markdown("---")
timeframe = st.segmented_control("Timeframe", ["📅 Diário", "📆 Semanal"], default="📅 Diário")
use_weekly = (timeframe == "📆 Semanal")

# Determine columns based on toggle
score_col = "technical_score_weekly" if use_weekly else "technical_score"
tier_col = "technical_tier_weekly" if use_weekly else "technical_tier"
pattern_suffix = "_weekly" if use_weekly else ""

# Ensure columns exist (fallback to daily if weekly not present)
if score_col not in df.columns:
    score_col = "technical_score"
if tier_col not in df.columns:
    tier_col = "technical_tier"

# Apply filters
filtered = df.copy()
filtered = filtered[filtered[tier_col].isin(tier_filter)]
filtered = filtered[filtered["fundamental_tag"].isin(fund_filter)]
filtered = filtered[filtered["category"].isin(category_filter)]
filtered = filtered[filtered[score_col] >= min_tech_score]

if pattern_filters:
    mask = pd.Series([False] * len(filtered), index=filtered.index)
    if "VCP" in pattern_filters:
        mask |= filtered[f"vcp{pattern_suffix}"] if f"vcp{pattern_suffix}" in filtered.columns else filtered["vcp"]
    if "Wedge" in pattern_filters:
        mask |= filtered[f"wedge{pattern_suffix}"] if f"wedge{pattern_suffix}" in filtered.columns else filtered["wedge"]
    if "Cup & Handle" in pattern_filters:
        mask |= filtered[f"cup_handle{pattern_suffix}"] if f"cup_handle{pattern_suffix}" in filtered.columns else filtered["cup_handle"]
    if "Double Bottom" in pattern_filters:
        mask |= filtered[f"double_bottom{pattern_suffix}"] if f"double_bottom{pattern_suffix}" in filtered.columns else filtered["double_bottom"]
    if "Inverse H&S" in pattern_filters:
        mask |= filtered[f"inverse_hs{pattern_suffix}"] if f"inverse_hs{pattern_suffix}" in filtered.columns else filtered["inverse_hs"]
    if "Breakout" in pattern_filters:
        mask |= filtered[f"breakout{pattern_suffix}"] if f"breakout{pattern_suffix}" in filtered.columns else filtered["breakout"]
    if "Pre-Breakout" in pattern_filters:
        mask |= filtered[f"pre_breakout{pattern_suffix}"] if f"pre_breakout{pattern_suffix}" in filtered.columns else filtered["pre_breakout"]
    filtered = filtered[mask]

# ═══════════════════════════════════════════════════════════════
# TOP METRICS
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
col1, col2, col3, col4, col5, col6 = st.columns(6)

col1.metric("Filtrados", len(filtered))
col2.metric("Tier S", int((filtered[tier_col] == "S").sum()))
col3.metric("Tier A", int((filtered[tier_col] == "A").sum()))

vcp_val = int(filtered[f"vcp{pattern_suffix}"].sum()) if f"vcp{pattern_suffix}" in filtered.columns else int(filtered["vcp"].sum())
col4.metric("VCPs", vcp_val)

wedge_val = int(filtered[f"wedge{pattern_suffix}"].sum()) if f"wedge{pattern_suffix}" in filtered.columns else int(filtered["wedge"].sum())
col5.metric("Wedges", wedge_val)

bo_val = int(filtered[f"breakout{pattern_suffix}"].sum()) if f"breakout{pattern_suffix}" in filtered.columns else int(filtered["breakout"].sum())
col6.metric("Breakouts 🔥", bo_val)

# ═══════════════════════════════════════════════════════════════
# PATTERN SECTIONS
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("📌 Padrões Detectados" + (" — Semanal" if use_weekly else " — Diário"))

pat_tab1, pat_tab2, pat_tab3, pat_tab4, pat_tab5, pat_tab6 = st.tabs([
    "🔥 Breakouts", "🌀 VCPs", "🏆 Cup & Handle", "🔺 Wedges", "⬆️ Double Bottom", "👤 Inverse H&S"
])

def show_pattern_table(df_section, pattern_col, title):
    df_pat = df_section[df_section[pattern_col] == True].sort_values(score_col, ascending=False)
    if df_pat.empty:
        st.info(f"Nenhum {title} detectado no screening de hoje.")
        return
    
    cols = ["rank", "display", tier_col, score_col, "price", "breakout_vol_ratio", "fundamental_tag"]
    cols = [c for c in cols if c in df_pat.columns]
    disp = df_pat[cols].copy()
    disp.columns = ["Rank", "Ticker", "Tier", "Score Téc.", "Preço", "Ratio Vol", "Fund."][:len(cols)]
    st.dataframe(disp.head(20), use_container_width=True, hide_index=True)

with pat_tab1:
    show_pattern_table(filtered, f"breakout{pattern_suffix}" if f"breakout{pattern_suffix}" in filtered.columns else "breakout", "Breakout")
with pat_tab2:
    show_pattern_table(filtered, f"vcp{pattern_suffix}" if f"vcp{pattern_suffix}" in filtered.columns else "vcp", "VCP")
with pat_tab3:
    show_pattern_table(filtered, f"cup_handle{pattern_suffix}" if f"cup_handle{pattern_suffix}" in filtered.columns else "cup_handle", "Cup & Handle")
with pat_tab4:
    show_pattern_table(filtered, f"wedge{pattern_suffix}" if f"wedge{pattern_suffix}" in filtered.columns else "wedge", "Wedge")
with pat_tab5:
    show_pattern_table(filtered, f"double_bottom{pattern_suffix}" if f"double_bottom{pattern_suffix}" in filtered.columns else "double_bottom", "Double Bottom")
with pat_tab6:
    show_pattern_table(filtered, f"inverse_hs{pattern_suffix}" if f"inverse_hs{pattern_suffix}" in filtered.columns else "inverse_hs", "Inverse H&S")

# ═══════════════════════════════════════════════════════════════
# RANKING TABLE
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("🏆 Ranking Geral" + (" — Semanal" if use_weekly else " — Diário"))

display_df = filtered[["rank", "display", "name", "category", tier_col, score_col,
                        "fundamental_tag", "fundamental_score", "price",
                        "vcp", "wedge", "cup_handle", "double_bottom", "inverse_hs",
                        "pre_breakout", "breakout", "roe", "pl", "pvp"]].copy()

display_df["Padrões"] = filtered.apply(lambda row: get_pattern_badges(row, weekly=use_weekly), axis=1)
display_df = display_df[["rank", "display", "name", "category", tier_col, score_col, 
                          "fundamental_tag", "fundamental_score", "price", "Padrões", "roe", "pl", "pvp"]]
display_df.columns = ["Rank", "Ticker", "Nome", "Cat.", "Tier", "Score Téc.", "Tag Fund.", "Score Fund.", "Preço", "Padrões", "ROE", "P/L", "P/VP"]

# Color tier and fund tag
def color_tier(val):
    colors = {
        "S": "background-color: #00e67620; color: #00e676; font-weight: bold",
        "A": "background-color: #ffd60020; color: #ffd600; font-weight: bold",
        "B": "background-color: #448aff20; color: #448aff",
        "C": ""
    }
    return colors.get(val, "")

styled_df = display_df.style.map(color_tier, subset=["Tier"]).map(color_fund_tag, subset=["Tag Fund."])

st.dataframe(
    styled_df,
    use_container_width=True,
    hide_index=True,
    height=600
)

# ═══════════════════════════════════════════════════════════════
# CHARTS
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("📊 Visualizações")

chart_tab1, chart_tab2, chart_tab3 = st.tabs(["Score por Tier", "Distribuição de Padrões", "Fundamentalistas"])

with chart_tab1:
    tier_counts = filtered[tier_col].value_counts().reindex(["S", "A", "B", "C"], fill_value=0).reset_index()
    tier_counts.columns = ["Tier", "Count"]
    fig = px.bar(
        tier_counts, x="Tier", y="Count", color="Tier",
        color_discrete_map={"S": "#00e676", "A": "#ffd600", "B": "#448aff", "C": "#555555"},
        title="Distribuição por Tier Técnico",
        template="plotly_dark"
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#0f0f0f",
        font=dict(color="#e0e0e0"),
        title_font_color="#e0e0e0"
    )
    st.plotly_chart(fig, use_container_width=True)

with chart_tab2:
    pattern_counts = {
        "VCP": int(filtered[f"vcp{pattern_suffix}"].sum() if f"vcp{pattern_suffix}" in filtered.columns else filtered["vcp"].sum()),
        "Wedge": int(filtered[f"wedge{pattern_suffix}"].sum() if f"wedge{pattern_suffix}" in filtered.columns else filtered["wedge"].sum()),
        "Cup & Handle": int(filtered[f"cup_handle{pattern_suffix}"].sum() if f"cup_handle{pattern_suffix}" in filtered.columns else filtered["cup_handle"].sum()),
        "Double Bottom": int(filtered[f"double_bottom{pattern_suffix}"].sum() if f"double_bottom{pattern_suffix}" in filtered.columns else filtered["double_bottom"].sum()),
        "Inverse H&S": int(filtered[f"inverse_hs{pattern_suffix}"].sum() if f"inverse_hs{pattern_suffix}" in filtered.columns else filtered["inverse_hs"].sum()),
        "Pre-Breakout": int(filtered[f"pre_breakout{pattern_suffix}"].sum() if f"pre_breakout{pattern_suffix}" in filtered.columns else filtered["pre_breakout"].sum()),
        "Breakout": int(filtered[f"breakout{pattern_suffix}"].sum() if f"breakout{pattern_suffix}" in filtered.columns else filtered["breakout"].sum()),
    }
    pat_df = pd.DataFrame(list(pattern_counts.items()), columns=["Padrão", "Quantidade"])
    fig2 = px.bar(
        pat_df, x="Padrão", y="Quantidade", color="Padrão",
        title="Padrões Detectados",
        template="plotly_dark"
    )
    fig2.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#0f0f0f",
        font=dict(color="#e0e0e0"),
        title_font_color="#e0e0e0",
        showlegend=False
    )
    st.plotly_chart(fig2, use_container_width=True)

with chart_tab3:
    fund_counts = filtered["fundamental_tag"].value_counts().reindex(["Forte", "OK", "Fraco"], fill_value=0).reset_index()
    fund_counts.columns = ["Tag", "Count"]
    fig3 = px.pie(
        fund_counts, names="Tag", values="Count", color="Tag",
        color_discrete_map={"Forte": "#00e676", "OK": "#ffd600", "Fraco": "#ff5252"},
        title="Distribuição Fundamentalista",
        template="plotly_dark"
    )
    fig3.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e0e0e0"),
        title_font_color="#e0e0e0"
    )
    st.plotly_chart(fig3, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# TICKER DETAIL
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("🔍 Detalhe do Ativo")

selected_ticker = st.selectbox("Selecione um ticker para ver detalhes", filtered["display"].unique())

if selected_ticker:
    row = filtered[filtered["display"] == selected_ticker].iloc[0]
    
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Score Técnico", f"{row[score_col]:.2f}")
    c2.metric("Tier", row[tier_col])
    c3.metric("Score Fund.", f"{row['fundamental_score']:.1f}/5")
    c4.metric("Tag Fund.", row["fundamental_tag"])
    c5.metric("Preço", f"R$ {row['price']:.2f}" if row["category"] != "BDR" else f"$ {row['price']:.2f}")
    
    st.markdown(f"**Padrões:** {get_pattern_badges(row, weekly=use_weekly)}")
    
    # Fundamentals detail
    st.markdown("---")
    st.subheader("📋 Indicadores Fundamentalistas")
    
    fcol1, fcol2, fcol3, fcol4, fcol5 = st.columns(5)
    fcol1.metric("ROE", f"{row.get('roe', 0):.1f}%")
    fcol2.metric("P/L", f"{row.get('pl', 0):.1f}")
    fcol3.metric("P/VP", f"{row.get('pvp', 0):.1f}")
    fcol4.metric("Volume Fin.", f"R$ {row.get('volume_financeiro', 0):,.0f}")
    fcol5.metric("MME50", f"R$ {row.get('sma50', 0):.2f}")
    
    st.info("Gráficos detalhados serão adicionados em uma versão futura. Para análise visual, copie o ticker e abra no ProfitChart.")

# ═══════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown(
    f"""
    <div style="text-align: center; padding: 20px; color: #444; font-size: 12px; font-weight: 600; letter-spacing: 0.5px;">
        © Marcelo Vasconcelos | Screening Momentum v3.0 | Atualizado: {summary.get('date', 'N/A')}
    </div>
    """,
    unsafe_allow_html=True
)
