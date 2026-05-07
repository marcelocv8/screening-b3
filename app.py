import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime

st.set_page_config(
    page_title="Screening B3",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

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
    positions = {1: 5, 2: 25, 3: 50, 4: 75, 5: 95}
    left_pct = positions.get(score, 50)
    colors = {1: "#ff5252", 2: "#ff9100", 3: "#ffd600", 4: "#69f0ae", 5: "#00e676"}
    score_color = colors.get(score, "#ffd600")
    return f"""
    <div style="margin-bottom: 8px;">
        <div style="position: relative; height: 20px; border-radius: 10px; background: linear-gradient(90deg, #ff5252 0%, #ff9100 25%, #ffd600 50%, #69f0ae 75%, #00e676 100%);">
            <div style="position: absolute; top: -4px; left: {left_pct}%; transform: translateX(-50%); width: 0; height: 0; border-left: 6px solid transparent; border-right: 6px solid transparent; border-top: 8px solid {score_color};"></div>
            <div style="position: absolute; top: -18px; left: {left_pct}%; transform: translateX(-50%); background: {score_color}; color: #000; padding: 1px 6px; border-radius: 4px; font-size: 11px; font-weight: 800; font-family: monospace;">{score}</div>
        </div>
        <div style="display: flex; justify-content: space-between; margin-top: 2px; font-size: 9px; color: #888;">
            <span>1</span><span>2</span><span>3</span><span>4</span><span>5</span>
        </div>
    </div>
    """

# ═══════════════════════════════════════════════════════════════
# DARK SUAVE THEME (via config.toml nativo)
# ═══════════════════════════════════════════════════════════════
st.html("""
<style>
  .stApp { background-color: #1a1a2e !important; }
  .main .block-container { background-color: #1a1a2e !important; padding-top: 1.5rem; max-width: 1400px; }
  [data-testid="stMetric"] { background-color: #16213e; border: 1px solid #0f3460; border-radius: 10px; padding: 12px; }
  [data-testid="stMetricValue"] { color: #00e676 !important; font-family: monospace !important; font-weight: 800 !important; }
  [data-testid="stMetricLabel"] { color: #8892b0 !important; font-size: 10px !important; text-transform: uppercase; letter-spacing: 1px; }
  .stTabs [data-baseweb="tab"] { background-color: #16213e; border: 1px solid #0f3460; border-radius: 8px; color: #8892b0; font-size: 12px; font-weight: 600; }
  .stTabs [aria-selected="true"] { background-color: #00e676 !important; color: #1a1a2e !important; border-color: #00e676 !important; }
  .stDataFrame th { background-color: #16213e !important; color: #8892b0 !important; font-size: 10px !important; text-transform: uppercase; letter-spacing: 1px; font-weight: 700 !important; }
  .stDataFrame td { border-bottom: 1px solid #0f3460 !important; color: #ccd6f6 !important; font-size: 13px !important; }
  .stDataFrame tr:hover td { background-color: rgba(0, 230, 118, 0.05) !important; }
</style>
""")

# Load data
df, summary, breadth, ai_opinion = load_data()

# ═══════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════
st.title("🚀 Screening Momentum")

if df.empty:
    st.warning("Nenhum resultado encontrado. O screening ainda não foi executado.")
    st.info("O screening roda automaticamente às 16:20 (BRT) em dias úteis via GitHub Actions.")
    st.stop()

# Toggle timeframe
st.markdown("---")
timeframe = st.segmented_control("Timeframe", ["📅 Diário", "📆 Semanal"], default="📅 Diário")
use_weekly = (timeframe == "📆 Semanal")

# Determine columns
score_col = "technical_score_weekly" if use_weekly else "technical_score"
tier_col = "technical_tier_weekly" if use_weekly else "technical_tier"
pattern_suffix = "_weekly" if use_weekly else ""

if score_col not in df.columns:
    score_col = "technical_score"
if tier_col not in df.columns:
    tier_col = "technical_tier"

# ═══════════════════════════════════════════════════════════════
# MARKET BREADTH + IA
# ═══════════════════════════════════════════════════════════════
if breadth:
    alloc_score = breadth.get("allocation_score", 3)
    regime = breadth.get("regime", "Neutro")
    alloc_pct = breadth.get("allocation_pct", "40-60%")
    total_signals = breadth.get("total_signals", 0)
    avg_signals = breadth.get("avg_signals", 0)
    signal_vs_avg = breadth.get("signal_vs_avg", 1.0)
    score_colors = {5: "#00e676", 4: "#69f0ae", 3: "#ffd600", 2: "#ff9100", 1: "#ff5252"}
    score_color = score_colors.get(alloc_score, "#ffd600")
    
    hcol1, hcol2, hcol3 = st.columns([1.1, 1.7, 2.0])
    
    with hcol1:
        st.markdown(render_accelerometer(alloc_score), unsafe_allow_html=True)
        st.markdown(f"<div style='text-align:center;'><div style='font-size:24px; font-weight:800; font-family:monospace; color:{score_color};'>{alloc_score}/5</div><div style='font-size:11px; color:#8892b0;'>{alloc_pct}</div></div>", unsafe_allow_html=True)
    
    with hcol2:
        st.markdown(f"**Regime:** <span style='color:{score_color}; font-weight:700;'>{regime}</span>", unsafe_allow_html=True)
        st.markdown(f"""
        <div style="font-size:12px; color:#8892b0; line-height:1.7;">
        % > MME50: <strong style="color:#ccd6f6;">{breadth.get('pct_above_sma50', 0)}%</strong><br>
        % > MME200: <strong style="color:#ccd6f6;">{breadth.get('pct_above_sma200', 0)}%</strong><br>
        Breakouts: <strong style="color:#ccd6f6;">{breadth.get('breakout_count', 0)}</strong> | VCPs: <strong style="color:#ccd6f6;">{breadth.get('vcp_count', 0)}</strong><br>
        Sinais: <strong style="color:{score_color};">{total_signals}</strong> (média: {avg_signals:.1f} | vs média: {signal_vs_avg:.2f}x)
        </div>
        """, unsafe_allow_html=True)
    
    with hcol3:
        if ai_opinion and ai_opinion.get("opinion"):
            source_icon = "🤖" if ai_opinion.get("has_ai") else "⚠️"
            source_label = "Gemini" if ai_opinion.get("has_ai") else "Fallback"
            opinion_text = ai_opinion["opinion"].replace("\n", "<br>")
            st.markdown(f"""
            <div style="background-color:#16213e; border:1px solid #0f3460; border-left:3px solid #ffd600; border-radius:10px; padding:14px; height:100%;">
                <div style="font-size:10px; text-transform:uppercase; letter-spacing:1px; color:#ffd600; font-weight:700; margin-bottom:8px;">{source_icon} Parecer da IA ({source_label})</div>
                <div style="font-size:12px; color:#ccd6f6; line-height:1.6; max-height:160px; overflow-y:auto;">{opinion_text}</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("<div style='background-color:#16213e; border:1px solid #0f3460; border-radius:10px; padding:14px; text-align:center; color:#8892b0; font-size:12px;'>Parecer da IA não disponível.</div>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# METRICS
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Total", len(df))
col2.metric("Tier S", int((df[tier_col] == "S").sum()))
col3.metric("Tier A", int((df[tier_col] == "A").sum()))

vcp_total = int(df[f"vcp{pattern_suffix}"].sum()) if f"vcp{pattern_suffix}" in df.columns else int(df["vcp"].sum())
col4.metric("VCPs", vcp_total)

wedge_total = int(df[f"wedge{pattern_suffix}"].sum()) if f"wedge{pattern_suffix}" in df.columns else int(df["wedge"].sum())
col5.metric("Wedges", wedge_total)

bo_total = int(df[f"breakout{pattern_suffix}"].sum()) if f"breakout{pattern_suffix}" in df.columns else int(df["breakout"].sum())
col6.metric("Breakouts 🔥", bo_total)

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
        st.info(f"Nenhum {title} detectado.")
        return
    cols = ["rank", "display", tier_col, score_col, "price", "breakout_vol_ratio", "fundamental_tag"]
    cols = [c for c in cols if c in df_pat.columns]
    disp = df_pat[cols].copy()
    disp.columns = ["Rank", "Ticker", "Tier", "Score Téc.", "Preço", "Ratio Vol", "Fund."][:len(cols)]
    st.dataframe(disp.head(20), use_container_width=True, hide_index=True)

with pat_tab1:
    show_pattern_table(df, f"breakout{pattern_suffix}" if f"breakout{pattern_suffix}" in df.columns else "breakout", "Breakout")
with pat_tab2:
    show_pattern_table(df, f"vcp{pattern_suffix}" if f"vcp{pattern_suffix}" in df.columns else "vcp", "VCP")
with pat_tab3:
    show_pattern_table(df, f"cup_handle{pattern_suffix}" if f"cup_handle{pattern_suffix}" in df.columns else "cup_handle", "Cup & Handle")
with pat_tab4:
    show_pattern_table(df, f"wedge{pattern_suffix}" if f"wedge{pattern_suffix}" in df.columns else "wedge", "Wedge")
with pat_tab5:
    show_pattern_table(df, f"double_bottom{pattern_suffix}" if f"double_bottom{pattern_suffix}" in df.columns else "double_bottom", "Double Bottom")
with pat_tab6:
    show_pattern_table(df, f"inverse_hs{pattern_suffix}" if f"inverse_hs{pattern_suffix}" in df.columns else "inverse_hs", "Inverse H&S")

# ═══════════════════════════════════════════════════════════════
# RANKING TABLE
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("🏆 Ranking Geral" + (" — Semanal" if use_weekly else " — Diário"))

display_df = df[["rank", "display", "name", "category", tier_col, score_col,
                 "fundamental_tag", "fundamental_score", "price",
                 "vcp", "wedge", "cup_handle", "double_bottom", "inverse_hs",
                 "pre_breakout", "breakout", "roe", "pl", "pvp"]].copy()

display_df["Padrões"] = df.apply(lambda row: get_pattern_badges(row, weekly=use_weekly), axis=1)
display_df = display_df[["rank", "display", "name", "category", tier_col, score_col, 
                          "fundamental_tag", "fundamental_score", "price", "Padrões", "roe", "pl", "pvp"]]
display_df.columns = ["Rank", "Ticker", "Nome", "Cat.", "Tier", "Score Téc.", "Tag Fund.", "Score Fund.", "Preço", "Padrões", "ROE", "P/L", "P/VP"]

def color_tier(val):
    colors = {
        "S": "background-color: #00e67620; color: #00e676; font-weight: bold",
        "A": "background-color: #ffd60020; color: #ffd600; font-weight: bold",
        "B": "background-color: #448aff20; color: #448aff",
        "C": ""
    }
    return colors.get(val, "")

styled_df = display_df.style.map(color_tier, subset=["Tier"]).map(color_fund_tag, subset=["Tag Fund."])

st.dataframe(styled_df, use_container_width=True, hide_index=True, height=600)

# ═══════════════════════════════════════════════════════════════
# CHARTS
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("📊 Visualizações")

chart_tab1, chart_tab2, chart_tab3 = st.tabs(["Score por Tier", "Distribuição de Padrões", "Fundamentalistas"])

with chart_tab1:
    tier_counts = df[tier_col].value_counts().reindex(["S", "A", "B", "C"], fill_value=0).reset_index()
    tier_counts.columns = ["Tier", "Count"]
    fig = px.bar(tier_counts, x="Tier", y="Count", color="Tier",
                 color_discrete_map={"S": "#00e676", "A": "#ffd600", "B": "#448aff", "C": "#555555"},
                 title="Distribuição por Tier Técnico", template="plotly_dark")
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#16213e", font=dict(color="#ccd6f6"))
    st.plotly_chart(fig, use_container_width=True)

with chart_tab2:
    pattern_counts = {
        "VCP": int(df[f"vcp{pattern_suffix}"].sum() if f"vcp{pattern_suffix}" in df.columns else df["vcp"].sum()),
        "Wedge": int(df[f"wedge{pattern_suffix}"].sum() if f"wedge{pattern_suffix}" in df.columns else df["wedge"].sum()),
        "Cup & Handle": int(df[f"cup_handle{pattern_suffix}"].sum() if f"cup_handle{pattern_suffix}" in df.columns else df["cup_handle"].sum()),
        "Double Bottom": int(df[f"double_bottom{pattern_suffix}"].sum() if f"double_bottom{pattern_suffix}" in df.columns else df["double_bottom"].sum()),
        "Inverse H&S": int(df[f"inverse_hs{pattern_suffix}"].sum() if f"inverse_hs{pattern_suffix}" in df.columns else df["inverse_hs"].sum()),
        "Pre-Breakout": int(df[f"pre_breakout{pattern_suffix}"].sum() if f"pre_breakout{pattern_suffix}" in df.columns else df["pre_breakout"].sum()),
        "Breakout": int(df[f"breakout{pattern_suffix}"].sum() if f"breakout{pattern_suffix}" in df.columns else df["breakout"].sum()),
    }
    pat_df = pd.DataFrame(list(pattern_counts.items()), columns=["Padrão", "Quantidade"])
    fig2 = px.bar(pat_df, x="Padrão", y="Quantidade", color="Padrão", title="Padrões Detectados", template="plotly_dark")
    fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#16213e", font=dict(color="#ccd6f6"), showlegend=False)
    st.plotly_chart(fig2, use_container_width=True)

with chart_tab3:
    fund_counts = df["fundamental_tag"].value_counts().reindex(["Forte", "OK", "Fraco"], fill_value=0).reset_index()
    fund_counts.columns = ["Tag", "Count"]
    fig3 = px.pie(fund_counts, names="Tag", values="Count", color="Tag",
                  color_discrete_map={"Forte": "#00e676", "OK": "#ffd600", "Fraco": "#ff5252"},
                  title="Distribuição Fundamentalista", template="plotly_dark")
    fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#ccd6f6"))
    st.plotly_chart(fig3, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# TICKER DETAIL
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("🔍 Detalhe do Ativo")

selected_ticker = st.selectbox("Selecione um ticker", df["display"].unique())

if selected_ticker:
    row = df[df["display"] == selected_ticker].iloc[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Score Técnico", f"{row[score_col]:.2f}")
    c2.metric("Tier", row[tier_col])
    c3.metric("Score Fund.", f"{row['fundamental_score']:.1f}/5")
    c4.metric("Tag Fund.", row["fundamental_tag"])
    c5.metric("Preço", f"R$ {row['price']:.2f}" if row["category"] != "BDR" else f"$ {row['price']:.2f}")
    st.markdown(f"**Padrões:** {get_pattern_badges(row, weekly=use_weekly)}")

# ═══════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown(f"<div style='text-align:center; padding:16px; color:#8892b0; font-size:11px;'>© Marcelo Vasconcelos | Screening Momentum v3.1 | {summary.get('date', 'N/A')}</div>", unsafe_allow_html=True)
