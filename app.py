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
    if row.get(f"wedge_or_trend{suffix}"):
        conf = row.get(f"wedge_or_trend{suffix}_conf", row.get("wedge_or_trend_conf", 0))
        badges.append(f"🔥 WEDGE/TREND({conf:.0%})")
    if row.get(f"vcp{suffix}"):
        conf = row.get(f"vcp{suffix}_conf", row.get("vcp_conf", 0))
        badges.append(f"🌀 VCP({conf:.0%})")
    if row.get(f"wedge{suffix}"):
        conf = row.get(f"wedge{suffix}_conf", row.get("wedge_conf", 0))
        badges.append(f"🔺 WEDGE CLÁSSICO({conf:.0%})")
    if row.get(f"cup_handle{suffix}"):
        conf = row.get(f"cup_handle{suffix}_conf", row.get("cup_handle_conf", 0))
        badges.append(f"🏆 C&H({conf:.0%})")
    if row.get(f"double_bottom{suffix}"):
        conf = row.get(f"double_bottom{suffix}_conf", row.get("double_bottom_conf", 0))
        badges.append(f"⬆️ DB({conf:.0%})")
    if row.get(f"pre_breakout{suffix}"):
        badges.append("⏳ PRE-BO")
    if row.get(f"breakout{suffix}"):
        badges.append("🔥 BREAKOUT")
    return " | ".join(badges) if badges else "—"

def color_fund_tag(val):
    colors = {
        "Forte": "background-color: #d4edda; color: #155724; font-weight: bold",
        "OK": "background-color: #fff3cd; color: #856404",
        "Fraco": "background-color: #f8d7da; color: #721c24"
    }
    return colors.get(val, "")

# Load data
df, summary, breadth, ai_opinion = load_data()

# ═══════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════
st.title("📈 Screening B3")

if df.empty:
    st.warning("Nenhum resultado encontrado. O screening ainda não foi executado.")
    st.info("O screening roda automaticamente às 16:20 (BRT) em dias úteis via GitHub Actions.")
    st.stop()

# Responsive CSS for mobile
st.markdown("""
<style>
    .stDataFrame { font-size: 12px !important; }
    @media (max-width: 768px) {
        .stDataFrame { font-size: 10px !important; }
        h1 { font-size: 1.5rem !important; }
        h2 { font-size: 1.2rem !important; }
        h3 { font-size: 1.0rem !important; }
    }
</style>
""", unsafe_allow_html=True)

# Toggle timeframe
st.markdown("---")
timeframe = st.radio("Timeframe", ["📅 Diário", "📆 Semanal"], horizontal=True)
use_weekly = (timeframe == "📆 Semanal")

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
    
    score_colors = {5: "#2ca02c", 4: "#7cb342", 3: "#ffbb33", 2: "#ff7043", 1: "#d62728"}
    score_color = score_colors.get(alloc_score, "#ffbb33")
    
    # Mobile-friendly: stack columns on small screens
    hcol1, hcol2 = st.columns([1, 1])
    
    with hcol1:
        st.metric("Alocação", f"{alloc_score}/5", alloc_pct)
        st.markdown(f"<div style='color:{score_color}; font-weight:bold; font-size:14px;'>{regime}</div>", unsafe_allow_html=True)
        st.markdown(f"**Market Breadth:**")
        st.markdown(f"""
        - % acima MME50: **{breadth.get('pct_above_sma50', 0)}%**
        - % acima MME200: **{breadth.get('pct_above_sma200', 0)}%**
        - Breakouts: **{breadth.get('breakout_count', 0)}** | VCPs: **{breadth.get('vcp_count', 0)}**
        - Sinais hoje: **{total_signals}** (média: {avg_signals:.1f} | vs média: {signal_vs_avg:.2f}x)
        """)
    
    with hcol2:
        if ai_opinion and ai_opinion.get("opinion"):
            source_icon = "🤖" if ai_opinion.get("has_ai") else "⚠️"
            source_label = "Gemini" if ai_opinion.get("has_ai") else "Fallback"
            with st.container(border=True):
                st.markdown(f"**{source_icon} Parecer da IA** ({source_label})")
                st.markdown(ai_opinion["opinion"])
        else:
            st.info("Parecer da IA não disponível.")

# ═══════════════════════════════════════════════════════════════
# METRICS - Mobile friendly (2-3 cols instead of 6)
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
# Use 3 columns for better mobile experience
col1, col2, col3 = st.columns(3)
col1.metric("Total", len(df))
col2.metric("Tier S", int((df[tier_col] == "S").sum()))
col3.metric("Tier A", int((df[tier_col] == "A").sum()))

wot_total = int(df[f"wedge_or_trend{pattern_suffix}"].sum()) if f"wedge_or_trend{pattern_suffix}" in df.columns else int(df.get("wedge_or_trend", pd.Series([0])).sum())
vcp_total = int(df[f"vcp{pattern_suffix}"].sum()) if f"vcp{pattern_suffix}" in df.columns else int(df["vcp"].sum())
wedge_total = int(df[f"wedge{pattern_suffix}"].sum()) if f"wedge{pattern_suffix}" in df.columns else int(df["wedge"].sum())
bo_total = int(df[f"breakout{pattern_suffix}"].sum()) if f"breakout{pattern_suffix}" in df.columns else int(df["breakout"].sum())

col4, col5, col6 = st.columns(3)
col4.metric("Wedge/Trend", wot_total)
col5.metric("VCPs", vcp_total)
col6.metric("Breakouts 🔥", bo_total)

# ═══════════════════════════════════════════════════════════════
# PATTERN SECTIONS
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("📌 Padrões Detectados" + (" — Semanal" if use_weekly else " — Diário"))

pat_tab1, pat_tab2, pat_tab3, pat_tab4, pat_tab5, pat_tab6 = st.tabs([
    "🔥 Wedge or Trend", "🌀 VCPs", "🏆 Cup & Handle", "🔺 Wedges Clássicos", "⬆️ Double Bottom", "🔥 Breakouts"
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
    show_pattern_table(df, f"wedge_or_trend{pattern_suffix}" if f"wedge_or_trend{pattern_suffix}" in df.columns else "wedge_or_trend", "Wedge or Trend")
with pat_tab2:
    show_pattern_table(df, f"vcp{pattern_suffix}" if f"vcp{pattern_suffix}" in df.columns else "vcp", "VCP")
with pat_tab3:
    show_pattern_table(df, f"cup_handle{pattern_suffix}" if f"cup_handle{pattern_suffix}" in df.columns else "cup_handle", "Cup & Handle")
with pat_tab4:
    show_pattern_table(df, f"wedge{pattern_suffix}" if f"wedge{pattern_suffix}" in df.columns else "wedge", "Wedge Clássico")
with pat_tab5:
    show_pattern_table(df, f"double_bottom{pattern_suffix}" if f"double_bottom{pattern_suffix}" in df.columns else "double_bottom", "Double Bottom")
with pat_tab6:
    show_pattern_table(df, f"breakout{pattern_suffix}" if f"breakout{pattern_suffix}" in df.columns else "breakout", "Breakout")

# ═══════════════════════════════════════════════════════════════
# RANKING TABLE
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("🏆 Ranking Geral" + (" — Semanal" if use_weekly else " — Diário"))

# Use a more compact display for mobile
display_df = df[["rank", "display", "name", "category", tier_col, score_col,
                 "fundamental_tag", "fundamental_score", "price",
                 "wedge_or_trend", "vcp", "wedge", "cup_handle", "double_bottom",
                 "pre_breakout", "breakout", "roe", "pl", "pvp"]].copy()

display_df["Padrões"] = df.apply(lambda row: get_pattern_badges(row, weekly=use_weekly), axis=1)
display_df = display_df[["rank", "display", "name", "category", tier_col, score_col, 
                          "fundamental_tag", "fundamental_score", "price", "Padrões", "roe", "pl", "pvp"]]
display_df.columns = ["Rank", "Ticker", "Nome", "Cat.", "Tier", "Score Téc.", "Tag Fund.", "Score Fund.", "Preço", "Padrões", "ROE", "P/L", "P/VP"]

def color_tier(val):
    colors = {
        "S": "background-color: #d4edda; color: #155724; font-weight: bold",
        "A": "background-color: #fff3cd; color: #856404; font-weight: bold",
        "B": "background-color: #d1ecf1; color: #0c5460",
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
                 color_discrete_map={"S": "#2ca02c", "A": "#ffbb33", "B": "#33b5e5", "C": "#999999"},
                 title="Distribuição por Tier Técnico")
    st.plotly_chart(fig, use_container_width=True)

with chart_tab2:
    pattern_counts = {
        "Wedge/Trend": int(df[f"wedge_or_trend{pattern_suffix}"].sum() if f"wedge_or_trend{pattern_suffix}" in df.columns else df.get("wedge_or_trend", pd.Series([0])).sum()),
        "VCP": int(df[f"vcp{pattern_suffix}"].sum() if f"vcp{pattern_suffix}" in df.columns else df["vcp"].sum()),
        "Wedge Clássico": int(df[f"wedge{pattern_suffix}"].sum() if f"wedge{pattern_suffix}" in df.columns else df["wedge"].sum()),
        "Cup & Handle": int(df[f"cup_handle{pattern_suffix}"].sum() if f"cup_handle{pattern_suffix}" in df.columns else df["cup_handle"].sum()),
        "Double Bottom": int(df[f"double_bottom{pattern_suffix}"].sum() if f"double_bottom{pattern_suffix}" in df.columns else df["double_bottom"].sum()),
        "Pre-Breakout": int(df[f"pre_breakout{pattern_suffix}"].sum() if f"pre_breakout{pattern_suffix}" in df.columns else df["pre_breakout"].sum()),
        "Breakout": int(df[f"breakout{pattern_suffix}"].sum() if f"breakout{pattern_suffix}" in df.columns else df["breakout"].sum()),
    }
    pat_df = pd.DataFrame(list(pattern_counts.items()), columns=["Padrão", "Quantidade"])
    fig2 = px.bar(pat_df, x="Padrão", y="Quantidade", color="Padrão", title="Padrões Detectados")
    st.plotly_chart(fig2, use_container_width=True)

with chart_tab3:
    fund_counts = df["fundamental_tag"].value_counts().reindex(["Forte", "OK", "Fraco"], fill_value=0).reset_index()
    fund_counts.columns = ["Tag", "Count"]
    fig3 = px.pie(fund_counts, names="Tag", values="Count", color="Tag",
                  color_discrete_map={"Forte": "#2ca02c", "OK": "#ffbb33", "Fraco": "#d62728"},
                  title="Distribuição Fundamentalista")
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
st.caption(f"Screening B3 v3.1 | Atualizado: {summary.get('date', 'N/A')} | Desenvolvido por Marcelo Vasconcelos")
