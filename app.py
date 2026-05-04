import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

st.set_page_config(
    page_title="Screening B3 - Minervini + Patterns",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

RESULTS_DIR = Path("results")

def load_data():
    """Load latest screening results."""
    parquet_path = RESULTS_DIR / "latest.parquet"
    summary_path = RESULTS_DIR / "summary.json"
    
    summary = {}
    if summary_path.exists():
        import json
        with open(summary_path, "r") as f:
            summary = json.load(f)
    
    if not parquet_path.exists():
        return pd.DataFrame(), summary
    
    df = pd.read_parquet(parquet_path)
    return df, summary

def get_pattern_badges(row):
    """Generate HTML badges for detected patterns."""
    badges = []
    if row.get("vcp"):
        badges.append(f"<span style='color:#1f77b4;font-weight:bold'>VCP({row.get('vcp_conf',0):.0%})</span>")
    if row.get("wedge"):
        badges.append(f"<span style='color:#9467bd;font-weight:bold'>WEDGE({row.get('wedge_conf',0):.0%})</span>")
    if row.get("cup_handle"):
        badges.append(f"<span style='color:#2ca02c;font-weight:bold'>C&H({row.get('cup_handle_conf',0):.0%})</span>")
    if row.get("double_bottom"):
        badges.append(f"<span style='color:#ff7f0e;font-weight:bold'>DB({row.get('double_bottom_conf',0):.0%})</span>")
    if row.get("inverse_hs"):
        badges.append(f"<span style='color:#d62728;font-weight:bold'>IH&S({row.get('inverse_hs_conf',0):.0%})</span>")
    if row.get("pre_breakout"):
        badges.append("<span style='color:#17becf;font-weight:bold'>PRE-BO</span>")
    if row.get("breakout"):
        badges.append("<span style='color:#ff4500;font-weight:bold'>🔥 BREAKOUT</span>")
    return " ".join(badges) if badges else "—"

# Load data
df, summary = load_data()

# Sidebar
st.sidebar.title("📊 Screening B3")

if summary:
    st.sidebar.markdown(f"**Data:** {summary.get('date', 'N/A')}")
    st.sidebar.markdown(f"**Ativos analisados:** {summary.get('qualified', 0)}")
    st.sidebar.markdown(f"**Tier S:** {summary.get('tier_s', 0)} | **A:** {summary.get('tier_a', 0)} | **B:** {summary.get('tier_b', 0)}")
    st.sidebar.markdown(f"**Breakouts hoje:** {summary.get('breakouts', 0)}")

st.sidebar.markdown("---")

# Filters
st.sidebar.subheader("Filtros")

tier_filter = st.sidebar.multiselect(
    "Tier mínimo",
    options=["S", "A", "B", "C"],
    default=["S", "A", "B"]
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

min_score = st.sidebar.slider("Score mínimo", 0.0, 20.0, 0.0, 0.5)

# Main content
st.title("🚀 Screening Momentum — Minervini + Qullamaggie")

if df.empty:
    st.warning("Nenhum resultado encontrado. O screening ainda não foi executado ou os arquivos não estão disponíveis.")
    st.info("O screening roda automaticamente às 16:20 (BRT) em dias úteis via GitHub Actions.")
    st.stop()

# Apply filters
filtered = df.copy()
filtered = filtered[filtered["tier"].isin(tier_filter)]
filtered = filtered[filtered["category"].isin(category_filter)]
filtered = filtered[filtered["score"] >= min_score]

if pattern_filters:
    mask = pd.Series([False] * len(filtered), index=filtered.index)
    if "VCP" in pattern_filters:
        mask |= filtered["vcp"]
    if "Wedge" in pattern_filters:
        mask |= filtered["wedge"]
    if "Cup & Handle" in pattern_filters:
        mask |= filtered["cup_handle"]
    if "Double Bottom" in pattern_filters:
        mask |= filtered["double_bottom"]
    if "Inverse H&S" in pattern_filters:
        mask |= filtered["inverse_hs"]
    if "Breakout" in pattern_filters:
        mask |= filtered["breakout"]
    if "Pre-Breakout" in pattern_filters:
        mask |= filtered["pre_breakout"]
    filtered = filtered[mask]

# Top stats
st.markdown("---")
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Filtrados", len(filtered))
col2.metric("Tier S", (filtered["tier"] == "S").sum())
col3.metric("Tier A", (filtered["tier"] == "A").sum())
col4.metric("VCPs", filtered["vcp"].sum())
col5.metric("Breakouts 🔥", filtered["breakout"].sum())

# Highlight breakout leaders
st.markdown("---")
st.subheader("🔥 Breakouts de Hoje (Volume ≥ 150% da média)")

breakouts = filtered[filtered["breakout"] == True].sort_values("score", ascending=False)
if not breakouts.empty:
    bo_display = breakouts[["rank", "display", "tier", "score", "price", "breakout_vol_ratio"]].copy()
    bo_display.columns = ["Rank", "Ticker", "Tier", "Score", "Preço", "Ratio Volume"]
    st.dataframe(bo_display.head(20), use_container_width=True, hide_index=True)
else:
    st.info("Nenhum breakout detectado no screening de hoje.")

# Top Ranking
st.markdown("---")
st.subheader("🏆 Ranking Geral")

# Prepare display columns
display_df = filtered[["rank", "display", "name", "category", "tier", "score", "price",
                        "vcp", "wedge", "cup_handle", "double_bottom", "inverse_hs",
                        "pre_breakout", "breakout", "roe", "pl", "pvp"]].copy()

display_df["Padrões"] = filtered.apply(get_pattern_badges, axis=1)
display_df = display_df[["rank", "display", "name", "category", "tier", "score", "price", "Padrões", "roe", "pl", "pvp"]]
display_df.columns = ["Rank", "Ticker", "Nome", "Categoria", "Tier", "Score", "Preço", "Padrões", "ROE", "P/L", "P/VP"]

# Color tier column
def color_tier(val):
    colors = {"S": "background-color: #d4edda; color: #155724; font-weight: bold",
              "A": "background-color: #fff3cd; color: #856404; font-weight: bold",
              "B": "background-color: #d1ecf1; color: #0c5460",
              "C": ""}
    return colors.get(val, "")

st.dataframe(
    display_df.style.applymap(color_tier, subset=["Tier"]),
    use_container_width=True,
    hide_index=True,
    height=600
)

# Charts
st.markdown("---")
st.subheader("📊 Visualizações")

chart_tab1, chart_tab2 = st.tabs(["Score por Tier", "Distribuição de Padrões"])

with chart_tab1:
    tier_counts = filtered["tier"].value_counts().reindex(["S", "A", "B", "C"], fill_value=0).reset_index()
    tier_counts.columns = ["Tier", "Count"]
    fig = px.bar(tier_counts, x="Tier", y="Count", color="Tier",
                 color_discrete_map={"S": "#2ca02c", "A": "#ffbb33", "B": "#33b5e5", "C": "#999999"},
                 title="Distribuição por Tier")
    st.plotly_chart(fig, use_container_width=True)

with chart_tab2:
    pattern_counts = {
        "VCP": filtered["vcp"].sum(),
        "Wedge": filtered["wedge"].sum(),
        "Cup & Handle": filtered["cup_handle"].sum(),
        "Double Bottom": filtered["double_bottom"].sum(),
        "Inverse H&S": filtered["inverse_hs"].sum(),
        "Pre-Breakout": filtered["pre_breakout"].sum(),
        "Breakout": filtered["breakout"].sum(),
    }
    pat_df = pd.DataFrame(list(pattern_counts.items()), columns=["Padrão", "Quantidade"])
    fig2 = px.bar(pat_df, x="Padrão", y="Quantidade", color="Padrão", title="Padrões Detectados")
    st.plotly_chart(fig2, use_container_width=True)

# Ticker detail
st.markdown("---")
st.subheader("🔍 Detalhe do Ativo")

selected_ticker = st.selectbox("Selecione um ticker para ver detalhes", filtered["display"].unique())

if selected_ticker:
    row = filtered[filtered["display"] == selected_ticker].iloc[0]
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Score", f"{row['score']:.2f}")
    c2.metric("Tier", row["tier"])
    c3.metric("Preço", f"R$ {row['price']:.2f}" if row["category"] != "BDR" else f"$ {row['price']:.2f}")
    c4.metric("Volume Financeiro", f"R$ {row['volume_financeiro']:,.0f}")
    
    st.markdown(f"**Padrões:** {get_pattern_badges(row)}")
    
    # Try to show mini chart from saved JSON if available
    json_path = RESULTS_DIR / f"screening_*.json"
    st.info("Gráficos detalhados serão adicionados em uma versão futura. Para análise visual, copie o ticker e abra no ProfitChart.")

# Footer
st.markdown("---")
st.caption(f"Screening B3 v1.0 | Atualizado: {summary.get('date', 'N/A')} | Desenvolvido por marcelocv8")
