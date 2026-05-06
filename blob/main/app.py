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
    """Return plain-text badges with emojis for st.dataframe compatibility."""
    badges = []
    if row.get("vcp"):
        badges.append(f"🌀 VCP({row.get('vcp_conf',0):.0%})")
    if row.get("wedge"):
        badges.append(f"🔺 WEDGE({row.get('wedge_conf',0):.0%})")
    if row.get("cup_handle"):
        badges.append(f"🏆 C&H({row.get('cup_handle_conf',0):.0%})")
    if row.get("double_bottom"):
        badges.append(f"⬆️ DB({row.get('double_bottom_conf',0):.0%})")
    if row.get("inverse_hs"):
        badges.append(f"👤 IH&S({row.get('inverse_hs_conf',0):.0%})")
    if row.get("pre_breakout"):
        badges.append("⏳ PRE-BO")
    if row.get("breakout"):
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
df, summary = load_data()

# Sidebar
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

# Main content
st.title("🚀 Screening Momentum — Minervini + Qullamaggie")

if df.empty:
    st.warning("Nenhum resultado encontrado. O screening ainda não foi executado ou os arquivos não estão disponíveis.")
    st.info("O screening roda automaticamente às 16:20 (BRT) em dias úteis via GitHub Actions.")
    st.stop()

# Apply filters
filtered = df.copy()
filtered = filtered[filtered["technical_tier"].isin(tier_filter)]
filtered = filtered[filtered["fundamental_tag"].isin(fund_filter)]
filtered = filtered[filtered["category"].isin(category_filter)]
filtered = filtered[filtered["technical_score"] >= min_tech_score]

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
col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Total Filtrados", len(filtered))
col2.metric("Tier S", int((filtered["technical_tier"] == "S").sum()))
col3.metric("Tier A", int((filtered["technical_tier"] == "A").sum()))
col4.metric("VCPs", int(filtered["vcp"].sum()))
col5.metric("Wedges", int(filtered["wedge"].sum()))
col6.metric("Breakouts 🔥", int(filtered["breakout"].sum()))

# Pattern sections
st.markdown("---")
st.subheader("📌 Padrões Detectados")

pat_tab1, pat_tab2, pat_tab3, pat_tab4, pat_tab5, pat_tab6 = st.tabs([
    "🔥 Breakouts", "🌀 VCPs", "🏆 Cup & Handle", "🔺 Wedges", "⬆️ Double Bottom", "👤 Inverse H&S"
])

def show_pattern_table(df_section, pattern_col, title):
    df_pat = df_section[df_section[pattern_col] == True].sort_values("technical_score", ascending=False)
    if df_pat.empty:
        st.info(f"Nenhum {title} detectado no screening de hoje.")
        return
    
    cols = ["rank", "display", "technical_tier", "technical_score", "price", "breakout_vol_ratio", "fundamental_tag"]
    cols = [c for c in cols if c in df_pat.columns]
    disp = df_pat[cols].copy()
    disp.columns = ["Rank", "Ticker", "Tier", "Score Téc.", "Preço", "Ratio Vol", "Fund."][:len(cols)]
    st.dataframe(disp.head(20), use_container_width=True, hide_index=True)

with pat_tab1:
    show_pattern_table(filtered, "breakout", "Breakout")
with pat_tab2:
    show_pattern_table(filtered, "vcp", "VCP")
with pat_tab3:
    show_pattern_table(filtered, "cup_handle", "Cup & Handle")
with pat_tab4:
    show_pattern_table(filtered, "wedge", "Wedge")
with pat_tab5:
    show_pattern_table(filtered, "double_bottom", "Double Bottom")
with pat_tab6:
    show_pattern_table(filtered, "inverse_hs", "Inverse H&S")

# Top Ranking
st.markdown("---")
st.subheader("🏆 Ranking Geral (por Score Técnico)")

display_df = filtered[["rank", "display", "name", "category", "technical_tier", "technical_score",
                        "fundamental_tag", "fundamental_score", "price",
                        "vcp", "wedge", "cup_handle", "double_bottom", "inverse_hs",
                        "pre_breakout", "breakout", "roe", "pl", "pvp"]].copy()

display_df["Padrões"] = filtered.apply(get_pattern_badges, axis=1)
display_df = display_df[["rank", "display", "name", "category", "technical_tier", "technical_score", 
                          "fundamental_tag", "fundamental_score", "price", "Padrões", "roe", "pl", "pvp"]]
display_df.columns = ["Rank", "Ticker", "Nome", "Cat.", "Tier", "Score Téc.", "Tag Fund.", "Score Fund.", "Preço", "Padrões", "ROE", "P/L", "P/VP"]

# Color tier and fund tag
def color_tier(val):
    colors = {"S": "background-color: #d4edda; color: #155724; font-weight: bold",
              "A": "background-color: #fff3cd; color: #856404; font-weight: bold",
              "B": "background-color: #d1ecf1; color: #0c5460",
              "C": ""}
    return colors.get(val, "")

styled_df = display_df.style.map(color_tier, subset=["Tier"]).map(color_fund_tag, subset=["Tag Fund."])

st.dataframe(
    styled_df,
    use_container_width=True,
    hide_index=True,
    height=600
)

# Charts
st.markdown("---")
st.subheader("📊 Visualizações")

chart_tab1, chart_tab2, chart_tab3 = st.tabs(["Score por Tier", "Distribuição de Padrões", "Fundamentalistas"])

with chart_tab1:
    tier_counts = filtered["technical_tier"].value_counts().reindex(["S", "A", "B", "C"], fill_value=0).reset_index()
    tier_counts.columns = ["Tier", "Count"]
    fig = px.bar(tier_counts, x="Tier", y="Count", color="Tier",
                 color_discrete_map={"S": "#2ca02c", "A": "#ffbb33", "B": "#33b5e5", "C": "#999999"},
                 title="Distribuição por Tier Técnico")
    st.plotly_chart(fig, use_container_width=True)

with chart_tab2:
    pattern_counts = {
        "VCP": int(filtered["vcp"].sum()),
        "Wedge": int(filtered["wedge"].sum()),
        "Cup & Handle": int(filtered["cup_handle"].sum()),
        "Double Bottom": int(filtered["double_bottom"].sum()),
        "Inverse H&S": int(filtered["inverse_hs"].sum()),
        "Pre-Breakout": int(filtered["pre_breakout"].sum()),
        "Breakout": int(filtered["breakout"].sum()),
    }
    pat_df = pd.DataFrame(list(pattern_counts.items()), columns=["Padrão", "Quantidade"])
    fig2 = px.bar(pat_df, x="Padrão", y="Quantidade", color="Padrão", title="Padrões Detectados")
    st.plotly_chart(fig2, use_container_width=True)

with chart_tab3:
    fund_counts = filtered["fundamental_tag"].value_counts().reindex(["Forte", "OK", "Fraco"], fill_value=0).reset_index()
    fund_counts.columns = ["Tag", "Count"]
    fig3 = px.pie(fund_counts, names="Tag", values="Count", color="Tag",
                  color_discrete_map={"Forte": "#2ca02c", "OK": "#ffbb33", "Fraco": "#d62728"},
                  title="Distribuição Fundamentalista")
    st.plotly_chart(fig3, use_container_width=True)

# Ticker detail
st.markdown("---")
st.subheader("🔍 Detalhe do Ativo")

selected_ticker = st.selectbox("Selecione um ticker para ver detalhes", filtered["display"].unique())

if selected_ticker:
    row = filtered[filtered["display"] == selected_ticker].iloc[0]
    
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Score Técnico", f"{row['technical_score']:.2f}")
    c2.metric("Tier", row["technical_tier"])
    c3.metric("Score Fund.", f"{row['fundamental_score']:.1f}/5")
    c4.metric("Tag Fund.", row["fundamental_tag"])
    c5.metric("Preço", f"R$ {row['price']:.2f}" if row["category"] != "BDR" else f"$ {row['price']:.2f}")
    
    st.markdown(f"**Padrões:** {get_pattern_badges(row)}")
    
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

# Footer
st.markdown("---")
st.caption(f"Screening B3 v2.1 | Atualizado: {summary.get('date', 'N/A')} | Desenvolvido por marcelocv8")
