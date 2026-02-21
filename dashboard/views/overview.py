"""Pagina Overview della dashboard MIDA."""

import streamlit as st
import pandas as pd
import plotly.express as px

from dashboard.charts import (
    format_currency, _apply_theme, PERIOD_COLORS,
    chart_missions_by_period,
    chart_budget_by_period,
    chart_personnel_by_period,
    chart_missions_by_participation,
    chart_personnel_distribution,
    chart_period_bar,
)
from dashboard.analysis import (
    create_period_analysis,
    create_participation_analysis,
    create_historical_period_analysis,
)


_KPI_CSS = """
<style>
.kpi-row { display:flex; gap:0.6rem; margin-bottom:0.6rem; flex-wrap:wrap; }
.kpi-card {
    flex:1; min-width:140px; padding:0.9rem 1rem; border-radius:12px;
    background: linear-gradient(135deg, #F5F3EE 0%, #EAE6DC 100%);
    border-left:4px solid var(--accent);
    text-align:center;
    box-shadow: 0 1px 4px rgba(61,79,30,0.08);
    transition: transform 0.15s ease, box-shadow 0.15s ease;
}
.kpi-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(61,79,30,0.15);
}
.kpi-card .value {
    font-size:1.5rem; font-weight:700;
    color: var(--accent);
}
.kpi-card .label { font-size:0.78rem; color:#5A5F63; margin-top:2px; letter-spacing:0.3px; text-transform:uppercase; }
</style>
"""


def _kpi(label: str, value: str, accent: str = "#0077B6") -> str:
    return (
        f'<div class="kpi-card" style="--accent:{accent};">'
        f'<div class="value">{value}</div>'
        f'<div class="label">{label}</div></div>'
    )


def render(df: pd.DataFrame) -> None:
    """Renderizza la pagina Overview."""

    st.markdown(_KPI_CSS, unsafe_allow_html=True)

    # === KPI ROW 1 ===
    n_active = int(df["is_active"].sum()) if "is_active" in df.columns else 0
    st.markdown(
        '<div class="kpi-row">'
        + _kpi("Missioni Totali", f"{len(df):,}", "#3D4F1E")
        + _kpi("Personale Totale", f"{df['personale_totale'].sum():,.0f}", "#1B3A5C")
        + _kpi("Costo Totale", format_currency(df["costo_totale"].sum()), "#4A5D23")
        + _kpi("Missioni Attive", str(n_active), "#6B8C2A")
        + "</div>",
        unsafe_allow_html=True,
    )

    # === KPI ROW 2 ===
    costo_medio = df["costo_totale"].mean() if len(df) > 0 else 0
    pers_medio = df["personale_totale"].mean() if len(df) > 0 else 0
    n_paesi = df["paese"].nunique() if "paese" in df.columns else 0
    st.markdown(
        '<div class="kpi-row">'
        + _kpi("Personale Militare", f"{df['personale_militare'].sum():,.0f}", "#1B3A5C")
        + _kpi("Personale Civile", f"{df['personale_civile'].sum():,.0f}", "#2C5F8A")
        + _kpi("Costo Medio/Missione", format_currency(costo_medio), "#5A5F63")
        + _kpi("Paesi Coinvolti", str(n_paesi), "#8B1A1A")
        + "</div>",
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # === ANALISI PER PERIODI TEMPORALI ===
    st.markdown("## Analisi per Periodi Temporali")
    period_stats = create_period_analysis(df)

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(chart_missions_by_period(period_stats), use_container_width=True, key="ov_missions_period")
        st.plotly_chart(chart_budget_by_period(period_stats), use_container_width=True, key="ov_budget_period")
    with col2:
        st.plotly_chart(chart_personnel_by_period(period_stats), use_container_width=True, key="ov_personnel_period")
        display = period_stats.copy()
        display["Costo Totale"] = display["Costo Totale"].apply(format_currency)
        for c in ["Personale Militare", "Personale Civile", "Personale Totale"]:
            display[c] = display[c].apply(lambda x: f"{x:,.0f}")
        st.dataframe(display, use_container_width=True, hide_index=True)

    st.markdown("---")

    # === PERIODI STORICI ===
    st.markdown("## Periodi Storici")
    df_period = create_historical_period_analysis(df)

    st.plotly_chart(chart_period_bar(df_period), use_container_width=True, key="ov_period_bar")

    agg = df_period.groupby("Periodo Storico").agg(
        personale_totale=("personale_totale", "sum"),
        costo_totale=("costo_totale", "sum"),
    ).reset_index()

    col1, col2 = st.columns(2)
    with col1:
        fig_p = px.bar(agg, x="Periodo Storico", y="personale_totale",
                       title="Personale per Periodo Storico", color="Periodo Storico",
                       color_discrete_sequence=PERIOD_COLORS, text="personale_totale")
        fig_p.update_traces(textposition="outside", texttemplate="%{text:,.0f}")
        st.plotly_chart(_apply_theme(fig_p).update_layout(showlegend=False),
                        use_container_width=True, key="ov_period_pers")
    with col2:
        fig_c = px.bar(agg, x="Periodo Storico", y="costo_totale",
                       title="Costo per Periodo Storico", color="Periodo Storico",
                       color_discrete_sequence=PERIOD_COLORS)
        fig_c.update_traces(hovertemplate="<b>%{x}</b><br>Costo: \u20ac%{y:,.0f}<extra></extra>")
        st.plotly_chart(_apply_theme(fig_c).update_layout(showlegend=False),
                        use_container_width=True, key="ov_period_cost")

    st.markdown("---")

    # === ANALISI PER TIPO DI PARTECIPAZIONE ===
    st.markdown("## Analisi per Tipo di Partecipazione")
    part_stats = create_participation_analysis(df)

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(chart_missions_by_participation(part_stats), use_container_width=True, key="ov_part_type")
    with col2:
        st.plotly_chart(chart_personnel_distribution(part_stats), use_container_width=True, key="ov_part_dist")

    part_display = part_stats.copy()
    part_display["Costo Totale"] = part_display["Costo Totale"].apply(format_currency)
    part_display["Personale Totale"] = part_display["Personale Totale"].apply(lambda x: f"{x:,.0f}")
    st.dataframe(part_display, use_container_width=True, hide_index=True)
