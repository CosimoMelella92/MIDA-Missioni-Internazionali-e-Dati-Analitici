"""
Pagina Timeline della dashboard MIDA.
Mostra timeline interattive per organizzazione, regione e durata.
"""

import pandas as pd
import streamlit as st

from dashboard.analysis import get_timeline_data
from dashboard.charts import (
    chart_gantt,
    chart_interactive_timeline,
    chart_scatter_timeline,
    chart_timeline_by_organization,
    chart_timeline_by_region,
    chart_timeline_with_duration,
    format_currency,
)


def render(df: pd.DataFrame) -> None:
    """Renderizza la pagina Timeline."""

    # === TIMELINE INTERATTIVA AVANZATA ===
    st.markdown("## ⏳ Timeline Interattiva delle Missioni (1948-oggi)")

    # Slider per range temporale
    st.markdown("### 🎛️ Controllo Timeline")
    col1, col2 = st.columns([2, 1])

    # Filtra solo missioni con date valide per calcolare range
    df_with_dates = df.dropna(subset=["data_inizio"])
    if df_with_dates.empty:
        st.warning("Nessuna missione con date valide trovata.")
        return

    min_year = int(df_with_dates["data_inizio"].dt.year.min())
    max_year = int(df_with_dates["data_fine"].dt.year.max()) if df_with_dates["data_fine"].notna().any() else 2025
    max_year = max(max_year, 2025)

    if min_year == max_year:
        min_year = max(1948, min_year - 1)
        max_year = min(2026, max_year + 1)

    with col1:
        selected_years = st.slider(
            "Seleziona Range Temporale",
            min_value=min_year, max_value=max_year,
            value=(min_year, max_year), step=1,
            key="tl_slider_main",
        )
    with col2:
        if st.button("🔄 Reset Timeline"):
            selected_years = (min_year, max_year)

    # Filtra dati per periodo
    df_timeline = get_timeline_data(df, selected_years)

    # Statistiche temporali
    st.markdown("### 📈 Statistiche Temporali")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📅 Periodo", f"{selected_years[0]} - {selected_years[1]}",
                   delta=f"{selected_years[1] - selected_years[0]} anni")
    with col2:
        st.metric("🎯 Missioni nel Periodo", len(df_timeline),
                   delta=f"{len(df_timeline) - len(df)} vs totale")
    with col3:
        avg_p = df_timeline["personale_totale"].mean() if len(df_timeline) > 0 else 0
        st.metric("👥 Personale Medio", f"{avg_p:.0f}")
    with col4:
        total_c = df_timeline["costo_totale"].sum()
        st.metric("💰 Costo Totale", format_currency(total_c))

    # Scatter timeline
    st.markdown("### 📊 Timeline Interattiva")
    if len(df_timeline) > 0:
        st.plotly_chart(chart_scatter_timeline(df_timeline), use_container_width=True, key="tl_scatter")
    else:
        st.warning("Nessuna missione trovata per il periodo selezionato")

    # Gantt chart
    if len(df_timeline) > 0:
        st.markdown("### 📊 Timeline con Durata Missioni")
        st.plotly_chart(chart_gantt(df_timeline), use_container_width=True, key="tl_gantt")

    st.markdown("---")

    # === TAB TIMELINE AVANZATE ===
    st.markdown("## ⏰ Timeline Avanzate")

    tab1, tab2, tab3 = st.tabs([
        "📊 Per Organizzazione",
        "🌍 Per Regione",
        "📈 Per Durata",
    ])

    with tab1:
        st.subheader("📊 Timeline per Organizzazione")
        st.plotly_chart(chart_timeline_by_organization(df), use_container_width=True, key="tl_org")
        st.info("Subplot separati per ogni organizzazione con barre (missioni) e linee (personale).")

    with tab2:
        st.subheader("🌍 Timeline per Regione")
        st.plotly_chart(chart_timeline_by_region(df), use_container_width=True, key="tl_region")
        st.info("Evoluzione temporale delle missioni per area geografica.")

    with tab3:
        st.subheader("📈 Timeline con Durata Missioni")
        st.plotly_chart(chart_timeline_with_duration(df), use_container_width=True, key="tl_duration")
        st.info("Top 15 missioni più longeve per organizzazione.")

    st.markdown("---")

    # === TIMELINE INTERATTIVA CON SLIDER SECONDARIO ===
    st.markdown("## 🎛️ Timeline Interattiva Dettagliata")

    col1, col2 = st.columns([3, 1])
    with col1:
        selected_years_2 = st.slider(
            "Seleziona periodo temporale",
            min_value=min_year, max_value=max_year,
            value=(min_year, max_year), step=1,
            key="tl_slider_detail",
        )
    with col2:
        st.metric("Anni selezionati", f"{selected_years_2[1] - selected_years_2[0] + 1}")

    df_period = df[
        df["data_inizio"].notna()
        & (df["data_inizio"].dt.year >= selected_years_2[0])
        & (df["data_inizio"].dt.year <= selected_years_2[1])
    ]

    if len(df_period) > 0:
        st.plotly_chart(
            chart_interactive_timeline(df_period, selected_years_2),
            use_container_width=True, key="tl_interactive_detail",
        )
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Missioni", len(df_period))
        with col2:
            st.metric("Personale", f"{df_period['personale_totale'].sum():,.0f}")
        with col3:
            st.metric("Costo", format_currency(df_period["costo_totale"].sum()))
        with col4:
            st.metric("Organizzazioni", len(df_period["tipo_missione"].unique()))
    else:
        st.warning("Nessuna missione trovata per il periodo selezionato")
