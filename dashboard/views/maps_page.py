"""
Pagina Mappe della dashboard MIDA.
Mostra mappe interattive delle missioni (lazy loading).
"""

import streamlit as st
import pandas as pd

from dashboard.charts import format_currency, ORG_COLORS


def render(df: pd.DataFrame) -> None:
    """Renderizza la pagina Mappe."""

    st.markdown("## 🗺️ Mappe Interattive Avanzate")

    # Lazy import delle mappe
    try:
        from dashboard.maps import (
            render_world_map, render_heatmap, render_timeline_map,
            render_cluster_map, render_active_missions_map,
            add_coordinates_to_dataframe,
        )
        maps_available = True
    except ImportError:
        maps_available = False

    if not maps_available:
        st.warning("⚠️ Le funzioni delle mappe non sono disponibili.")
        st.code("pip install folium>=0.14.0 geopandas>=0.12.0 pydeck>=0.8.0 geopy>=2.3.0")
        return

    # Prepara coordinate
    if "lat" not in df.columns or "lon" not in df.columns:
        df = add_coordinates_to_dataframe(df)

    # Filtro per organizzazione
    org_list = sorted(df["tipo_missione"].unique())
    org_sel = st.selectbox(
        "Seleziona organizzazione per visualizzare le mappe:",
        ["Tutte le organizzazioni"] + org_list,
        key="maps_org_select",
    )

    if org_sel != "Tutte le organizzazioni":
        df_map = df[df["tipo_missione"] == org_sel]
        st.info(f"Mostrando {len(df_map)} missioni: {org_sel}")
    else:
        df_map = df
        st.info(f"Mostrando tutte le {len(df_map)} missioni")

    # Tab mappe
    tab0, tab1, tab2, tab3, tab4 = st.tabs([
        "�🇹 Missioni Attive", "�🌍 Mappa del Mondo", "🔥 Mappa di Calore",
        "⏰ Timeline", "🔗 Cluster",
    ])

    with tab0:
        render_active_missions_map(df)

    with tab1:
        st.subheader(f"🌍 Mappa del Mondo - {org_sel}")
        render_world_map(df_map)

    with tab2:
        st.subheader(f"🔥 Mappa di Calore - {org_sel}")
        render_heatmap(df_map)

    with tab3:
        st.subheader(f"⏰ Timeline - {org_sel}")
        render_timeline_map(df_map)

    with tab4:
        st.subheader(f"🔗 Cluster - {org_sel}")
        render_cluster_map(df_map)

    st.markdown("---")

    # Statistiche organizzazioni nelle mappe
    st.markdown("### 📊 Statistiche delle Organizzazioni")
    _render_org_map_stats(df_map)


def _render_org_map_stats(df: pd.DataFrame) -> None:
    """Renderizza le statistiche per organizzazione nella sezione mappe."""
    org_stats = df.groupby("tipo_missione").agg(
        nome=("nome", "count"),
        personale_totale=("personale_totale", "sum"),
        costo_totale=("costo_totale", "sum"),
    ).reset_index()
    org_stats.columns = ["Organizzazione", "Numero Missioni", "Personale Totale", "Costo Totale"]

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("#### 🏛️ Organizzazioni")
        for _, row in org_stats.iterrows():
            color = ORG_COLORS.get(row["Organizzazione"], "#666")
            st.markdown(
                f'<span style="color:{color};font-size:16px;">●</span> '
                f'**{row["Organizzazione"]}**: {row["Numero Missioni"]}',
                unsafe_allow_html=True,
            )

    with col2:
        st.markdown("#### 👥 Personale Totale")
        for _, row in org_stats.iterrows():
            st.write(f"**{row['Organizzazione']}**: {row['Personale Totale']:,.0f}")

    with col3:
        st.markdown("#### 💰 Costo Totale")
        for _, row in org_stats.iterrows():
            st.write(f"**{row['Organizzazione']}**: {format_currency(row['Costo Totale'])}")

    # Tabella riassuntiva
    st.markdown("### 📋 Tabella Riassuntiva")
    display = org_stats.copy()
    display["Personale Totale"] = display["Personale Totale"].apply(lambda x: f"{x:,.0f}")
    display["Costo Totale"] = display["Costo Totale"].apply(format_currency)
    st.dataframe(display, use_container_width=True)
