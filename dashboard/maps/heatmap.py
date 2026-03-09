import folium
import pandas as pd
import streamlit as st
from folium.plugins import HeatMap
from streamlit_folium import st_folium


def render_heatmap(df: pd.DataFrame):
    """
    Mappa di calore della concentrazione di personale nelle missioni.
    """
    if df.empty or 'lat' not in df.columns:
        st.warning("Nessun dato con coordinate disponibile.")
        return

    total_pers = int(df['personale_totale'].sum())
    mean_pers = int(df['personale_totale'].mean()) if len(df) > 0 else 0
    max_pers = int(df['personale_totale'].max()) if len(df) > 0 else 0
    zones = int((df['personale_totale'] > 0).sum())

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Personale Totale", f"{total_pers:,}".replace(",", "."))
    with col2:
        st.metric("Media / Missione", f"{mean_pers:,}".replace(",", "."))
    with col3:
        st.metric("Max Singola Missione", f"{max_pers:,}".replace(",", "."))
    with col4:
        st.metric("Zone Attive", zones)

    m = folium.Map(location=[25, 15], zoom_start=2, tiles='CartoDB positron',
                   control_scale=True)

    heat_data = [
        [row['lat'], row['lon'], max(float(row['personale_totale'] or 0), 1)]
        for _, row in df.iterrows()
    ]

    HeatMap(
        heat_data,
        radius=28,
        blur=18,
        min_opacity=0.35,
        max_zoom=8,
        gradient={0.2: '#2196F3', 0.4: '#4CAF50', 0.6: '#FFC107', 0.8: '#FF5722', 1.0: '#D50000'},
    ).add_to(m)

    # Gradient legend
    legend_html = (
        '<div style="position:fixed;bottom:30px;left:30px;z-index:9999;'
        'background:white;padding:12px 16px;border-radius:8px;'
        'box-shadow:0 2px 8px rgba(0,0,0,.15);width:220px;">'
        '<div style="font-weight:700;font-size:13px;margin-bottom:8px;">'
        'Concentrazione Personale</div>'
        '<div style="height:12px;border-radius:4px;'
        'background:linear-gradient(to right,#2196F3,#4CAF50,#FFC107,#FF5722,#D50000);"></div>'
        '<div style="display:flex;justify-content:space-between;font-size:10px;margin-top:2px;">'
        '<span>Bassa</span><span>Media</span><span>Alta</span></div>'
        f'<div style="font-size:11px;color:#666;margin-top:6px;">'
        f'Totale: {total_pers:,} unità</div></div>'.replace(",", ".")
    )
    m.get_root().html.add_child(folium.Element(legend_html))

    st_folium(m, use_container_width=True, height=550)
