import folium
import pandas as pd
import streamlit as st
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium

from dashboard.charts import ORG_COLORS

# Colori folium icon per organizzazione
_ICON_COLORS = {
    'ONU': 'blue', 'UE': 'orange', 'NATO': 'green', 'ITA': 'red',
    'Bilateral': 'beige', 'Multinational': 'purple', 'Coalizione': 'pink', 'Altro': 'gray',
}


def _fmt(val) -> str:
    try:
        return f"{int(val):,}".replace(",", ".")
    except (ValueError, TypeError):
        return "0"


def render_cluster_map(df: pd.DataFrame):
    """
    Mappa con cluster per organizzazione. Zoom per espandere i gruppi.
    Layer control per attivare/disattivare organizzazioni.
    """
    if df.empty or 'lat' not in df.columns:
        st.warning("Nessun dato con coordinate disponibile.")
        return

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Missioni", len(df))
    with col2:
        st.metric("Organizzazioni", df['tipo_missione'].nunique())
    with col3:
        st.metric("Paesi", df['paese'].nunique())
    with col4:
        st.metric("Regioni", df['regione'].nunique() if 'regione' in df.columns else "-")

    m = folium.Map(location=[25, 15], zoom_start=2, tiles='CartoDB positron',
                   control_scale=True)

    # Cluster separati per organizzazione con colori custom
    clusters = {}
    for org in sorted(df['tipo_missione'].unique()):
        color = ORG_COLORS.get(org, '#7f7f7f')
        clusters[org] = MarkerCluster(
            name=org,
            overlay=True,
            control=True,
            icon_create_function=f"""
            function(cluster) {{
                var count = cluster.getChildCount();
                var size = count < 10 ? 30 : count < 50 ? 38 : 46;
                return L.divIcon({{
                    html: '<div style="background:{color};color:#fff;border-radius:50%;'
                          + 'width:' + size + 'px;height:' + size + 'px;'
                          + 'display:flex;align-items:center;justify-content:center;'
                          + 'font-weight:700;font-size:12px;'
                          + 'box-shadow:0 2px 6px rgba(0,0,0,.3);">'
                          + count + '</div>',
                    className: 'marker-cluster-custom',
                    iconSize: L.point(size, size)
                }});
            }}
            """,
        ).add_to(m)

    for _, row in df.iterrows():
        org = row['tipo_missione']
        icon_color = _ICON_COLORS.get(org, 'gray')
        pers = float(row.get('personale_totale', 0) or 0)

        popup_html = (
            f"<div style='min-width:200px;font-family:sans-serif;'>"
            f"<h4 style='margin:0 0 6px 0;color:#1a1a2e;'>{row['nome']}</h4>"
            f"<table style='font-size:12px;'>"
            f"<tr><td><b>Paese</b></td><td>{row.get('paese','N/A')}</td></tr>"
            f"<tr><td><b>Regione</b></td><td>{row.get('regione','N/A')}</td></tr>"
            f"<tr><td><b>Organizzazione</b></td><td>{org}</td></tr>"
            f"<tr><td><b>Personale</b></td><td>{_fmt(pers)}</td></tr>"
            f"</table></div>"
        )

        cluster = clusters.get(org, list(clusters.values())[0])
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{row['nome']} ({org})",
            icon=folium.Icon(color=icon_color, icon='flag', prefix='fa'),
        ).add_to(cluster)

    folium.LayerControl(collapsed=False).add_to(m)

    # Leggenda
    org_counts = df['tipo_missione'].value_counts()
    legend_items = ""
    for org in sorted(org_counts.index):
        c = ORG_COLORS.get(org, '#7f7f7f')
        cnt = org_counts[org]
        legend_items += (
            f'<div style="display:flex;align-items:center;margin:3px 0;">'
            f'<span style="background:{c};width:12px;height:12px;border-radius:50%;'
            f'display:inline-block;margin-right:6px;"></span>'
            f'<span style="flex:1;font-size:12px;">{org}</span>'
            f'<span style="font-weight:600;font-size:12px;">{cnt}</span></div>'
        )
    legend_html = (
        f'<div style="position:fixed;bottom:30px;left:30px;z-index:9999;'
        f'background:white;padding:12px 16px;border-radius:8px;'
        f'box-shadow:0 2px 8px rgba(0,0,0,.15);max-width:220px;">'
        f'<div style="font-weight:700;font-size:13px;margin-bottom:6px;">'
        f'Cluster per Organizzazione</div>{legend_items}</div>'
    )
    m.get_root().html.add_child(folium.Element(legend_html))

    st_folium(m, use_container_width=True, height=550)
