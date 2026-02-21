import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
import math

from dashboard.charts import ORG_COLORS


def _fmt(val) -> str:
    try:
        return f"{int(val):,}".replace(",", ".")
    except (ValueError, TypeError):
        return "0"


def render_timeline_map(df: pd.DataFrame):
    """
    Mappa con slider temporale: evoluzione delle missioni anno per anno.
    Gestisce correttamente NaT in data_inizio/data_fine.
    """
    if df.empty or 'lat' not in df.columns:
        st.warning("Nessun dato con coordinate disponibile.")
        return

    # Filtra solo missioni con almeno data_inizio valida
    df_valid = df.dropna(subset=['data_inizio']).copy()
    if df_valid.empty:
        st.warning("Nessuna missione con date valide.")
        return

    min_year = int(df_valid['data_inizio'].dt.year.min())
    max_year_start = int(df_valid['data_inizio'].dt.year.max())
    max_year_end = int(df_valid['data_fine'].dt.year.max()) if df_valid['data_fine'].notna().any() else max_year_start
    max_year = max(max_year_start, max_year_end, 2025)

    selected_year = st.slider(
        'Seleziona anno',
        min_value=min_year, max_value=max_year,
        value=max(2000, min_year),
        key="timeline_map_slider",
    )

    # Missioni attive nell'anno: inizio <= anno E (fine >= anno O fine=NaT)
    mask = (
        (df_valid['data_inizio'].dt.year <= selected_year)
        & (
            df_valid['data_fine'].isna()
            | (df_valid['data_fine'].dt.year >= selected_year)
        )
    )
    df_active = df_valid[mask]

    # Metriche
    total_pers = int(df_active['personale_totale'].sum())
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Anno", selected_year)
    with col2:
        st.metric("Missioni Attive", len(df_active))
    with col3:
        st.metric("Personale", _fmt(total_pers))
    with col4:
        st.metric("Organizzazioni", df_active['tipo_missione'].nunique() if len(df_active) > 0 else 0)

    # Mappa
    m = folium.Map(location=[25, 15], zoom_start=2, tiles='CartoDB positron',
                   control_scale=True)

    for _, row in df_active.iterrows():
        org = row['tipo_missione']
        color = ORG_COLORS.get(org, '#7f7f7f')
        pers = float(row.get('personale_totale', 0) or 0)
        radius = max(5, min(22, 5 + math.sqrt(pers) / 3))

        di = str(row['data_inizio'])[:4] if pd.notna(row['data_inizio']) else '?'
        df_val = str(row['data_fine'])[:4] if pd.notna(row['data_fine']) else 'in corso'

        popup_html = (
            f"<div style='min-width:200px;font-family:sans-serif;'>"
            f"<h4 style='margin:0 0 6px 0;color:#1a1a2e;'>{row['nome']}</h4>"
            f"<table style='font-size:12px;'>"
            f"<tr><td><b>Paese</b></td><td>{row.get('paese','N/A')}</td></tr>"
            f"<tr><td><b>Organizzazione</b></td><td>{org}</td></tr>"
            f"<tr><td><b>Periodo</b></td><td>{di} - {df_val}</td></tr>"
            f"<tr><td><b>Personale</b></td><td>{_fmt(pers)}</td></tr>"
            f"</table></div>"
        )

        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=radius,
            color=color,
            weight=1.5,
            fill=True,
            fill_color=color,
            fill_opacity=0.65,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{row['nome']} ({di}-{df_val})",
        ).add_to(m)

    # Leggenda
    org_counts = df_active['tipo_missione'].value_counts() if len(df_active) > 0 else pd.Series(dtype=int)
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
        f'Missioni Attive {selected_year}</div>{legend_items}</div>'
    )
    m.get_root().html.add_child(folium.Element(legend_html))

    st_folium(m, use_container_width=True, height=550)