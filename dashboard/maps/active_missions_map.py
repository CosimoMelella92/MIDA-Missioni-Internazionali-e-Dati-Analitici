"""
Mappa delle missioni attive dell'Italia — auto-aggiornante.
Filtra automaticamente per is_active == True e mostra lo stato corrente.
"""

import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
import math
from datetime import datetime

from dashboard.charts import ORG_COLORS


_FOLIUM_ICON_COLORS = {
    "ONU": "blue", "UE": "orange", "NATO": "green", "ITA": "red",
    "Bilateral": "beige", "Multinational": "purple", "Coalizione": "pink", "Altro": "gray",
}

_STATUS_CSS = """
<style>
.active-header {
    background: linear-gradient(135deg, #264653 0%, #2A9D8F 100%);
    color: white; padding: 1.2rem 1.5rem; border-radius: 12px;
    margin-bottom: 1rem; text-align: center;
}
.active-header h2 { margin: 0; font-size: 1.5rem; }
.active-header .sub { font-size: 0.9rem; opacity: 0.85; margin-top: 4px; }
.active-kpi-row { display: flex; gap: 0.6rem; margin-bottom: 1rem; }
.active-kpi {
    flex: 1; padding: 0.8rem; border-radius: 8px; text-align: center;
    background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
    border-top: 3px solid var(--accent);
}
.active-kpi .val { font-size: 1.4rem; font-weight: 700; color: #2B2D42; }
.active-kpi .lbl { font-size: 0.75rem; color: #6C757D; }
</style>
"""


def _fmt(val) -> str:
    try:
        return f"{int(val):,}".replace(",", ".")
    except (ValueError, TypeError):
        return "0"


def render_active_missions_map(df: pd.DataFrame):
    """
    Mappa delle missioni italiane attive nel presente.
    Si struttura automaticamente in base ai dati correnti.
    """
    year = datetime.now().year

    # Filtra solo missioni attive
    active = df[df["is_active"] == True].copy() if "is_active" in df.columns else df.head(0)

    if active.empty or "lat" not in active.columns:
        st.warning("Nessuna missione attiva con coordinate disponibile.")
        return

    # Header
    st.markdown(_STATUS_CSS, unsafe_allow_html=True)
    st.markdown(
        f'<div class="active-header">'
        f'<h2>Missioni Internazionali Italiane Attive — {year}</h2>'
        f'<div class="sub">Fonte: Ministero della Difesa · Aggiornamento automatico</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # KPI
    n_missions = len(active)
    n_countries = active["paese"].nunique()
    n_orgs = active["tipo_missione"].nunique()
    total_pers = active["personale_totale"].sum()
    n_regions = active["regione"].nunique() if "regione" in active.columns else 0

    st.markdown(
        '<div class="active-kpi-row">'
        + _kpi("Missioni Attive", str(n_missions), "#264653")
        + _kpi("Paesi", str(n_countries), "#0077B6")
        + _kpi("Organizzazioni", str(n_orgs), "#2A9D8F")
        + _kpi("Personale Totale", _fmt(total_pers), "#E63946")
        + _kpi("Regioni", str(n_regions), "#E9C46A")
        + "</div>",
        unsafe_allow_html=True,
    )

    # Breakdown per organizzazione
    org_counts = active["tipo_missione"].value_counts()
    cols = st.columns(len(org_counts))
    for i, (org, count) in enumerate(org_counts.items()):
        with cols[i]:
            color = ORG_COLORS.get(org, "#6C757D")
            pers = active[active["tipo_missione"] == org]["personale_totale"].sum()
            st.markdown(
                f'<div style="text-align:center;padding:0.5rem;border-radius:8px;'
                f'border-left:3px solid {color};background:#f8f9fa;">'
                f'<div style="font-weight:700;color:{color};">{org}</div>'
                f'<div style="font-size:1.2rem;font-weight:600;">{count}</div>'
                f'<div style="font-size:0.7rem;color:#6C757D;">missioni · {_fmt(pers)} pers.</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown("")

    # === MAPPA ===
    m = folium.Map(
        location=[30, 20], zoom_start=3,
        tiles="CartoDB positron", control_scale=True,
    )

    # Feature groups per organizzazione
    groups = {}
    for org in sorted(active["tipo_missione"].unique()):
        fg = folium.FeatureGroup(name=f"{org} ({org_counts.get(org, 0)})", show=True)
        fg.add_to(m)
        groups[org] = fg

    # Linee da Roma a ogni missione
    rome = [41.8719, 12.5674]
    for _, row in active.iterrows():
        org = row["tipo_missione"]
        color = ORG_COLORS.get(org, "#7f7f7f")
        pers = float(row.get("personale_totale", 0) or 0)
        radius = max(6, min(28, 6 + math.sqrt(pers) / 2.5))

        # Linea tratteggiata Roma → missione
        folium.PolyLine(
            locations=[rome, [row["lat"], row["lon"]]],
            color=color, weight=1, opacity=0.3, dash_array="5 5",
        ).add_to(groups.get(org, m))

        # Popup dettagliato
        data_inizio = ""
        if pd.notna(row.get("data_inizio")):
            try:
                data_inizio = pd.Timestamp(row["data_inizio"]).strftime("%d/%m/%Y")
            except Exception:
                data_inizio = str(row["data_inizio"])

        popup_html = (
            f"<div style='min-width:220px;font-family:Inter,sans-serif;'>"
            f"<h4 style='margin:0 0 8px 0;color:#264653;border-bottom:2px solid {color};padding-bottom:4px;'>"
            f"{row['nome']}</h4>"
            f"<table style='font-size:12px;width:100%;'>"
            f"<tr><td><b>Paese</b></td><td>{row.get('paese', 'N/A')}</td></tr>"
            f"<tr><td><b>Regione</b></td><td>{row.get('regione', 'N/A')}</td></tr>"
            f"<tr><td><b>Organizzazione</b></td><td>"
            f"<span style='color:{color};font-weight:600;'>{org}</span></td></tr>"
            f"<tr><td><b>Personale</b></td><td>{_fmt(pers)}</td></tr>"
            f"<tr><td><b>Inizio</b></td><td>{data_inizio or 'N/D'}</td></tr>"
            f"<tr><td><b>Stato</b></td><td>"
            f"<span style='color:#06D6A0;font-weight:700;'>● ATTIVA</span></td></tr>"
            f"</table></div>"
        )

        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=radius,
            color=color, weight=2,
            fill=True, fill_color=color, fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=320),
            tooltip=f"🟢 {row['nome']} ({org}) — {_fmt(pers)} pers.",
        ).add_to(groups.get(org, m))

    # Marker Roma (base)
    folium.Marker(
        location=rome,
        icon=folium.Icon(color="red", icon="home", prefix="fa"),
        popup="<b>Roma — Italia</b><br>Base operativa",
        tooltip="Roma — Italia",
    ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    # Leggenda
    legend_items = ""
    for org in sorted(org_counts.index):
        c = ORG_COLORS.get(org, "#7f7f7f")
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
        f'Missioni Attive {year}</div>{legend_items}</div>'
    )
    m.get_root().html.add_child(folium.Element(legend_html))

    st_folium(m, use_container_width=True, height=600)

    # Tabella missioni attive
    st.markdown(f"### Elenco Missioni Attive ({n_missions})")
    display_cols = ["nome", "paese", "regione", "tipo_missione", "personale_totale"]
    display_cols = [c for c in display_cols if c in active.columns]
    table = active[display_cols].sort_values("tipo_missione").copy()
    table["personale_totale"] = table["personale_totale"].apply(lambda x: _fmt(x))
    table.columns = ["Missione", "Paese", "Regione", "Organizzazione", "Personale"][:len(display_cols)]
    st.dataframe(table, use_container_width=True, hide_index=True)


def _kpi(label: str, value: str, accent: str) -> str:
    return (
        f'<div class="active-kpi" style="--accent:{accent};">'
        f'<div class="val">{value}</div>'
        f'<div class="lbl">{label}</div></div>'
    )
