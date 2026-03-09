"""
MIDA Dashboard - Entry Point Unico.
Avvia con: streamlit run dashboard/app.py
"""

import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

# Aggiungi la root del progetto al path
_project_root = Path(__file__).parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from dashboard.data_loader import get_data_stats, load_data
from dashboard.filters import (
    apply_filters,
    render_debug_sidebar,
    render_sidebar_filters,
)

# Configurazione pagina
st.set_page_config(
    page_title="MIDA - Analisi Missioni Internazionali",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# CSS personalizzato — tema militare italiano
st.markdown("""
<style>
    /* ── Font ── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', 'Segoe UI', sans-serif; }

    /* ── Palette militare ──
       Verde oliva:  #3D4F1E (scuro) / #4A5D23 (medio) / #6B8C2A (chiaro)
       Blu marina:   #1B3A5C (scuro) / #2C5F8A (medio)
       Sabbia:       #F5F3EE (chiaro) / #EAE6DC (medio) / #D4CFC3 (scuro)
       Grigio acciaio: #5A5F63 / #8B9298
       Rosso Esercito: #8B1A1A
    */

    /* ── Header ── */
    @media (max-width: 768px) {
        .main-header { font-size: 1.6rem !important; }
    }
    .main-header {
        font-size: 2.4rem; font-weight: 700; text-align: center;
        background: linear-gradient(135deg, #3D4F1E 0%, #1B3A5C 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem; letter-spacing: -0.5px;
    }
    .sub-header {
        text-align: center; color: #5A5F63; font-size: 0.95rem;
        margin-bottom: 1.5rem;
    }

    /* ── Info box ── */
    .info-box {
        background: linear-gradient(135deg, #EAE6DC 0%, #D4CFC3 100%);
        padding: 1rem 1.2rem; border-radius: 10px;
        border-left: 4px solid #4A5D23; margin: 0.5rem 0 1.5rem 0;
        font-size: 0.9rem; color: #2C2C2C;
    }

    /* ── Sidebar — verde oliva scuro ── */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #3D4F1E 0%, #2A3614 100%) !important;
    }
    [data-testid="stSidebar"] * {
        color: #EAE6DC !important;
    }
    [data-testid="stSidebar"] .stRadio label {
        color: #F5F3EE !important; font-weight: 500;
    }
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stMultiSelect label,
    [data-testid="stSidebar"] .stSlider label {
        color: #F5F3EE !important; font-size: 0.85rem; font-weight: 600;
    }
    /* Dropdown/select inputs — sfondo chiaro per leggibilità */
    [data-testid="stSidebar"] [data-baseweb="select"],
    [data-testid="stSidebar"] [data-baseweb="input"] {
        background-color: #F5F3EE !important;
        border-radius: 8px !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] * {
        color: #2C2C2C !important;
    }
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div {
        background-color: #F5F3EE !important;
        border: 1px solid #D4CFC3 !important;
        border-radius: 8px !important;
    }
    [data-testid="stSidebar"] .stMultiSelect [data-baseweb="select"] > div {
        background-color: #F5F3EE !important;
        border: 1px solid #D4CFC3 !important;
        border-radius: 8px !important;
    }
    /* Placeholder text più scuro */
    [data-testid="stSidebar"] [data-baseweb="select"] [data-baseweb="tag"] {
        background-color: #4A5D23 !important;
        color: #F5F3EE !important;
    }
    [data-testid="stSidebar"] input::placeholder {
        color: #5A5F63 !important; opacity: 1 !important;
    }
    /* Slider track */
    [data-testid="stSidebar"] .stSlider [data-baseweb="slider"] div[role="slider"] {
        background-color: #6B8C2A !important;
    }

    /* ── Tabs — blu marina ── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px; border-bottom: 2px solid #D4CFC3;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0; padding: 8px 20px;
        font-weight: 500; color: #5A5F63;
    }
    .stTabs [aria-selected="true"] {
        background: #1B3A5C !important; color: #F5F3EE !important;
        border-bottom: 2px solid #6B8C2A;
    }

    /* ── Dataframes ── */
    .stDataFrame { border-radius: 8px; overflow: hidden; }

    /* ── Footer ── */
    .mida-footer {
        text-align: center; color: #8B9298; padding: 1.5rem 0 0.5rem 0;
        font-size: 0.82rem; border-top: 1px solid #D4CFC3;
    }
    .mida-footer a { color: #4A5D23; text-decoration: none; font-weight: 500; }

    /* ── Plotly chart containers ── */
    .js-plotly-plot { border-radius: 8px; }

    /* ── Responsività ── */
    @media (max-width: 768px) {
        .main-header { font-size: 1.5rem !important; letter-spacing: 0; }
        .sub-header { font-size: 0.8rem; }
        .kpi-row { flex-direction: column; }
        .kpi-card { min-width: 100% !important; }
        .stTabs [data-baseweb="tab"] { padding: 6px 12px; font-size: 0.8rem; }
        [data-testid="stSidebar"] { min-width: 240px !important; }
    }
    @media (max-width: 480px) {
        .main-header { font-size: 1.2rem !important; }
        .mida-footer { font-size: 0.7rem; }
    }

    /* ── Scrollbar personalizzata ── */
    ::-webkit-scrollbar { width: 8px; }
    ::-webkit-scrollbar-track { background: #EAE6DC; }
    ::-webkit-scrollbar-thumb { background: #8B9298; border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: #5A5F63; }
</style>
""", unsafe_allow_html=True)


# ── Dark mode CSS override ──
_DARK_CSS = """
<style>
    /* ── Dark mode overrides ── */
    .stApp, [data-testid="stAppViewContainer"] {
        background-color: #1A1F25 !important;
    }
    .stApp [data-testid="stHeader"] { background-color: #1A1F25 !important; }
    .main-header {
        background: linear-gradient(135deg, #6B8C2A 0%, #2C5F8A 100%) !important;
        -webkit-background-clip: text !important; -webkit-text-fill-color: transparent !important;
    }
    .sub-header { color: #8B9298 !important; }
    .info-box {
        background: linear-gradient(135deg, #2A2F35 0%, #1F242A 100%) !important;
        border-left-color: #6B8C2A !important; color: #D4CFC3 !important;
    }
    /* Text */
    .stApp, .stApp p, .stApp span, .stApp label, .stApp div,
    .stApp h1, .stApp h2, .stApp h3, .stApp h4 {
        color: #D4CFC3 !important;
    }
    .stApp a { color: #6B8C2A !important; }
    /* Tabs */
    .stTabs [data-baseweb="tab"] { color: #8B9298 !important; }
    .stTabs [aria-selected="true"] {
        background: #2C5F8A !important; color: #F5F3EE !important;
    }
    .stTabs [data-baseweb="tab-list"] { border-bottom-color: #3A3F45 !important; }
    /* Dataframes */
    .stDataFrame, [data-testid="stDataFrame"] {
        background-color: #22272E !important;
    }
    /* KPI cards */
    .kpi-card {
        background: linear-gradient(135deg, #22272E 0%, #2A2F35 100%) !important;
        border-color: #3A3F45 !important;
    }
    .kpi-card .label { color: #8B9298 !important; }
    /* Footer */
    .mida-footer { color: #5A5F63 !important; border-top-color: #3A3F45 !important; }
    .mida-footer a { color: #6B8C2A !important; }
    /* Scrollbar dark */
    ::-webkit-scrollbar-track { background: #1A1F25 !important; }
    ::-webkit-scrollbar-thumb { background: #3A3F45 !important; }
    ::-webkit-scrollbar-thumb:hover { background: #5A5F63 !important; }
    /* Markdown containers */
    .stMarkdown, [data-testid="stMarkdownContainer"] { color: #D4CFC3 !important; }
    /* Download buttons */
    .stDownloadButton button {
        background-color: #2A2F35 !important; color: #D4CFC3 !important;
        border: 1px solid #3A3F45 !important;
    }
</style>
"""


def main():
    # Dark mode toggle in sidebar
    dark_mode = st.sidebar.toggle("🌙 Dark Mode", value=False, key="dark_mode")
    if dark_mode:
        st.markdown(_DARK_CSS, unsafe_allow_html=True)

    # Header
    st.markdown(
        '<h1 class="main-header">🌍 MIDA - Missioni Internazionali e Dati Analitici</h1>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="sub-header">Analisi interattiva delle missioni internazionali italiane · 1948-2026 · 237 missioni · 38 attive</p>',
        unsafe_allow_html=True,
    )

    # Carica dati
    df = load_data()
    if df is None or df.empty:
        st.error("Impossibile caricare i dati delle missioni.")
        return

    # Filtri sidebar
    filters = render_sidebar_filters(df)
    df_filtered = apply_filters(df, filters)

    # Debug sidebar
    render_debug_sidebar(df, df_filtered)

    # Navigazione pagine
    page = st.sidebar.radio(
        "📑 Navigazione",
        ["📊 Panoramica", "🏛️ Missioni e Dati", "⏳ Timeline", "🗺️ Mappe"],
        index=0,
    )

    try:
        if page == "📊 Panoramica":
            from dashboard.views.overview import render as render_overview
            render_overview(df_filtered)
        elif page == "🏛️ Missioni e Dati":
            from dashboard.views.missions import render as render_missions
            render_missions(df_filtered)
        elif page == "⏳ Timeline":
            from dashboard.views.timeline import render as render_timeline
            render_timeline(df_filtered)
        elif page == "🗺️ Mappe":
            from dashboard.views.maps_page import render as render_maps
            render_maps(df_filtered)
    except Exception as e:
        st.error(f"Errore nel rendering della pagina: {e}")
        import traceback
        st.code(traceback.format_exc())

    # Footer
    st.markdown(f"""
    <div class="mida-footer">
        MIDA — Missioni Internazionali e Dati Analitici · Universit&agrave; di Catania<br>
        <small>Dati: <a href="https://www.difesa.it/operazionimilitari/" target="_blank">Ministero della Difesa</a>
        · Aggiornamento: {datetime.now().strftime('%d/%m/%Y %H:%M')}</small>
    </div>
    """, unsafe_allow_html=True)


main()
