import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Importa le funzioni delle mappe
try:
    from map_utils import (
        load_geo_data, create_world_map_plotly, create_region_map_plotly,
        create_heatmap_plotly, create_folium_map, create_timeline_map,
        create_mission_clusters_map
    )
    MAPS_AVAILABLE = True
except ImportError:
    # Fallback se il modulo non è disponibile
    def load_geo_data():
        return pd.DataFrame()
    
    def create_world_map_plotly(df, geo_df):
        return go.Figure()
    
    def create_region_map_plotly(df, geo_df):
        return go.Figure()
    
    def create_heatmap_plotly(df, geo_df):
        return go.Figure()
    
    def create_folium_map(df, geo_df):
        return None
    
    def create_timeline_map(df, geo_df):
        return go.Figure()
    
    def create_mission_clusters_map(df, geo_df):
        return None
    
    MAPS_AVAILABLE = False

# Configurazione pagina
st.set_page_config(
    page_title="MIDA - Analisi Missioni Internazionali",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizzato
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        color: #1f77b4;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .period-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c3e50;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #e3f2fd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #2196f3;
        margin: 1rem 0;
    }
    .map-container {
        border: 1px solid #ddd;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    """Carica e prepara i dati delle missioni"""
    try:
        # Prova prima il file completo
        df = pd.read_csv('data/processed/missioni_complete.csv')
    except:
        try:
            # Fallback al file originale
            df = pd.read_csv('data/processed/missioni.csv')
            # Aggiungi colonne mancanti per compatibilità
            df['regione'] = 'Non specificata'
            df['sub_regione'] = 'Non specificata'
            df['tipo_partecipazione'] = 'civmil'
            df['personale_militare'] = df['personale'] * 0.7
            df['personale_civile'] = df['personale'] * 0.3
            df['personale_totale'] = df['personale']
        except:
            st.error("Impossibile caricare i dati delle missioni")
            return None
    
    # Converti le date
    df['data_inizio'] = pd.to_datetime(df['data_inizio'], errors='coerce')
    df['data_fine'] = pd.to_datetime(df['data_fine'], errors='coerce')
    
    # Gestisci date mancanti e aggiorna le date di fine per missioni attive
    current_date = pd.Timestamp.now()
    
    # Se la data di fine è nel passato o è 2024-12-31, considera la missione come attiva se è recente
    for idx, row in df.iterrows():
        if pd.isna(row['data_fine']) or row['data_fine'] <= current_date:
            # Se la missione è iniziata negli ultimi 5 anni, considera attiva
            if (current_date - row['data_inizio']).days < 1825:  # 5 anni
                df.at[idx, 'data_fine'] = current_date + pd.Timedelta(days=365)  # Estendi di 1 anno
    
    return df

def create_period_analysis(df):
    """Crea l'analisi per periodi temporali"""
    
    # Definisci i periodi
    def get_period(row):
        start_year = row['data_inizio'].year
        if start_year < 2001:
            return "1991-2001"
        elif start_year < 2015:
            return "2001-2015"
        else:
            return "2015-ad oggi"
    
    df['periodo'] = df.apply(get_period, axis=1)
    
    # Analisi per periodo
    period_stats = df.groupby('periodo').agg({
        'nome': 'count',
        'personale_militare': 'sum',
        'personale_civile': 'sum',
        'personale_totale': 'sum',
        'costo_totale': 'sum'
    }).reset_index()
    
    period_stats.columns = ['Periodo', 'Numero Missioni', 'Personale Militare', 
                           'Personale Civile', 'Personale Totale', 'Costo Totale']
    
    return period_stats

def create_participation_analysis(df):
    """Analisi per tipo di partecipazione"""
    participation_stats = df.groupby('tipo_partecipazione').agg({
        'nome': 'count',
        'personale_totale': 'sum',
        'costo_totale': 'sum'
    }).reset_index()
    
    participation_stats.columns = ['Tipo Partecipazione', 'Numero Missioni', 
                                  'Personale Totale', 'Costo Totale']
    
    return participation_stats

def create_regional_analysis(df):
    """Analisi per regione e sub-regione"""
    regional_stats = df.groupby(['regione', 'sub_regione']).agg({
        'nome': 'count',
        'personale_totale': 'sum',
        'costo_totale': 'sum'
    }).reset_index()
    
    regional_stats.columns = ['Regione', 'Sub-Regione', 'Numero Missioni', 
                             'Personale Totale', 'Costo Totale']
    
    return regional_stats

def format_currency(value):
    """Formatta i valori monetari"""
    if value >= 1e9:
        return f"€{value/1e9:.1f}B"
    elif value >= 1e6:
        return f"€{value/1e6:.1f}M"
    elif value >= 1e3:
        return f"€{value/1e3:.1f}K"
    else:
        return f"€{value:,.0f}"

def main():
    # Header principale
    st.markdown('<h1 class="main-header">🌍 MIDA - Missioni Internazionali e Dati Analitici</h1>', 
                unsafe_allow_html=True)
    
    # Info box
    st.markdown("""
    <div class="info-box">
        <strong>📊 Dashboard Interattiva</strong><br>
        Analisi completa delle missioni internazionali italiane dal 1991 ad oggi. 
        Utilizza i filtri nella sidebar per personalizzare la visualizzazione.
    </div>
    """, unsafe_allow_html=True)
    
    # Carica i dati
    df = load_data()
    if df is None:
        st.error("Errore nel caricamento dei dati")
        return
    
    # Carica dati geografici
    geo_df = load_geo_data()
    
    # Debug info
    if st.sidebar.checkbox("🔧 Debug Info"):
        st.sidebar.write("**Debug Info:**")
        st.sidebar.write(f"Dati caricati: {len(df)} righe")
        st.sidebar.write(f"Mappe disponibili: {MAPS_AVAILABLE}")
        st.sidebar.write(f"Coordinate caricate: {len(geo_df)} paesi")
        st.sidebar.write(f"Data corrente: {pd.Timestamp.now()}")
        
        # Mostra alcune date di fine per debug
        st.sidebar.write("**Esempi date fine:**")
        for i, row in df.head(5).iterrows():
            st.sidebar.write(f"{row['nome']}: {row['data_fine']}")
    
    # Sidebar per filtri
    st.sidebar.header("🔍 Filtri")
    
    # Filtro per periodo
    periodi = ['Tutti i periodi'] + sorted(df['data_inizio'].dt.year.unique().tolist())
    anno_selezionato = st.sidebar.selectbox("Anno di inizio", periodi)
    
    # Filtro per tipo di partecipazione
    tipi_partecipazione = ['Tutti'] + sorted(df['tipo_partecipazione'].unique().tolist())
    tipo_selezionato = st.sidebar.selectbox("Tipo di partecipazione", tipi_partecipazione)
    
    # Filtro per regione
    regioni = ['Tutte le regioni'] + sorted(df['regione'].unique().tolist())
    regione_selezionata = st.sidebar.selectbox("Regione", regioni)
    
    # Filtro per tipo missione
    tipi_missione = ['Tutti i tipi'] + sorted(df['tipo_missione'].unique().tolist())
    tipo_missione_selezionato = st.sidebar.selectbox("Tipo missione", tipi_missione)
    
    # Filtro per organizzazione
    organizzazioni = ['Tutte le organizzazioni'] + sorted(df['tipo_missione'].unique().tolist())
    organizzazione_selezionata = st.sidebar.selectbox("Organizzazione", organizzazioni)
    
    # Applica filtri
    df_filtered = df.copy()
    if anno_selezionato != 'Tutti i periodi':
        df_filtered = df_filtered[df_filtered['data_inizio'].dt.year == anno_selezionato]
    if tipo_selezionato != 'Tutti':
        df_filtered = df_filtered[df_filtered['tipo_partecipazione'] == tipo_selezionato]
    if regione_selezionata != 'Tutte le regioni':
        df_filtered = df_filtered[df_filtered['regione'] == regione_selezionata]
    if tipo_missione_selezionato != 'Tutti i tipi':
        df_filtered = df_filtered[df_filtered['tipo_missione'] == tipo_missione_selezionato]
    if organizzazione_selezionata != 'Tutte le organizzazioni':
        df_filtered = df_filtered[df_filtered['tipo_missione'] == organizzazione_selezionata]
    
    # Metriche principali
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Missioni Totali", len(df_filtered))
    
    with col2:
        personale_totale = df_filtered['personale_totale'].sum()
        st.metric("👥 Personale Totale", f"{personale_totale:,.0f}")
    
    with col3:
        costo_totale = df_filtered['costo_totale'].sum()
        st.metric("💰 Costo Totale", format_currency(costo_totale))
    
    with col4:
        # Calcolo corretto delle missioni attive
        current_date = pd.Timestamp.now()
        missioni_attive = len(df_filtered[df_filtered['data_fine'] > current_date])
        st.metric("🟢 Missioni Attive", missioni_attive)
    
    # Statistiche aggiuntive
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        personale_militare = df_filtered['personale_militare'].sum()
        st.metric("🎖️ Personale Militare", f"{personale_militare:,.0f}")
    
    with col2:
        personale_civile = df_filtered['personale_civile'].sum()
        st.metric("👔 Personale Civile", f"{personale_civile:,.0f}")
    
    with col3:
        costo_medio = df_filtered['costo_totale'].mean() if len(df_filtered) > 0 else 0
        st.metric("💵 Costo Medio per Missione", format_currency(costo_medio))
    
    with col4:
        personale_medio = df_filtered['personale_totale'].mean() if len(df_filtered) > 0 else 0
        st.metric("👤 Personale Medio per Missione", f"{personale_medio:.0f}")
    
    st.markdown("---")
    
    # 1. ANALISI PER PERIODI TEMPORALI
    st.markdown('<h2 class="period-header">📅 Analisi per Periodi Temporali</h2>', 
                unsafe_allow_html=True)
    
    period_stats = create_period_analysis(df_filtered)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Grafico a barre per numero di missioni per periodo
        fig_missions = px.bar(
            period_stats,
            x='Periodo',
            y='Numero Missioni',
            title='Numero di Missioni per Periodo',
            color='Periodo',
            color_discrete_sequence=['#1f77b4', '#ff7f0e', '#2ca02c']
        )
        fig_missions.update_layout(showlegend=False)
        st.plotly_chart(fig_missions, use_container_width=True, key="missions_period")
        
        # Grafico per budget per periodo
        fig_budget = px.pie(
            period_stats,
            values='Costo Totale',
            names='Periodo',
            title='Distribuzione Budget per Periodo'
        )
        st.plotly_chart(fig_budget, use_container_width=True, key="budget_period")
    
    with col2:
        # Grafico per personale per periodo
        fig_personnel = go.Figure()
        
        fig_personnel.add_trace(go.Bar(
            name='Personale Militare',
            x=period_stats['Periodo'],
            y=period_stats['Personale Militare'],
            marker_color='#1f77b4'
        ))
        
        fig_personnel.add_trace(go.Bar(
            name='Personale Civile',
            x=period_stats['Periodo'],
            y=period_stats['Personale Civile'],
            marker_color='#ff7f0e'
        ))
        
        fig_personnel.update_layout(
            title='Personale per Periodo (Militare vs Civile)',
            barmode='stack',
            xaxis_title='Periodo',
            yaxis_title='Numero di Personale'
        )
        st.plotly_chart(fig_personnel, use_container_width=True, key="personnel_period")
        
        # Tabella riassuntiva
        st.subheader("📋 Riepilogo per Periodo")
        
        # Formatta i dati per la visualizzazione
        period_display = period_stats.copy()
        period_display['Costo Totale'] = period_display['Costo Totale'].apply(format_currency)
        period_display['Personale Militare'] = period_display['Personale Militare'].apply(lambda x: f"{x:,.0f}")
        period_display['Personale Civile'] = period_display['Personale Civile'].apply(lambda x: f"{x:,.0f}")
        period_display['Personale Totale'] = period_display['Personale Totale'].apply(lambda x: f"{x:,.0f}")
        
        st.dataframe(period_display, use_container_width=True)
    
    st.markdown("---")
    
    # 2. ANALISI PER TIPO DI PARTECIPAZIONE
    st.markdown('<h2 class="period-header">🎯 Analisi per Tipo di Partecipazione</h2>', 
                unsafe_allow_html=True)
    
    participation_stats = create_participation_analysis(df_filtered)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Grafico per tipo di partecipazione
        fig_participation = px.bar(
            participation_stats,
            x='Tipo Partecipazione',
            y='Numero Missioni',
            title='Numero di Missioni per Tipo di Partecipazione',
            color='Tipo Partecipazione',
            color_discrete_map={
                'mil': '#1f77b4',
                'civ': '#ff7f0e', 
                'civmil': '#2ca02c'
            }
        )
        st.plotly_chart(fig_participation, use_container_width=True, key="participation_type")
    
    with col2:
        # Grafico a torta per distribuzione personale
        fig_personnel_dist = px.pie(
            participation_stats,
            values='Personale Totale',
            names='Tipo Partecipazione',
            title='Distribuzione Personale per Tipo di Partecipazione'
        )
        st.plotly_chart(fig_personnel_dist, use_container_width=True, key="personnel_distribution")
    
    # Tabella dettagliata
    st.subheader("📊 Dettagli per Tipo di Partecipazione")
    
    # Formatta i dati
    participation_display = participation_stats.copy()
    participation_display['Costo Totale'] = participation_display['Costo Totale'].apply(format_currency)
    participation_display['Personale Totale'] = participation_display['Personale Totale'].apply(lambda x: f"{x:,.0f}")
    
    st.dataframe(participation_display, use_container_width=True)
    
    st.markdown("---")
    
    # 3. ANALISI PER REGIONE E SUB-REGIONE
    st.markdown('<h2 class="period-header">🌍 Analisi per Regione e Sub-Regione</h2>', 
                unsafe_allow_html=True)
    
    regional_stats = create_regional_analysis(df_filtered)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Grafico per numero di missioni per regione
        region_summary = regional_stats.groupby('Regione')['Numero Missioni'].sum().reset_index()
        fig_region_missions = px.bar(
            region_summary,
            x='Regione',
            y='Numero Missioni',
            title='Numero di Missioni per Regione',
            color='Regione'
        )
        st.plotly_chart(fig_region_missions, use_container_width=True, key="region_missions")
        
        # Mappa di calore per sub-regioni
        if len(regional_stats) > 0:
            pivot_table = regional_stats.pivot_table(
                values='Numero Missioni',
                index='Regione',
                columns='Sub-Regione',
                aggfunc='sum',
                fill_value=0
            )
            
            fig_heatmap = px.imshow(
                pivot_table,
                title='Mappa di Calore: Missioni per Regione e Sub-Regione',
                aspect='auto',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig_heatmap, use_container_width=True, key="region_heatmap")
    
    with col2:
        # Grafico per costo per regione
        region_cost = regional_stats.groupby('Regione')['Costo Totale'].sum().reset_index()
        fig_region_cost = px.bar(
            region_cost,
            x='Regione',
            y='Costo Totale',
            title='Costo Totale per Regione',
            color='Regione'
        )
        st.plotly_chart(fig_region_cost, use_container_width=True, key="region_cost")
        
        # Grafico per personale per regione
        region_personnel = regional_stats.groupby('Regione')['Personale Totale'].sum().reset_index()
        fig_region_personnel = px.bar(
            region_personnel,
            x='Regione',
            y='Personale Totale',
            title='Personale Totale per Regione',
            color='Regione'
        )
        st.plotly_chart(fig_region_personnel, use_container_width=True, key="region_personnel")
    
    # Tabella dettagliata per regione
    st.subheader("📋 Dettagli per Regione e Sub-Regione")
    
    # Formatta i dati
    regional_display = regional_stats.copy()
    regional_display['Costo Totale'] = regional_display['Costo Totale'].apply(format_currency)
    regional_display['Personale Totale'] = regional_display['Personale Totale'].apply(lambda x: f"{x:,.0f}")
    
    st.dataframe(regional_display, use_container_width=True)
    
    st.markdown("---")
    
    # 4. ANALISI PER ORGANIZZAZIONE
    st.markdown('<h2 class="period-header">🏛️ Analisi per Organizzazione</h2>', 
                unsafe_allow_html=True)
    
    # Analisi per organizzazione
    org_stats = df_filtered.groupby('tipo_missione').agg({
        'nome': 'count',
        'personale_totale': 'sum',
        'costo_totale': 'sum',
        'personale_militare': 'sum',
        'personale_civile': 'sum'
    }).reset_index()
    
    org_stats.columns = ['Organizzazione', 'Numero Missioni', 'Personale Totale', 
                        'Costo Totale', 'Personale Militare', 'Personale Civile']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Grafico per numero di missioni per organizzazione
        fig_org_missions = px.bar(
            org_stats,
            x='Organizzazione',
            y='Numero Missioni',
            title='Numero di Missioni per Organizzazione',
            color='Organizzazione',
            color_discrete_map={
                'ONU': '#1f77b4',
                'UE': '#ff7f0e',
                'NATO': '#2ca02c',
                'ITA': '#d62728'
            }
        )
        st.plotly_chart(fig_org_missions, use_container_width=True, key="org_missions")
        
        # Grafico a torta per distribuzione personale
        fig_org_personnel = px.pie(
            org_stats,
            values='Personale Totale',
            names='Organizzazione',
            title='Distribuzione Personale per Organizzazione'
        )
        st.plotly_chart(fig_org_personnel, use_container_width=True, key="org_personnel")
    
    with col2:
        # Grafico per personale militare vs civile per organizzazione
        fig_org_mil_civ = go.Figure()
        
        fig_org_mil_civ.add_trace(go.Bar(
            name='Personale Militare',
            x=org_stats['Organizzazione'],
            y=org_stats['Personale Militare'],
            marker_color='#1f77b4'
        ))
        
        fig_org_mil_civ.add_trace(go.Bar(
            name='Personale Civile',
            x=org_stats['Organizzazione'],
            y=org_stats['Personale Civile'],
            marker_color='#ff7f0e'
        ))
        
        fig_org_mil_civ.update_layout(
            title='Personale per Organizzazione (Militare vs Civile)',
            barmode='stack',
            xaxis_title='Organizzazione',
            yaxis_title='Numero di Personale'
        )
        st.plotly_chart(fig_org_mil_civ, use_container_width=True, key="org_mil_civ")
        
        # Grafico per costo per organizzazione
        fig_org_cost = px.bar(
            org_stats,
            x='Organizzazione',
            y='Costo Totale',
            title='Costo Totale per Organizzazione',
            color='Organizzazione',
            color_discrete_map={
                'ONU': '#1f77b4',
                'UE': '#ff7f0e',
                'NATO': '#2ca02c',
                'ITA': '#d62728'
            }
        )
        st.plotly_chart(fig_org_cost, use_container_width=True, key="org_cost")
    
    # Tabella dettagliata per organizzazione
    st.subheader("📊 Dettagli per Organizzazione")
    
    # Formatta i dati
    org_display = org_stats.copy()
    org_display['Costo Totale'] = org_display['Costo Totale'].apply(format_currency)
    org_display['Personale Totale'] = org_display['Personale Totale'].apply(lambda x: f"{x:,.0f}")
    org_display['Personale Militare'] = org_display['Personale Militare'].apply(lambda x: f"{x:,.0f}")
    org_display['Personale Civile'] = org_display['Personale Civile'].apply(lambda x: f"{x:,.0f}")
    
    st.dataframe(org_display, use_container_width=True)
    
    st.markdown("---")
    
    # 🗺️ SEZIONE MAPPE MIGLIORATA
    st.markdown('<h2 class="period-header">🗺️ Mappe Interattive Avanzate</h2>', 
                unsafe_allow_html=True)
    
    # Info box per le mappe
    st.markdown("""
    <div class="info-box">
        <strong>🗺️ Guida alle Mappe</strong><br>
        • <strong>Mappa del Mondo:</strong> Visualizzazione completa con colori per organizzazione<br>
        • <strong>Mappa di Calore:</strong> Densità del personale impiegato nelle missioni<br>
        • <strong>Timeline:</strong> Evoluzione temporale delle missioni (usa lo slider)<br>
        • <strong>Cluster:</strong> Mappa interattiva con raggruppamento automatico
    </div>
    """, unsafe_allow_html=True)
    
    if not MAPS_AVAILABLE:
        st.warning("⚠️ Le funzioni delle mappe non sono disponibili. Installa le dipendenze con: pip install folium geopandas pydeck geopy")
        st.info("📦 Dipendenze mancanti per le mappe:")
        st.code("pip install folium>=0.14.0 geopandas>=0.12.0 pydeck>=0.8.0 geopy>=2.3.0")
    else:
        # Tab per diverse tipologie di mappe
        map_tab1, map_tab2, map_tab3, map_tab4 = st.tabs([
            "🌍 Mappa del Mondo", 
            "🔥 Mappa di Calore", 
            "⏰ Timeline", 
            "📍 Cluster"
        ])
        
        with map_tab1:
            st.subheader("🌍 Mappa del Mondo - Distribuzione Missioni")
            
            # Mappa del mondo con Plotly
            world_map = create_world_map_plotly(df_filtered, geo_df)
            st.plotly_chart(world_map, use_container_width=True, key="world_map_improved")
            
            # Informazioni sulla mappa
            st.info("""
            **🎯 Legenda Mappa:**
            - **🔵 Blu:** Missioni ONU
            - **🟠 Arancione:** Missioni UE  
            - **🟢 Verde:** Missioni NATO
            - **🔴 Rosso:** Missioni Italiane
            - **Dimensioni:** Basate sul numero di personale
            - **Hover:** Mostra dettagli completi della missione
            """)
        
        with map_tab2:
            st.subheader("🔥 Mappa di Calore - Densità Personale")
            
            # Mappa di calore
            heatmap = create_heatmap_plotly(df_filtered, geo_df)
            st.plotly_chart(heatmap, use_container_width=True, key="heatmap_improved")
            
            st.info("""
            **🔥 Mappa di Calore:**
            - Mostra la densità del personale impiegato
            - Zone più scure = più personale
            - Utile per identificare aree di maggiore impegno
            - Scala colori: Blu (basso) → Rosso (alto)
            """)
        
        with map_tab3:
            st.subheader("⏰ Timeline Geografica - Evoluzione Temporale")
            
            # Mappa timeline
            timeline_map = create_timeline_map(df_filtered, geo_df)
            st.plotly_chart(timeline_map, use_container_width=True, key="timeline_improved")
            
            st.info("""
            **⏰ Timeline Interattiva:**
            - Mostra l'evoluzione delle missioni nel tempo
            - Usa i controlli per navigare tra gli anni
            - Visualizza come si sono sviluppate le missioni geograficamente
            - Colori per organizzazione mantenuti nel tempo
            """)
        
        with map_tab4:
            st.subheader("📍 Mappa con Cluster - Raggruppamento Missioni")
            
            # Mappa con cluster (Folium)
            try:
                import folium
                cluster_map = create_mission_clusters_map(df_filtered, geo_df)
                if cluster_map:
                    st.components.v1.html(cluster_map._repr_html_(), height=600)
                else:
                    st.warning("Mappa cluster non disponibile")
            except ImportError:
                st.warning("Folium non installato. Installa con: pip install folium")
            
            st.info("""
            **📍 Mappa Cluster:**
            - Raggruppa missioni vicine per una migliore visualizzazione
            - Zoom per vedere i dettagli
            - Clicca sui marker per informazioni complete
            - Layer control per attivare/disattivare organizzazioni
            """)
    
    st.markdown("---")
    
    # 5. TIMELINE DELLE MISSIONI
    st.markdown('<h2 class="period-header">⏰ Timeline delle Missioni</h2>', 
                unsafe_allow_html=True)
    
    # Crea timeline
    fig_timeline = go.Figure()
    
    for _, row in df_filtered.iterrows():
        fig_timeline.add_trace(go.Scatter(
            x=[row['data_inizio'], row['data_fine']],
            y=[row['nome'], row['nome']],
            mode='lines+markers',
            name=row['nome'],
            line=dict(width=3),
            marker=dict(size=8),
            hovertemplate=f"<b>{row['nome']}</b><br>" +
                         f"Paese: {row['paese']}<br>" +
                         f"Tipo: {row['tipo_partecipazione']}<br>" +
                         f"Personale: {row['personale_totale']}<br>" +
                         f"Costo: {format_currency(row['costo_totale'])}<br>" +
                         "<extra></extra>"
        ))
    
    fig_timeline.update_layout(
        title='Timeline delle Missioni',
        xaxis_title='Data',
        yaxis_title='Missione',
        height=600,
        showlegend=False
    )
    
    st.plotly_chart(fig_timeline, use_container_width=True, key="mission_timeline")
    
    st.markdown("---")
    
    # 6. TABELLA COMPLETA DEI DATI
    st.markdown('<h2 class="period-header">📊 Dati Completi delle Missioni</h2>', 
                unsafe_allow_html=True)
    
    # Formatta le colonne per una migliore visualizzazione
    df_display = df_filtered.copy()
    df_display['costo_totale_formatted'] = df_display['costo_totale'].apply(format_currency)
    df_display['data_inizio_formatted'] = df_display['data_inizio'].dt.strftime('%Y-%m-%d')
    df_display['data_fine_formatted'] = df_display['data_fine'].dt.strftime('%Y-%m-%d')
    df_display['personale_totale_formatted'] = df_display['personale_totale'].apply(lambda x: f"{x:,.0f}")
    
    # Seleziona solo le colonne da mostrare
    display_columns = ['nome', 'paese', 'regione', 'sub_regione', 'tipo_partecipazione', 
                      'data_inizio_formatted', 'data_fine_formatted', 'personale_totale_formatted', 
                      'costo_totale_formatted', 'tipo_missione']
    
    df_display = df_display[display_columns]
    df_display.columns = ['Missione', 'Paese', 'Regione', 'Sub-Regione', 'Tipo Partecipazione',
                         'Data Inizio', 'Data Fine', 'Personale Totale', 'Costo Totale', 'Tipo Missione']
    
    st.dataframe(df_display, use_container_width=True)
    
    # Esportazione dati
    st.markdown("---")
    st.markdown('<h3 class="period-header">📥 Esportazione Dati</h3>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Download CSV
        csv_data = df_filtered.to_csv(index=False, encoding='utf-8')
        st.download_button(
            label="📄 Scarica CSV",
            data=csv_data,
            file_name=f"missioni_internazionali_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    with col2:
        # Download Excel
        try:
            import io
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_filtered.to_excel(writer, sheet_name='Missioni', index=False)
                period_stats.to_excel(writer, sheet_name='Analisi Periodi', index=False)
                participation_stats.to_excel(writer, sheet_name='Analisi Partecipazione', index=False)
                regional_stats.to_excel(writer, sheet_name='Analisi Regioni', index=False)
            
            buffer.seek(0)
            st.download_button(
                label="📊 Scarica Excel",
                data=buffer.getvalue(),
                file_name=f"missioni_internazionali_analisi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except ImportError:
            st.warning("Per l'esportazione Excel, installa: pip install openpyxl")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        <p>🌍 MIDA - Missioni Internazionali e Dati Analitici</p>
        <p>Dashboard creata per l'analisi delle missioni internazionali italiane</p>
        <p><small>Ultimo aggiornamento: {}</small></p>
    </div>
    """.format(datetime.now().strftime('%d/%m/%Y %H:%M')), unsafe_allow_html=True)

if __name__ == "__main__":
    main() 