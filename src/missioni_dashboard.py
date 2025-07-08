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
    .timeline-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        margin: 1rem 0;
        color: white;
    }
    .timeline-header {
        font-size: 1.8rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 1rem;
        color: white;
    }
    .timeline-stats {
        background: rgba(255, 255, 255, 0.1);
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    """Carica e prepara i dati delle missioni"""
    try:
        # Prova prima il file completo
        df = pd.read_csv('data/processed/missioni_complete.csv')
        
        # Integra nuovi dati Excel se presenti
        df = integrate_excel_data(df)
        
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
            
            # Integra nuovi dati Excel se presenti
            df = integrate_excel_data(df)
            
        except:
            st.error("Impossibile caricare i dati delle missioni")
            return None
    
    # Converti le date
    df['data_inizio'] = pd.to_datetime(df['data_inizio'], errors='coerce')
    df['data_fine'] = pd.to_datetime(df['data_fine'], errors='coerce')
    
    # Assicurati che la colonna commitment esista
    if 'commitment' not in df.columns:
        # Crea una classificazione basata sui dati esistenti
        def classify_commitment(row):
            if row['tipo_partecipazione'] == 'civ':
                return 'Head of Mission'
            elif row['personale_totale'] > 500:
                return 'Troops'
            elif row['tipo_missione'] in ['EUTM', 'EUCAP']:
                return 'Head of Mission'
            else:
                return 'Troops'
        
        df['commitment'] = df.apply(classify_commitment, axis=1)
    
    # Pulisci i valori della colonna commitment (rimuovi spazi extra)
    if 'commitment' in df.columns:
        df['commitment'] = df['commitment'].str.strip()
    
    # Gestisci date mancanti e aggiorna le date di fine per missioni attive
    current_date = pd.Timestamp.now()
    
    # Se la data di fine è nel passato o è 2024-12-31, considera la missione come attiva se è recente
    for idx, row in df.iterrows():
        if pd.isna(row['data_fine']) or row['data_fine'] <= current_date:
            # Se la missione è iniziata negli ultimi 5 anni, considera attiva
            if (current_date - row['data_inizio']).days < 1825:  # 5 anni
                df.at[idx, 'data_fine'] = current_date + pd.Timedelta(days=365)  # Estendi di 1 anno
    
    # Rimuovi colonne duplicate se presenti
    df = df.loc[:, ~df.columns.duplicated()]
    
    return df

def integrate_excel_data(df_existing):
    """Integra i dati dai nuovi file Excel evitando duplicati"""
    try:
        # Carica i nuovi dati Excel
        missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
        expenditure_df = pd.read_excel('data/raw/Excel/missions_expenditure_Italy.xlsx')
        
        # Converti i dati Excel nel formato compatibile
        new_missions = []
        
        for _, row in missions_df.iterrows():
            # Normalizza il nome della missione per il confronto
            mission_name = str(row['mission']).strip()
            
            # Controlla se la missione esiste già nei dati attuali
            existing_mission = df_existing[df_existing['nome'].str.contains(mission_name, case=False, na=False)]
            
            if len(existing_mission) == 0:
                # Missione non esistente, aggiungi
                new_mission = {
                    'nome': mission_name,
                    'paese': str(row['country']).strip(),
                    'regione': str(row['region']).strip() if pd.notna(row['region']) else 'Non specificata',
                    'sub_regione': 'Non specificata',
                    'tipo_partecipazione': 'civmil',  # Default
                    'data_inizio': pd.to_datetime(row['date_start'], errors='coerce'),
                    'data_fine': pd.to_datetime(row['date_end'], errors='coerce'),
                    'personale_militare': 100,  # Valore di default
                    'personale_civile': 50,     # Valore di default
                    'personale_totale': 150,    # Valore di default
                    'costo_totale': 25000000,   # Valore di default
                    'tipo_missione': str(row['framework']).strip() if pd.notna(row['framework']) else 'ONU',
                    'commitment': 'Troops'  # Default
                }
                new_missions.append(new_mission)
        
        # Aggiungi i nuovi dati se ce ne sono
        if new_missions:
            new_df = pd.DataFrame(new_missions)
            df_existing = pd.concat([df_existing, new_df], ignore_index=True)
            st.success(f"Integrati {len(new_missions)} nuovi record dalle fonti Excel")
        
        # Rimozione duplicati: normalizza nome (minuscolo, senza spazi e trattini), paese, data_inizio
        def normalize_name(name):
            return str(name).lower().replace(' ', '').replace('-', '').replace('_', '')
        
        # Crea colonne normalizzate per il confronto
        df_existing['__norm_nome'] = df_existing['nome'].apply(normalize_name)
        df_existing['__norm_paese'] = df_existing['paese'].str.lower().str.strip()
        df_existing['__norm_data'] = df_existing['data_inizio'].astype(str).str[:10]
        
        # Trova e rimuovi duplicati basati su nome normalizzato e paese
        duplicates_mask = df_existing.duplicated(subset=['__norm_nome', '__norm_paese'], keep='first')
        if duplicates_mask.any():
            st.info(f"Rimossi {duplicates_mask.sum()} duplicati basati su nome e paese")
            df_existing = df_existing[~duplicates_mask]
        
        # Rimuovi le colonne temporanee
        df_existing = df_existing.drop(columns=['__norm_nome', '__norm_paese', '__norm_data'])
        
        # Rimuovi colonne duplicate se presenti
        df_existing = df_existing.loc[:, ~df_existing.columns.duplicated()]
        
        return df_existing
        
    except Exception as e:
        st.warning(f"Errore nell'integrazione dei dati Excel: {str(e)}")
        return df_existing

def normalize_excel_columns(df):
    """Normalizza le colonne del DataFrame Excel per compatibilità"""
    # Mappa delle colonne possibili
    column_mapping = {
        # Nome missione
        'nome_missione': 'nome',
        'missione': 'nome',
        'Nome Missione': 'nome',
        'Missione': 'nome',
        
        # Paese
        'paese': 'paese',
        'Paese': 'paese',
        'stato': 'paese',
        'Stato': 'paese',
        
        # Regione
        'regione': 'regione',
        'Regione': 'regione',
        'area_geografica': 'regione',
        'Area Geografica': 'regione',
        
        # Sub-regione
        'sub_regione': 'sub_regione',
        'Sub Regione': 'sub_regione',
        'area_specifica': 'sub_regione',
        'Area Specifica': 'sub_regione',
        
        # Tipo partecipazione
        'tipo_partecipazione': 'tipo_partecipazione',
        'Tipo Partecipazione': 'tipo_partecipazione',
        'partecipazione': 'tipo_partecipazione',
        'Partecipazione': 'tipo_partecipazione',
        
        # Date
        'data_inizio': 'data_inizio',
        'Data Inizio': 'data_inizio',
        'inizio': 'data_inizio',
        'Inizio': 'data_inizio',
        
        'data_fine': 'data_fine',
        'Data Fine': 'data_fine',
        'fine': 'data_fine',
        'Fine': 'data_fine',
        
        # Personale
        'personale_militare': 'personale_militare',
        'Personale Militare': 'personale_militare',
        'militari': 'personale_militare',
        'Militari': 'personale_militare',
        
        'personale_civile': 'personale_civile',
        'Personale Civile': 'personale_civile',
        'civili': 'personale_civile',
        'Civili': 'personale_civile',
        
        'personale_totale': 'personale_totale',
        'Personale Totale': 'personale_totale',
        'totale_personale': 'personale_totale',
        'Totale Personale': 'personale_totale',
        'personale': 'personale_totale',
        'Personale': 'personale_totale',
        
        # Costo
        'costo_totale': 'costo_totale',
        'Costo Totale': 'costo_totale',
        'costo': 'costo_totale',
        'Costo': 'costo_totale',
        
        # Tipo missione
        'tipo_missione': 'tipo_missione',
        'Tipo Missione': 'tipo_missione',
        'organizzazione': 'tipo_missione',
        'Organizzazione': 'tipo_missione',
        
        # Commitment
        'commitment': 'commitment',
        'Commitment': 'commitment',
        'tipo_commitment': 'commitment',
        'Tipo Commitment': 'commitment'
    }
    
    # Rinomina le colonne se necessario
    df = df.rename(columns=column_mapping)
    
    # Assicurati che le colonne essenziali esistano
    required_columns = ['nome', 'paese', 'regione', 'sub_regione', 'tipo_partecipazione', 
                       'data_inizio', 'data_fine', 'personale_militare', 'personale_civile', 
                       'personale_totale', 'costo_totale', 'tipo_missione', 'commitment']
    
    for col in required_columns:
        if col not in df.columns:
            if col == 'regione':
                df[col] = 'Non specificata'
            elif col == 'sub_regione':
                df[col] = 'Non specificata'
            elif col == 'tipo_partecipazione':
                df[col] = 'civmil'
            elif col == 'personale_militare':
                df[col] = df.get('personale_totale', 0) * 0.7
            elif col == 'personale_civile':
                df[col] = df.get('personale_totale', 0) * 0.3
            elif col == 'personale_totale':
                df[col] = df.get('personale_militare', 0) + df.get('personale_civile', 0)
            elif col == 'costo_totale':
                df[col] = 0
            elif col == 'tipo_missione':
                df[col] = 'Non specificato'
            elif col == 'commitment':
                df[col] = 'Troops'
    
    return df

def merge_mission_data(df_existing, df_new):
    """Unisce i dati esistenti con i nuovi dati evitando duplicati"""
    # Combina i DataFrame
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    
    # Rimuovi duplicati basati sul nome della missione
    df_combined = df_combined.drop_duplicates(subset=['nome'], keep='first')
    
    return df_combined

def create_period_analysis(df):
    """Crea l'analisi per periodi temporali"""
    
    # Crea una copia del DataFrame per evitare modifiche al DataFrame originale
    df_analysis = df.copy()
    
    # Rimuovi colonne duplicate se presenti
    df_analysis = df_analysis.loc[:, ~df_analysis.columns.duplicated()]
    
    # Verifica che la colonna data_inizio esista e sia nel formato corretto
    if 'data_inizio' not in df_analysis.columns:
        st.error("Colonna 'data_inizio' non trovata nel DataFrame")
        return pd.DataFrame()
    
    # Assicurati che data_inizio sia datetime
    df_analysis['data_inizio'] = pd.to_datetime(df_analysis['data_inizio'], errors='coerce')
    
    # Rimuovi righe con date non valide
    df_analysis = df_analysis.dropna(subset=['data_inizio'])
    
    # Definisci i periodi
    def get_period(row):
        try:
            start_year = row['data_inizio'].year
            if start_year < 1991:
                return "1948-1990"
            elif start_year < 2001:
                return "1991-2001"
            elif start_year < 2015:
                return "2001-2015"
            else:
                return "2015-ad oggi"
        except:
            return "2015-ad oggi"
    
    # Applica la funzione periodo in modo sicuro
    df_analysis['periodo'] = df_analysis.apply(get_period, axis=1)
    
    # Analisi per periodo
    period_stats = df_analysis.groupby('periodo').agg({
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

def create_commitment_analysis(df):
    """Analisi per tipo di commitment"""
    if 'commitment' not in df.columns:
        # Se la colonna commitment non esiste, crea una classificazione basata sui dati esistenti
        def classify_commitment(row):
            if row['tipo_partecipazione'] == 'civ':
                return 'Head of Mission'
            elif row['personale_totale'] > 500:
                return 'Troops'
            elif row['tipo_missione'] in ['EUTM', 'EUCAP']:
                return 'Head of Mission'
            else:
                return 'Troops'
        
        df['commitment'] = df.apply(classify_commitment, axis=1)
    
    commitment_stats = df.groupby('commitment').agg({
        'nome': 'count',
        'personale_totale': 'sum',
        'costo_totale': 'sum'
    }).reset_index()
    
    commitment_stats.columns = ['Tipo Commitment', 'Numero Missioni', 
                               'Personale Totale', 'Costo Totale']
    
    return commitment_stats

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

def map_commitment(row):
    c = str(row['commitment']).lower()
    n = str(row['nome']).lower()
    # UNIFIL override
    if 'unifil' in n:
        return 'Head of Mission'
    # Head of Mission
    if 'head of mission' in c:
        return 'Head of Mission'
    # Troops (naval)
    if 'naval' in c or 'eunavfor' in n or 'irini' in n:
        return 'Troops (naval)'
    # Troops (air)
    if 'air' in c:
        return 'Troops (air)'
    # Troops (ground forces)
    if 'ground' in c or 'isaf' in n or 'kfor' in n or 'inherent resolve' in n:
        return 'Troops (ground forces)'
    # Troops (logistical support)
    if 'logistical' in c or 'support' in c or 'eumm' in n or 'mfo' in n or 'unmogip' in n or 'unficyp' in n or 'unama' in n or 'euam' in n or 'eulex' in n or 'unami' in n:
        return 'Troops (logistical support)'
    # Default fallback
    return 'Troops (logistical support)'

def create_commitment_detailed(df):
    df = df.copy()
    df['Commitment Dettagliato'] = df.apply(map_commitment, axis=1)
    return df[['nome', 'paese', 'tipo_missione', 'personale_totale', 'Commitment Dettagliato']]

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
    
    # Forza conversione date e debug
    df['data_inizio'] = pd.to_datetime(df['data_inizio'], errors='coerce')
    df['data_fine'] = pd.to_datetime(df['data_fine'], errors='coerce')
    st.sidebar.write(f"Missioni con data_inizio non valida: {df['data_inizio'].isna().sum()}")
    st.sidebar.write(f"Missioni con data_fine non valida: {df['data_fine'].isna().sum()}")
    
    # Carica dati geografici
    geo_df = load_geo_data()
    
    # Debug info sempre visibile in sidebar
    st.sidebar.markdown('---')
    st.sidebar.header('🛠️ Debug Dati Missioni')
    st.sidebar.write(f"Missioni caricate dal CSV: {len(df)}")
    st.sidebar.write('Nomi missioni caricate:')
    for nome in df['nome'].unique():
        st.sidebar.write(f'- {nome}')
    # Missioni con date non valide
    invalid_dates = df[df['data_inizio'].isna() | df['data_fine'].isna()]
    if not invalid_dates.empty:
        st.sidebar.write('⚠️ Missioni con date non valide:')
        for _, row in invalid_dates.iterrows():
            st.sidebar.write(f"- {row['nome']} (inizio: {row['data_inizio']}, fine: {row['data_fine']})")
    # Missioni con campi chiave mancanti
    missing_fields = df[df['nome'].isna() | df['paese'].isna() | df['tipo_missione'].isna()]
    if not missing_fields.empty:
        st.sidebar.write('⚠️ Missioni con campi chiave mancanti:')
        for _, row in missing_fields.iterrows():
            st.sidebar.write(f"- {row['nome']} (paese: {row['paese']}, tipo_missione: {row['tipo_missione']})")
    
    # Sidebar per filtri
    st.sidebar.header("🔍 Filtri")
    
    # Filtro per periodo - include missioni attive che iniziano prima del 1991
    anni_disponibili = sorted(df['data_inizio'].dt.year.unique())
    periodi = ['Tutti i periodi'] + anni_disponibili
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
    
    # Filtro per commitment
    if 'commitment' in df.columns:
        commitments = ['Tutti i commitment'] + sorted(df['commitment'].unique().tolist())
        commitment_selezionato = st.sidebar.selectbox("Tipo di Commitment", commitments)
    else:
        commitment_selezionato = 'Tutti i commitment'
    
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
    if commitment_selezionato != 'Tutti i commitment' and 'commitment' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['commitment'] == commitment_selezionato]
    
    # Debug: mostra quante missioni sono nel DataFrame filtrato
    st.sidebar.write(f"Missioni dopo i filtri: {len(df_filtered)}")
    
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
    
    # === SEZIONE TIMELINE INTERATTIVA AVANZATA ===
    st.markdown('<h2 class="period-header">⏳ Timeline Interattiva delle Missioni (1948-oggi)</h2>', unsafe_allow_html=True)
    
    # Timeline interattiva con slider
    st.markdown("### 🎛️ Controllo Timeline")
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Slider per selezionare il range temporale
        min_year = int(df_filtered['data_inizio'].dt.year.min())
        max_year = int(df_filtered['data_inizio'].dt.year.max())
        selected_years = st.slider(
            "Seleziona Range Temporale",
            min_value=min_year,
            max_value=max_year,
            value=(min_year, max_year),
            step=1
        )
    
    with col2:
        # Pulsante per reset
        if st.button("🔄 Reset Timeline"):
            selected_years = (min_year, max_year)
    
    # Filtra i dati per il periodo selezionato
    df_timeline = df_filtered[
        (df_filtered['data_inizio'].dt.year >= selected_years[0]) &
        (df_filtered['data_inizio'].dt.year <= selected_years[1])
    ].copy()
    
    # Statistiche temporali
    st.markdown("### 📈 Statistiche Temporali")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📅 Periodo Analizzato",
            value=f"{selected_years[0]} - {selected_years[1]}",
            delta=f"{selected_years[1] - selected_years[0]} anni"
        )
    
    with col2:
        st.metric(
            label="🎯 Missioni nel Periodo",
            value=len(df_timeline),
            delta=f"{len(df_timeline) - len(df_filtered)} vs totale"
        )
    
    with col3:
        avg_personnel = df_timeline['personale_totale'].mean()
        st.metric(
            label="👥 Personale Medio",
            value=f"{avg_personnel:.0f}",
            delta=f"±{df_timeline['personale_totale'].std():.0f}"
        )
    
    with col4:
        total_cost = df_timeline['costo_totale'].sum()
        st.metric(
            label="💰 Costo Totale",
            value=format_currency(total_cost),
            delta=f"{len(df_timeline)} missioni"
        )
    
    # Timeline interattiva con Plotly
    st.markdown("### 📊 Timeline Interattiva")
    
    # Prepara i dati per la timeline
    timeline_data = df_timeline.copy()
    timeline_data['anno'] = timeline_data['data_inizio'].dt.year
    timeline_data['durata_mesi'] = ((timeline_data['data_fine'] - timeline_data['data_inizio']).dt.days / 30).fillna(12)
    
    # Colori per tipo di missione
    mission_colors = {
        'ONU': '#1f77b4',
        'NATO': '#ff7f0e',
        'EU': '#2ca02c',
        'OSCE': '#d62728',
        'Altri': '#9467bd'
    }
    
    # Crea la timeline interattiva
    fig_timeline = go.Figure()
    
    for mission_type in timeline_data['tipo_missione'].unique():
        missions = timeline_data[timeline_data['tipo_missione'] == mission_type]
        color = mission_colors.get(mission_type, '#9467bd')
        
        fig_timeline.add_trace(go.Scatter(
            x=missions['data_inizio'],
            y=missions['personale_totale'],
            mode='markers+lines',
            name=mission_type,
            text=missions['nome'],
            hovertemplate='<b>%{text}</b><br>' +
                         'Data: %{x}<br>' +
                         'Personale: %{y}<br>' +
                         'Tipo: ' + mission_type + '<br>' +
                         '<extra></extra>',
            marker=dict(
                size=missions['personale_totale'] / 50 + 5,
                color=color,
                opacity=0.7
            ),
            line=dict(width=2, color=color)
        ))
    
    fig_timeline.update_layout(
        title='Timeline Interattiva delle Missioni',
        xaxis_title='Anno',
        yaxis_title='Personale Totale',
        hovermode='closest',
        showlegend=True,
        height=500
    )
    
    st.plotly_chart(fig_timeline, use_container_width=True, key='timeline_interactive')
    
    # Timeline animata per anni
    st.markdown("### 🎬 Timeline Animata per Anni")
    
    # Crea timeline animata
    fig_animated = px.scatter(
        timeline_data,
        x='data_inizio',
        y='personale_totale',
        size='personale_totale',
        color='tipo_missione',
        hover_name='nome',
        animation_frame='anno',
        range_x=[timeline_data['data_inizio'].min(), timeline_data['data_inizio'].max()],
        range_y=[0, timeline_data['personale_totale'].max() * 1.1],
        title='Evoluzione delle Missioni nel Tempo',
        labels={'data_inizio': 'Data Inizio', 'personale_totale': 'Personale Totale'},
        color_discrete_map=mission_colors
    )
    
    fig_animated.update_layout(
        height=600,
        showlegend=True
    )
    
    st.plotly_chart(fig_animated, use_container_width=True, key='timeline_animated')
    
    # Timeline con barre temporali
    st.markdown("### 📊 Timeline con Durata Missioni")
    
    # Crea timeline con barre
    fig_gantt = go.Figure()
    
    # Ordina le missioni per data di inizio
    timeline_data_sorted = timeline_data.sort_values('data_inizio')
    
    for i, (_, mission) in enumerate(timeline_data_sorted.iterrows()):
        start_date = mission['data_inizio']
        end_date = mission['data_fine'] if pd.notna(mission['data_fine']) else start_date + pd.Timedelta(days=365)
        
        # Colore basato sul tipo di missione
        color = mission_colors.get(mission['tipo_missione'], '#9467bd')
        
        fig_gantt.add_trace(go.Scatter(
            x=[start_date, end_date],
            y=[mission['nome'], mission['nome']],
            mode='lines+markers',
            name=mission['tipo_missione'],
            line=dict(color=color, width=8),
            marker=dict(size=10, color=color),
            hovertemplate='<b>%{y}</b><br>' +
                         'Inizio: %{x}<br>' +
                         'Tipo: ' + mission['tipo_missione'] + '<br>' +
                         'Personale: ' + str(mission['personale_totale']) + '<br>' +
                         '<extra></extra>',
            showlegend=False
        ))
    
    fig_gantt.update_layout(
        title='Timeline delle Missioni con Durata',
        xaxis_title='Anno',
        yaxis_title='Missione',
        height=400 + len(timeline_data_sorted) * 20,  # Altezza dinamica
        showlegend=False,
        hovermode='closest'
    )
    
    st.plotly_chart(fig_gantt, use_container_width=True, key='timeline_gantt')
    
    # Timeline per periodi storici
    st.markdown("### 📅 Timeline per Periodi Storici")
    
    # Funzione per assegnare il periodo
    periodi_definiti = [
        (1948, 1990, '1948-1990'),
        (1991, 2001, '1991-2001'),
        (2002, 2015, '2002-2015'),
        (2016, 2100, '2016-oggi')
    ]
    def assegna_periodo(row):
        anno = row['data_inizio'].year if not pd.isna(row['data_inizio']) else None
        for start, end, label in periodi_definiti:
            if anno is not None and start <= anno <= end:
                return label
        return 'Pre-1948'

    # Crea una copia sicura del DataFrame
    df_period = df_filtered.copy()
    
    # Rimuovi colonne duplicate se presenti
    df_period = df_period.loc[:, ~df_period.columns.duplicated()]
    
    # Assicurati che data_inizio sia datetime
    df_period['data_inizio'] = pd.to_datetime(df_period['data_inizio'], errors='coerce')
    
    # Rimuovi righe con date non valide
    df_period = df_period.dropna(subset=['data_inizio'])
    
    # Applica la funzione periodo in modo sicuro
    df_period['Periodo Storico'] = df_period.apply(assegna_periodo, axis=1)

    # Numero missioni per periodo
    period_count = df_period['Periodo Storico'].value_counts().sort_index()
    fig_period_missions = px.bar(
        x=period_count.index,
        y=period_count.values,
        title='Numero di Missioni per Periodo Storico',
        labels={'x': 'Periodo', 'y': 'Numero Missioni'},
        color=period_count.index,
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.plotly_chart(fig_period_missions, use_container_width=True, key='period_missions')

    # Personale e costi per periodo
    agg = df_period.groupby('Periodo Storico').agg({
        'personale_totale': 'sum',
        'costo_totale': 'sum'
    }).reset_index()

    col1, col2 = st.columns(2)
    with col1:
        fig_pers = px.bar(
            agg, x='Periodo Storico', y='personale_totale',
            title='Personale Totale per Periodo',
            color='Periodo Storico',
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        st.plotly_chart(fig_pers, use_container_width=True, key='period_personale')
    with col2:
        fig_cost = px.bar(
            agg, x='Periodo Storico', y='costo_totale',
            title='Costo Totale per Periodo',
            color='Periodo Storico',
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        st.plotly_chart(fig_cost, use_container_width=True, key='period_costi')

    # Tabella riassuntiva
    agg['costo_totale'] = agg['costo_totale'].apply(format_currency)
    agg['personale_totale'] = agg['personale_totale'].apply(lambda x: f"{x:,.0f}")
    st.dataframe(agg.rename(columns={
        'Periodo Storico': 'Periodo',
        'personale_totale': 'Personale Totale',
        'costo_totale': 'Costo Totale'
    }), use_container_width=True)
    
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
            st.plotly_chart(fig_heatmap, use_container_width=True, key="heatmap_region")
    
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
    
    # 5. ANALISI PER COMMITMENT
    st.markdown('<h2 class="period-header">🎯 Analisi per Tipo di Commitment</h2>', 
                unsafe_allow_html=True)
    
    commitment_stats = create_commitment_analysis(df_filtered)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Grafico per numero di missioni per commitment
        fig_commitment_missions = px.bar(
            commitment_stats,
            x='Tipo Commitment',
            y='Numero Missioni',
            title='Numero di Missioni per Tipo di Commitment',
            color='Tipo Commitment',
            color_discrete_map={
                'Head of Mission': '#1f77b4',
                'Troops': '#ff7f0e'
            }
        )
        st.plotly_chart(fig_commitment_missions, use_container_width=True, key="commitment_missions")
        
        # Grafico a torta per distribuzione commitment
        fig_commitment_pie = px.pie(
            commitment_stats,
            values='Numero Missioni',
            names='Tipo Commitment',
            title='Distribuzione Missioni per Tipo di Commitment'
        )
        st.plotly_chart(fig_commitment_pie, use_container_width=True, key="commitment_pie")
    
    with col2:
        # Grafico per personale per commitment
        fig_commitment_personnel = px.bar(
            commitment_stats,
            x='Tipo Commitment',
            y='Personale Totale',
            title='Personale Totale per Tipo di Commitment',
            color='Tipo Commitment',
            color_discrete_map={
                'Head of Mission': '#1f77b4',
                'Troops': '#ff7f0e'
            }
        )
        st.plotly_chart(fig_commitment_personnel, use_container_width=True, key="commitment_personnel")
        
        # Grafico per costo per commitment
        fig_commitment_cost = px.bar(
            commitment_stats,
            x='Tipo Commitment',
            y='Costo Totale',
            title='Costo Totale per Tipo di Commitment',
            color='Tipo Commitment',
            color_discrete_map={
                'Head of Mission': '#1f77b4',
                'Troops': '#ff7f0e'
            }
        )
        st.plotly_chart(fig_commitment_cost, use_container_width=True, key="commitment_cost")
    
    # Tabella dettagliata per commitment
    st.subheader("📊 Dettagli per Tipo di Commitment")
    
    # Formatta i dati
    commitment_display = commitment_stats.copy()
    commitment_display['Costo Totale'] = commitment_display['Costo Totale'].apply(format_currency)
    commitment_display['Personale Totale'] = commitment_display['Personale Totale'].apply(lambda x: f"{x:,.0f}")
    
    st.dataframe(commitment_display, use_container_width=True)
    
    # Info box per spiegazione commitment
    st.markdown("""
    <div class="info-box">
        <strong>🎯 Classificazione Commitment:</strong><br>
        • <strong>Head of Mission:</strong> Missioni con personale principalmente civile o di supporto, 
          spesso missioni di training, monitoraggio o assistenza tecnica<br>
        • <strong>Troops:</strong> Missioni con significativo dispiegamento di forze militari, 
          incluse operazioni di peacekeeping, sicurezza e supporto logistico
    </div>
    """, unsafe_allow_html=True)
    
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
            st.plotly_chart(heatmap, use_container_width=True, key="heatmap_interactive")
            
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

    # --- SEZIONE FINALE: Commitment dettagliato ---
    st.markdown('<h2 class="period-header">🔎 Commitment dettagliato per missione</h2>', unsafe_allow_html=True)
    df_commitment = create_commitment_detailed(df_filtered)
    st.dataframe(df_commitment, use_container_width=True)
    # Grafico a barre
    st.markdown('**Distribuzione missioni per tipo di commitment**')
    fig_commitment_bar = px.bar(
        df_commitment.groupby('Commitment Dettagliato').size().reset_index(name='Numero Missioni'),
        x='Commitment Dettagliato', y='Numero Missioni', color='Commitment Dettagliato',
        title='Numero di missioni per tipo di commitment',
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.plotly_chart(fig_commitment_bar, use_container_width=True, key='commitment_detailed_bar')

if __name__ == "__main__":
    main() 