import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import warnings
import os
warnings.filterwarnings('ignore')

# Importa le funzioni delle mappe
try:
    from maps import (
        render_world_map, render_heatmap, render_timeline_map, render_cluster_map,
        add_coordinates_to_dataframe
    )
    MAPS_AVAILABLE = True
except ImportError:
    MAPS_AVAILABLE = False

# Configurazione pagina
st.set_page_config(
    page_title="MIDA - Analisi Missioni Internazionali",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizzato con responsive design
st.markdown("""
<style>
    /* Responsive design */
    @media (max-width: 768px) {
        .main-header {
            font-size: 2rem !important;
        }
        .period-header {
            font-size: 1.2rem !important;
        }
        .metric-card {
            padding: 0.5rem !important;
        }
        .info-box {
            padding: 0.5rem !important;
        }
    }
    
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
    
    /* Mobile optimizations */
    @media (max-width: 480px) {
        .stButton > button {
            width: 100% !important;
            margin: 0.5rem 0 !important;
        }
        .stDataFrame {
            font-size: 0.8rem !important;
        }
        .stPlotlyChart {
            height: 300px !important;
        }
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=60)  # Cache per 60 secondi per permettere aggiornamenti
def load_data():
    """Carica e prepara i dati delle missioni"""
    try:
        # Carica SOLO il file principale con campo is_active
        df = pd.read_csv('data/processed/missioni_complete.csv')
        
        # NON integrare dati Excel per preservare il campo is_active
        # df = integrate_excel_data(df)
        
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
    
    # Normalizza le regioni per accorpare "Americas" e "America"
    df = normalize_regions(df)
    
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
                # Estendi fino al 2025 per le missioni attive
                df.at[idx, 'data_fine'] = pd.Timestamp('2025-12-31')
    
    # Assicurati che la colonna is_active esista
    if 'is_active' not in df.columns:
        # Inizializza a False per tutte le missioni
        df['is_active'] = False
        # Nota: il campo is_active dovrebbe essere già presente nel CSV principale
        # Se manca, significa che sono stati integrati dati Excel senza questo campo
    
    # Rimuovi colonne duplicate se presenti
    df = df.loc[:, ~df.columns.duplicated()]
    
    return df

def normalize_regions(df):
    """Normalizza le regioni per accorpare varianti come 'Americas' e 'America'"""
    if 'regione' not in df.columns:
        return df
    
    # Mappa di normalizzazione delle regioni
    region_mapping = {
        'Americas': 'America',
        'America': 'America',
        'North America': 'America',
        'South America': 'America',
        'Central America': 'America',
        'Latin America': 'America',
        'Caribbean': 'America',
        'Caraibi': 'America',
        'America Centrale': 'America',
        'America Meridionale': 'America',
        'America Settentrionale': 'America',
        'America Latina': 'America',
        
        # Altre normalizzazioni comuni
        'Middle East': 'Medio Oriente',
        'Medio Oriente': 'Medio Oriente',
        'Near East': 'Medio Oriente',
        'Vicino Oriente': 'Medio Oriente',
        
        'Europe': 'Europa',
        'Europa': 'Europa',
        'European Union': 'Europa',
        'Unione Europea': 'Europa',
        
        'Asia': 'Asia',
        'Asia Centrale': 'Asia',
        'Asia Sudorientale': 'Asia',
        'Asia Meridionale': 'Asia',
        'Asia Orientale': 'Asia',
        'Far East': 'Asia',
        'Estremo Oriente': 'Asia',
        
        'Africa': 'Africa',
        'Sub-Saharan Africa': 'Africa',
        'Africa Sub-sahariana': 'Africa',
        'Northern Africa': 'Africa',
        'Nord Africa': 'Africa',
        'Western Africa': 'Africa',
        'Africa Occidentale': 'Africa',
        'Eastern Africa': 'Africa',
        'Africa Orientale': 'Africa',
        'Central Africa': 'Africa',
        'Africa Centrale': 'Africa',
        'Southern Africa': 'Africa',
        'Africa Meridionale': 'Africa',
        'Horn of Africa': 'Africa',
        'Corno d\'Africa': 'Africa'
    }
    
    # Applica la normalizzazione
    df['regione'] = df['regione'].map(region_mapping).fillna(df['regione'])
    
    return df

def integrate_excel_data(df_existing):
    """Integra i dati dai nuovi file Excel evitando duplicati e normalizzando organizzazioni"""
    # TEMPORANEAMENTE DISABILITATO: L'integrazione Excel sovrascrive il campo is_active
    # Ritorna il dataframe esistente senza modifiche
    return df_existing
    
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
                # Normalizza l'organizzazione
                framework = str(row['framework']).strip() if pd.notna(row['framework']) else 'ONU'
                normalized_org = normalize_organization(mission_name, framework)
                
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
                    'tipo_missione': normalized_org,
                    'commitment': 'Troops',  # Default
                    'is_active': False  # Le missioni da Excel sono storiche, non attive
                }
                new_missions.append(new_mission)
        
        # Aggiungi i nuovi dati se ce ne sono
        if new_missions:
            new_df = pd.DataFrame(new_missions)
            df_existing = pd.concat([df_existing, new_df], ignore_index=True)
            st.success(f"Integrati {len(new_missions)} nuovi record dalle fonti Excel")
        
        # Normalizza tutte le organizzazioni nel dataset
        df_existing = normalize_all_organizations(df_existing)
        
        # Normalizza le regioni per accorpare varianti come 'Americas' e 'America'
        df_existing = normalize_regions(df_existing)
        
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

def normalize_organization(mission_name, framework):
    """Normalizza l'organizzazione di una missione"""
    import re
    
    # Lista di pattern per identificare missioni ONU
    onu_patterns = [
        r'^UN[A-Z]',  # UNIFIL, UNMISS, UNPROFOR, etc.
        r'^MINU',     # MINURSO, MINUSTAH, etc.
        r'^UNAM',     # UNAMA, UNAMID, etc.
        r'^UNS',      # UNSCOM, UNSMIL, etc.
        r'^UNT',      # UNTAET, UNTMIH, etc.
        r'^UNM',      # UNMIK, UNMIL, etc.
        r'^UNI',      # UNISFA, etc.
        r'^UNF',      # UNFICYP, etc.
        r'^UNO',      # UNOCI, etc.
        r'^UNMOG',    # UNMOGIP
        r'^UNTSO',    # UNTSO
        r'peacekeeping',
        r'peace\s*keeping',
        r'united\s*nations',
        r'nazioni\s*unite'
    ]
    
    # Controlla se è una missione ONU
    mission_lower = str(mission_name).lower()
    framework_lower = str(framework).lower()
    
    for pattern in onu_patterns:
        if re.search(pattern, mission_lower, re.IGNORECASE):
            return 'ONU'
        if re.search(pattern, framework_lower, re.IGNORECASE):
            return 'ONU'
    
    # Controlla se il framework è già ONU o UN
    if framework_lower in ['onu', 'un', 'united nations', 'nazioni unite']:
        return 'ONU'
    
    # Altre organizzazioni
    if framework_lower in ['nato', 'north atlantic treaty organization']:
        return 'NATO'
    elif framework_lower in ['ue', 'eu', 'european union', 'unione europea']:
        return 'UE'
    elif framework_lower in ['ita', 'italia', 'italian']:
        return 'ITA'
    elif framework_lower in ['coalizione', 'coalition', 'multinational', 'multinazionale']:
        return 'Multinational'
    elif framework_lower in ['bilaterale', 'bilateral', 'bilaterale', 'bilateral']:
        return 'Bilateral'
    
    # Default
    return framework if framework != 'nan' else 'ONU'

def normalize_all_organizations(df):
    """Normalizza tutte le organizzazioni nel dataset"""
    for idx, row in df.iterrows():
        nome = row['nome']
        tipo_missione = row['tipo_missione']
        
        # Normalizza l'organizzazione
        normalized_org = normalize_organization(nome, tipo_missione)
        if normalized_org != tipo_missione:
            df.at[idx, 'tipo_missione'] = normalized_org
    
    return df

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

def create_timeline_by_organization(df):
    """Crea timeline raggruppata per organizzazione con visualizzazione migliorata"""
    # Raggruppa per organizzazione e anno
    df_timeline = df.copy()
    df_timeline['anno'] = df_timeline['data_inizio'].dt.year
    
    # Calcola statistiche per organizzazione e anno
    org_year_stats = df_timeline.groupby(['tipo_missione', 'anno']).agg({
        'nome': 'count',
        'personale_totale': 'sum',
        'costo_totale': 'sum'
    }).reset_index()
    
    # Colori per organizzazione
    color_map = {
        'ONU': '#1f77b4',
        'UE': '#ff7f0e',
        'NATO': '#2ca02c',
        'ITA': '#d62728',
        'Multinational': '#9467bd',
        'Bilateral': '#8c564b',
        'Coalizione': '#e377c2'
    }
    
    fig = go.Figure()
    
    # Crea subplot per ogni organizzazione
    organizations = df_timeline['tipo_missione'].unique()
    fig = make_subplots(
        rows=len(organizations), 
        cols=1,
        subplot_titles=[f'🏛️ {org}' for org in organizations],
        vertical_spacing=0.05,
        specs=[[{"secondary_y": True}] for _ in organizations]
    )
    
    for i, org in enumerate(organizations, 1):
        org_data = org_year_stats[org_year_stats['tipo_missione'] == org]
        color = color_map.get(org, '#7f7f7f')
        
        # Barre per numero di missioni
        fig.add_trace(
            go.Bar(
                x=org_data['anno'],
                y=org_data['nome'],
                name=f'{org} - Missioni',
                marker_color=color,
                opacity=0.8,
                hovertemplate=f"<b>{org}</b><br>" +
                             f"Anno: %{{x}}<br>" +
                             f"Missioni: %{{y}}<br>" +
                             "<extra></extra>"
            ),
            row=i, col=1
        )
        
        # Linea per personale totale
        fig.add_trace(
            go.Scatter(
                x=org_data['anno'],
                y=org_data['personale_totale'],
                name=f'{org} - Personale',
                mode='lines+markers',
                line=dict(color=color, width=3),
                marker=dict(size=8),
                yaxis='y2',
                hovertemplate=f"<b>{org}</b><br>" +
                             f"Anno: %{{x}}<br>" +
                             f"Personale: %{{y:,.0f}}<br>" +
                             "<extra></extra>"
            ),
            row=i, col=1,
            secondary_y=True
        )
    
    fig.update_layout(
        title='Timeline Missioni per Organizzazione (1948-2025)',
        height=300 * len(organizations),
        showlegend=False,
        hovermode='closest'
    )
    
    # Aggiorna layout per ogni subplot
    for i in range(len(organizations)):
        fig.update_xaxes(title_text="Anno", row=i+1, col=1)
        fig.update_yaxes(title_text="Numero Missioni", row=i+1, col=1)
        fig.update_yaxes(title_text="Personale Totale", row=i+1, col=1, secondary_y=True)
    
    return fig

def create_timeline_by_region(df):
    """Crea timeline raggruppata per regione con visualizzazione migliorata"""
    # Raggruppa per regione e anno
    df_timeline = df.copy()
    df_timeline['anno'] = df_timeline['data_inizio'].dt.year
    
    # Calcola statistiche per regione e anno
    region_year_stats = df_timeline.groupby(['regione', 'anno']).agg({
        'nome': 'count',
        'personale_totale': 'sum',
        'costo_totale': 'sum'
    }).reset_index()
    
    # Colori per regione
    region_colors = {
        'Africa': '#e74c3c',
        'Europa': '#3498db',
        'Medio Oriente': '#f39c12',
        'Asia': '#9b59b6',
        'America': '#2ecc71'
    }
    
    # Crea subplot per ogni regione
    regions = df_timeline['regione'].unique()
    fig = make_subplots(
        rows=len(regions), 
        cols=1,
        subplot_titles=[f'🌍 {region}' for region in regions],
        vertical_spacing=0.05,
        specs=[[{"secondary_y": True}] for _ in regions]
    )
    
    for i, region in enumerate(regions, 1):
        region_data = region_year_stats[region_year_stats['regione'] == region]
        color = region_colors.get(region, '#95a5a6')
        
        # Barre per numero di missioni
        fig.add_trace(
            go.Bar(
                x=region_data['anno'],
                y=region_data['nome'],
                name=f'{region} - Missioni',
                marker_color=color,
                opacity=0.8,
                hovertemplate=f"<b>{region}</b><br>" +
                             f"Anno: %{{x}}<br>" +
                             f"Missioni: %{{y}}<br>" +
                             "<extra></extra>"
            ),
            row=i, col=1
        )
        
        # Linea per personale totale
        fig.add_trace(
            go.Scatter(
                x=region_data['anno'],
                y=region_data['personale_totale'],
                name=f'{region} - Personale',
                mode='lines+markers',
                line=dict(color=color, width=3),
                marker=dict(size=8),
                yaxis='y2',
                hovertemplate=f"<b>{region}</b><br>" +
                             f"Anno: %{{x}}<br>" +
                             f"Personale: %{{y:,.0f}}<br>" +
                             "<extra></extra>"
            ),
            row=i, col=1,
            secondary_y=True
        )
    
    fig.update_layout(
        title='Timeline Missioni per Regione (1948-2025)',
        height=300 * len(regions),
        showlegend=False,
        hovermode='closest'
    )
    
    # Aggiorna layout per ogni subplot
    for i in range(len(regions)):
        fig.update_xaxes(title_text="Anno", row=i+1, col=1)
        fig.update_yaxes(title_text="Numero Missioni", row=i+1, col=1)
        fig.update_yaxes(title_text="Personale Totale", row=i+1, col=1, secondary_y=True)
    
    return fig

def create_timeline_with_duration(df):
    """Crea timeline con durata delle missioni usando barre orizzontali migliorate"""
    # Calcola durata in giorni
    df_duration = df.copy()
    df_duration['durata_giorni'] = (df_duration['data_fine'] - df_duration['data_inizio']).dt.days
    df_duration['durata_anni'] = df_duration['durata_giorni'] / 365.25
    
    # Filtra solo missioni con durata > 30 giorni per evitare rumore
    df_duration = df_duration[df_duration['durata_giorni'] > 30]
    
    # Raggruppa per organizzazione e mostra top 15 per durata
    top_missions = df_duration.nlargest(15, 'durata_giorni')
    
    # Colori per organizzazione
    color_map = {
        'ONU': '#1f77b4',
        'UE': '#ff7f0e',
        'NATO': '#2ca02c',
        'ITA': '#d62728',
        'Multinational': '#9467bd',
        'Bilateral': '#8c564b',
        'Coalizione': '#e377c2'
    }
    
    # Crea figura con subplot per organizzazione
    organizations = top_missions['tipo_missione'].unique()
    fig = make_subplots(
        rows=len(organizations), 
        cols=1,
        subplot_titles=[f'🏛️ {org} - Missioni più longeve' for org in organizations],
        vertical_spacing=0.08,
        specs=[[{"secondary_y": False}] for _ in organizations]
    )
    
    for i, org in enumerate(organizations, 1):
        org_missions = top_missions[top_missions['tipo_missione'] == org]
        color = color_map.get(org, '#7f7f7f')
        
        # Ordina per durata (più lunghe in alto)
        org_missions = org_missions.sort_values('durata_anni', ascending=True)
        
        for _, row in org_missions.iterrows():
            # Crea etichetta compatta
            label = f"{row['nome'][:20]}... ({row['durata_anni']:.1f}a)"
            
            # Barra orizzontale per durata
            fig.add_trace(
                go.Bar(
                    x=[row['durata_anni']],
                    y=[label],
                    orientation='h',
                    name=org,
                    marker_color=color,
                    opacity=0.8,
                    hovertemplate=f"<b>{row['nome']}</b><br>" +
                                 f"Paese: {row['paese']}<br>" +
                                 f"Durata: {row['durata_anni']:.1f} anni ({row['durata_giorni']} giorni)<br>" +
                                 f"Personale: {row['personale_totale']:,.0f}<br>" +
                                 f"Costo: €{row['costo_totale']:,.0f}<br>" +
                                 f"Inizio: {row['data_inizio'].strftime('%Y-%m-%d')}<br>" +
                                 f"Fine: {row['data_fine'].strftime('%Y-%m-%d')}<br>" +
                                 "<extra></extra>"
                ),
                row=i, col=1
            )
    
    fig.update_layout(
        title='Timeline con Durata delle Missioni (Top 15 per organizzazione)',
        height=200 * len(organizations),
        showlegend=False,
        hovermode='closest'
    )
    
    # Aggiorna layout per ogni subplot
    for i in range(len(organizations)):
        fig.update_xaxes(title_text="Durata (anni)", row=i+1, col=1)
        fig.update_yaxes(title_text="Missione", row=i+1, col=1)
    
    return fig

def create_interactive_timeline(df, selected_years):
    """Crea una timeline interattiva per il periodo selezionato"""
    # Raggruppa per mese e organizzazione
    df_timeline = df.copy()
    df_timeline['anno_mese'] = df_timeline['data_inizio'].dt.to_period('M')
    
    # Calcola statistiche mensili
    monthly_stats = df_timeline.groupby(['anno_mese', 'tipo_missione']).agg({
        'nome': 'count',
        'personale_totale': 'sum'
    }).reset_index()
    
    # Converti periodo in datetime per il plotting
    monthly_stats['data'] = monthly_stats['anno_mese'].dt.to_timestamp()
    
    # Colori per organizzazione
    color_map = {
        'ONU': '#1f77b4',
        'UE': '#ff7f0e',
        'NATO': '#2ca02c',
        'ITA': '#d62728',
        'Multinational': '#9467bd',
        'Bilateral': '#8c564b',
        'Coalizione': '#e377c2'
    }
    
    fig = go.Figure()
    
    # Aggiungi tracce per ogni organizzazione
    for org in monthly_stats['tipo_missione'].unique():
        org_data = monthly_stats[monthly_stats['tipo_missione'] == org]
        color = color_map.get(org, '#7f7f7f')
        
        # Linea per numero di missioni
        fig.add_trace(go.Scatter(
            x=org_data['data'],
            y=org_data['nome'],
            mode='lines+markers',
            name=f'🏛️ {org}',
            line=dict(color=color, width=3),
            marker=dict(size=8),
            hovertemplate=f"<b>{org}</b><br>" +
                         f"Data: %{{x|%B %Y}}<br>" +
                         f"Nuove missioni: %{{y}}<br>" +
                         "<extra></extra>"
        ))
    
    # Aggiungi area per personale totale
    total_personnel = monthly_stats.groupby('data')['personale_totale'].sum().reset_index()
    fig.add_trace(go.Scatter(
        x=total_personnel['data'],
        y=total_personnel['personale_totale'],
        mode='lines',
        name='👥 Personale Totale',
        line=dict(color='#2ecc71', width=4, dash='dash'),
        yaxis='y2',
        hovertemplate=f"<b>Personale Totale</b><br>" +
                     f"Data: %{{x|%B %Y}}<br>" +
                     f"Personale: %{{y:,.0f}}<br>" +
                     "<extra></extra>"
    ))
    
    fig.update_layout(
        title=f'Timeline Interattiva ({selected_years[0]}-{selected_years[1]})',
        xaxis_title='Data',
        yaxis_title='Nuove Missioni',
        height=500,
        hovermode='closest',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ),
        yaxis2=dict(
            title="Personale Totale",
            overlaying="y",
            side="right"
        )
    )
    
    return fig

def main():
    # Header principale
    st.markdown('<h1 class="main-header">🌍 MIDA - Missioni Internazionali e Dati Analitici</h1>', 
                unsafe_allow_html=True)
    
    # Info box
    st.markdown("""
    <div class="info-box">
        <strong>📊 Dashboard Interattiva</strong><br>
        Analisi completa delle missioni internazionali italiane dal 1949 ad oggi. 
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
    st.sidebar.write(f"Missioni attive: {(df['is_active'] == True).sum()}")
    
    # Dati geografici non più necessari con le nuove mappe
    # geo_df = load_geo_data()
    
    # Salva i dati in session_state per le notifiche
    st.session_state['df'] = df
    
    # Debug info sempre visibile in sidebar
    st.sidebar.markdown('---')
    st.sidebar.header('🛠️ Debug Dati Missioni')
    
    # Pulsante per ricaricare i dati
    if st.sidebar.button("🔄 Ricarica Dati"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.write(f"Missioni caricate dal CSV: {len(df)}")
    st.sidebar.write('Nomi missioni caricate:')
    for nome in df['nome'].unique():
        st.sidebar.write(f'- {nome}')
    # Missioni con date non valide
    invalid_dates = df[df['data_inizio'].isna() | (df['is_active'] == True)]
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
    
    # Sistema di notifiche
    try:
        from scripts.notification_system import display_notifications
        display_notifications()
    except ImportError:
        st.sidebar.info("Sistema di notifiche non disponibile")
    
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
        # Calcolo missioni attive usando il campo is_active (allineato con sito Ministero Difesa)
        missioni_attive = len(df_filtered[df_filtered['is_active'] == True])
        st.metric("🟢 Missioni Attive", missioni_attive)
    
    # Statistiche aggiuntive
    col1, col2, col3, col4 = st.columns(4)
    
# ... (rest of the code remains the same)
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
        max_year = int(df_filtered['data_fine'].dt.year.max())
        
        # Forza il range fino al 2025 per le missioni attive
        max_year = max(max_year, 2025)
        
        # Controllo per evitare min_value = max_value
        if min_year == max_year:
            # Se tutti i dati sono dello stesso anno, usa un range più ampio
            min_year = max(1948, min_year - 1)
            max_year = min(2025, max_year + 1)
        
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
                'ITA': '#d62728',
                'Multinational': '#9467bd',
                'Bilateral': '#8c564b',
                'Coalizione': '#e377c2'
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
                'ITA': '#d62728',
                'Multinational': '#9467bd',
                'Bilateral': '#8c564b',
                'Coalizione': '#e377c2'
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
    

    
    # 5. TIMELINE DELLE MISSIONI MIGLIORATA
    st.markdown('<h2 class="period-header">⏰ Timeline delle Missioni</h2>', 
                unsafe_allow_html=True)
    
    # Tab per diverse visualizzazioni timeline
    timeline_tab1, timeline_tab2, timeline_tab3 = st.tabs([
        "📊 Timeline per Organizzazione", 
        "🌍 Timeline per Regione", 
        "📈 Timeline con Durata"
    ])
    
    with timeline_tab1:
        st.subheader("📊 Timeline per Organizzazione")
        
        # Timeline raggruppata per organizzazione
        fig_timeline_org = create_timeline_by_organization(df_filtered)
        st.plotly_chart(fig_timeline_org, use_container_width=True, key="timeline_organization")
        
        st.info("""
        **📊 Timeline per Organizzazione:**
        - Subplot separati per ogni organizzazione
        - Barre per numero di missioni per anno
        - Linee per personale totale nel tempo
        - Visualizzazione ottimizzata per 200+ missioni
        """)
    
    with timeline_tab2:
        st.subheader("🌍 Timeline per Regione")
        
        # Timeline raggruppata per regione
        fig_timeline_region = create_timeline_by_region(df_filtered)
        st.plotly_chart(fig_timeline_region, use_container_width=True, key="timeline_region")
        
        st.info("""
        **🌍 Timeline per Regione:**
        - Subplot separati per ogni regione geografica
        - Evoluzione temporale delle missioni per area
        - Analisi dell'espansione geografica dell'impegno italiano
        """)
    
    with timeline_tab3:
        st.subheader("📈 Timeline con Durata Missioni")
        
        # Timeline con durata delle missioni
        fig_timeline_duration = create_timeline_with_duration(df_filtered)
        st.plotly_chart(fig_timeline_duration, use_container_width=True, key="timeline_duration")
        
        st.info("""
        **📈 Timeline con Durata:**
        - Barre orizzontali per durata delle missioni
        - Subplot separati per ogni organizzazione
        - Top 15 missioni più longeve per organizzazione
        - Durata espressa in anni per maggiore chiarezza
        - Visualizzazione ottimizzata per confrontare la longevità delle missioni
        """)
    
    # Nuova timeline interattiva con slider
    st.markdown("---")
    st.markdown('<h3 class="period-header">🎛️ Timeline Interattiva</h3>', unsafe_allow_html=True)
    
    # Slider per selezionare il periodo
    min_year = int(df_filtered['data_inizio'].dt.year.min())
    max_year = int(df_filtered['data_fine'].dt.year.max())
    
    # Forza il range fino al 2025 per le missioni attive
    max_year = max(max_year, 2025)
    
    col1, col2 = st.columns([3, 1])
    with col1:
        selected_years = st.slider(
            "Seleziona periodo temporale",
            min_value=min_year,
            max_value=max_year,
            value=(min_year, max_year),
            step=1
        )
    
    with col2:
        st.metric("Anni selezionati", f"{selected_years[1] - selected_years[0] + 1}")
    
    # Filtra dati per il periodo selezionato
    df_period = df_filtered[
        (df_filtered['data_inizio'].dt.year >= selected_years[0]) &
        (df_filtered['data_fine'].dt.year <= selected_years[1])
    ]
    
    if len(df_period) > 0:
        # Timeline interattiva per il periodo selezionato
        fig_interactive = create_interactive_timeline(df_period, selected_years)
        st.plotly_chart(fig_interactive, use_container_width=True, key="interactive_timeline")
        
        # Statistiche del periodo
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Missioni nel periodo", len(df_period))
        with col2:
            st.metric("Personale totale", f"{df_period['personale_totale'].sum():,.0f}")
        with col3:
            st.metric("Costo totale", format_currency(df_period['costo_totale'].sum()))
        with col4:
            st.metric("Organizzazioni", len(df_period['tipo_missione'].unique()))
    else:
        st.warning("Nessuna missione trovata per il periodo selezionato")
    
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
    
    col1, col2, col3 = st.columns(3)
    
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
    
    with col3:
        # Genera PDF Report
        if st.button("📋 Genera Report PDF", type="primary"):
            try:
                from scripts.pdf_report_generator import create_pdf_report
                import tempfile
                import time
                
                with st.spinner("Generando report PDF..."):
                    # Crea file temporaneo
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                        report_path = create_pdf_report(df_filtered, tmp_file.name)
                        
                        # Leggi il file PDF
                        with open(report_path, 'rb') as f:
                            pdf_data = f.read()
                        
                        # Chiudi il file prima di eliminarlo
                        f.close()
                        
                        # Aspetta un momento e poi elimina il file temporaneo
                        time.sleep(0.1)
                        try:
                            os.unlink(report_path)
                        except OSError:
                            # Se non riesce a eliminare il file, non è un problema critico
                            pass
                
                # Download del PDF
                st.download_button(
                    label="📋 Scarica Report PDF",
                    data=pdf_data,
                    file_name=f"report_missioni_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                    mime="application/pdf"
                )
                
                st.success("✅ Report PDF generato con successo!")
                
            except ImportError:
                st.error("❌ Per generare PDF, installa: pip install reportlab")
            except Exception as e:
                st.error(f"❌ Errore nella generazione PDF: {str(e)}")
    
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

    # 🗺️ MAPPE INTERATTIVE AVANZATE (SPOSTATE ALLA FINE)
    st.markdown("---")
    st.markdown('<h2 class="period-header">🗺️ Mappe Interattive Avanzate</h2>', 
                unsafe_allow_html=True)
    
    if MAPS_AVAILABLE:
        st.subheader("🗺️ Mappe delle Missioni per Organizzazione")
        
        # Prepara dati geografici (lat/lon) usando geocoding corretto
        if 'lat' not in df_filtered.columns or 'lon' not in df_filtered.columns:
            df_filtered = add_coordinates_to_dataframe(df_filtered)

        # Filtro per organizzazione
        organizzazioni_disponibili = sorted(df_filtered['tipo_missione'].unique())
        organizzazione_selezionata = st.selectbox(
            'Seleziona organizzazione per visualizzare le mappe:',
            ['Tutte le organizzazioni'] + organizzazioni_disponibili
        )
        
        # Filtra i dati per l'organizzazione selezionata
        if organizzazione_selezionata != 'Tutte le organizzazioni':
            df_mappa = df_filtered[df_filtered['tipo_missione'] == organizzazione_selezionata]
            st.info(f"Mostrando {len(df_mappa)} missioni dell'organizzazione: {organizzazione_selezionata}")
        else:
            df_mappa = df_filtered
            st.info(f"Mostrando tutte le {len(df_mappa)} missioni")
        
        # Tab per diverse tipologie di mappe
        map_tab1, map_tab2, map_tab3, map_tab4 = st.tabs([
            "🌍 Mappa del Mondo", 
            "🔥 Mappa di Calore", 
            "⏰ Timeline", 
            "🔗 Cluster"
        ])
        
        with map_tab1:
            st.subheader(f"🌍 Mappa del Mondo - {organizzazione_selezionata}")
            render_world_map(df_mappa)
            
        with map_tab2:
            st.subheader(f"🔥 Mappa di Calore - {organizzazione_selezionata}")
            render_heatmap(df_mappa)
            
        with map_tab3:
            st.subheader(f"⏰ Timeline - {organizzazione_selezionata}")
            render_timeline_map(df_mappa)
            
        with map_tab4:
            st.subheader(f"🔗 Cluster - {organizzazione_selezionata}")
            render_cluster_map(df_mappa)
        
        # Sezione dedicata alle statistiche delle organizzazioni
        st.markdown("---")
        st.markdown('<h3 class="period-header">📊 Statistiche delle Organizzazioni nelle Mappe</h3>', 
                    unsafe_allow_html=True)
        
        # Calcola statistiche per organizzazione
        org_map_stats = df_mappa.groupby('tipo_missione').agg({
            'nome': 'count',
            'personale_totale': 'sum',
            'costo_totale': 'sum'
        }).reset_index()
        org_map_stats.columns = ['Organizzazione', 'Numero Missioni', 'Personale Totale', 'Costo Totale']
        
        # Mostra statistiche in colonne
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            <div style="background-color: #f0f2f6; padding: 15px; border-radius: 8px; border-left: 4px solid #1f77b4;">
                <h4 style="margin: 0 0 10px 0; color: #1f77b4;">🏛️ Organizzazioni</h4>
            """, unsafe_allow_html=True)
            
            for _, row in org_map_stats.iterrows():
                color_map = {
                    'ONU': '#1f77b4',
                    'UE': '#ff7f0e', 
                    'NATO': '#2ca02c',
                    'ITA': '#d62728',
                    'Multinational': '#9467bd',
                    'Bilateral': '#8c564b'
                }
                color = color_map.get(row['Organizzazione'], '#666')
                st.markdown(f"""
                <div style="margin: 5px 0; display: flex; align-items: center;">
                    <span style="color: {color}; font-size: 16px; margin-right: 8px;">●</span>
                    <span style="flex: 1; font-weight: bold;">{row['Organizzazione']}</span>
                    <span style="color: #666;">{row['Numero Missioni']}</span>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div style="background-color: #f0f2f6; padding: 15px; border-radius: 8px; border-left: 4px solid #ff7f0e;">
                <h4 style="margin: 0 0 10px 0; color: #ff7f0e;">👥 Personale Totale</h4>
            """, unsafe_allow_html=True)
            
            for _, row in org_map_stats.iterrows():
                st.markdown(f"""
                <div style="margin: 5px 0; display: flex; justify-content: space-between;">
                    <span style="font-weight: bold;">{row['Organizzazione']}</span>
                    <span style="color: #666;">{row['Personale Totale']:,.0f}</span>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div style="background-color: #f0f2f6; padding: 15px; border-radius: 8px; border-left: 4px solid #2ca02c;">
                <h4 style="margin: 0 0 10px 0; color: #2ca02c;">💰 Costo Totale</h4>
            """, unsafe_allow_html=True)
            
            for _, row in org_map_stats.iterrows():
                st.markdown(f"""
                <div style="margin: 5px 0; display: flex; justify-content: space-between;">
                    <span style="font-weight: bold;">{row['Organizzazione']}</span>
                    <span style="color: #666;">€{row['Costo Totale']:,.0f}</span>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Tabella riassuntiva
        st.markdown("### 📋 Tabella Riassuntiva")
        org_display = org_map_stats.copy()
        org_display['Personale Totale'] = org_display['Personale Totale'].apply(lambda x: f"{x:,.0f}")
        org_display['Costo Totale'] = org_display['Costo Totale'].apply(format_currency)
        st.dataframe(org_display, use_container_width=True)
        
    else:
        st.warning("⚠️ Le funzioni delle mappe non sono disponibili. Installa le dipendenze con: pip install folium geopandas pydeck geopy")
        st.info("📦 Dipendenze mancanti per le mappe:")
        st.code("pip install folium>=0.14.0 geopandas>=0.12.0 pydeck>=0.8.0 geopy>=2.3.0")

if __name__ == "__main__":
    main() 