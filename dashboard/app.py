import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from pathlib import Path
import yaml

# Configurazione
config_path = Path(__file__).parent.parent.parent / 'config' / 'config.yaml'
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# Funzioni di utilità
def carica_dati():
    """Carica i dati più recenti dal file CSV"""
    try:
        data_path = Path(__file__).parent.parent.parent / 'data' / 'processed' / 'missioni.csv'
        df = pd.read_csv(data_path)
        # Converti le colonne delle date in datetime
        df['data_inizio'] = pd.to_datetime(df['data_inizio']).dt.date
        # Gestisci le date di fine vuote
        df['data_fine'] = pd.to_datetime(df['data_fine'], errors='coerce').dt.date
        # Converti le colonne numeriche
        df['personale'] = pd.to_numeric(df['personale'], errors='coerce')
        df['costo'] = pd.to_numeric(df['costo'], errors='coerce')
        return df
    except Exception as e:
        st.error(f"Errore nel caricamento dei dati: {str(e)}")
        return pd.DataFrame()

def filtra_dati(df, date_range, tipi_missione, paesi):
    """Filtra i dati in base ai parametri selezionati"""
    mask = pd.Series(True, index=df.index)
    
    if date_range and len(date_range) == 2:
        mask &= (df['data_inizio'] >= date_range[0]) & (df['data_inizio'] <= date_range[1])
    
    if tipi_missione:
        mask &= df['tipo'].isin(tipi_missione)
    
    if paesi:
        mask &= df['paese'].isin(paesi)
    
    return df[mask]

# Layout principale
st.set_page_config(page_title="Dashboard Missioni Internazionali", layout="wide")

st.title("Dashboard Missioni Internazionali")

# Carica dati
df = carica_dati()
if df.empty:
    st.error("Nessun dato disponibile")
    st.stop()

# Sidebar per i filtri
st.sidebar.header("Filtri")

# Filtro date
date_range = st.sidebar.date_input(
    "Periodo",
    value=(
        datetime.now() - timedelta(days=365),
        datetime.now()
    )
)

# Filtro tipi missione
tipi_missione = st.sidebar.multiselect(
    "Tipo Missione",
    options=sorted(df['tipo'].unique()),
    default=[]
)

# Filtro paesi
paesi = st.sidebar.multiselect(
    "Paese",
    options=sorted(df['paese'].unique()),
    default=[]
)

# Applica filtri
df_filtrato = filtra_dati(df, date_range, tipi_missione, paesi)

# Layout principale
col1, col2 = st.columns(2)

with col1:
    st.subheader("Distribuzione Missioni per Tipo")
    fig_tipo = px.pie(
        df_filtrato,
        names='tipo',
        title='Distribuzione per Tipo Missione'
    )
    st.plotly_chart(fig_tipo)

with col2:
    st.subheader("Distribuzione Missioni per Paese")
    df_paesi = df_filtrato['paese'].value_counts().reset_index()
    df_paesi.columns = ['paese', 'count']  # Rinomina le colonne
    fig_paese = px.bar(
        df_paesi,
        x='paese',
        y='count',
        title='Numero di Missioni per Paese'
    )
    st.plotly_chart(fig_paese)

# Timeline delle missioni
st.subheader("Timeline delle Missioni")
df_timeline = df_filtrato.sort_values('data_inizio')
fig_timeline = px.timeline(
    df_timeline,
    x_start='data_inizio',
    x_end='data_fine',
    y='nome',
    color='tipo',
    title='Timeline delle Missioni'
)
st.plotly_chart(fig_timeline)

# Statistiche generali
st.subheader("Statistiche Generali")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Totale Missioni",
        len(df_filtrato)
    )

with col2:
    st.metric(
        "Missioni Attive",
        len(df_filtrato[df_filtrato['data_fine'].isna()])
    )

with col3:
    st.metric(
        "Totale Personale",
        f"{df_filtrato['personale'].sum():,.0f}"
    )

with col4:
    st.metric(
        "Costo Totale (€)",
        f"{df_filtrato['costo'].sum():,.2f}"
    )

# Tabella dettagliata
st.subheader("Dettaglio Missioni")
st.dataframe(
    df_filtrato[[
        'nome',
        'paese',
        'data_inizio',
        'data_fine',
        'tipo',
        'personale',
        'costo'
    ]].sort_values('data_inizio', ascending=False)
) 