import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime
import os

def carica_dati():
    """
    Carica i dati da tutti i file JSON nella cartella data
    """
    dati = []
    for filename in os.listdir('data'):
        if filename.endswith('.json'):
            with open(os.path.join('data', filename), 'r', encoding='utf-8') as f:
                dati.extend(json.load(f))
    return pd.DataFrame(dati)

def main():
    st.set_page_config(page_title="MIDA Dashboard", layout="wide")
    
    st.title("MIDA - Missioni Internazionali e Dati Analitici")
    
    # Carica i dati
    df = carica_dati()
    
    # Sidebar per i filtri
    st.sidebar.title("Filtri")
    
    # Filtro per fonte
    fonti = ['Tutte'] + sorted(df['fonte'].unique().tolist())
    fonte_selezionata = st.sidebar.selectbox("Seleziona Fonte", fonti)
    
    # Filtro per periodo
    date_min = pd.to_datetime(df['data_estrazione']).min()
    date_max = pd.to_datetime(df['data_estrazione']).max()
    periodo = st.sidebar.date_input(
        "Seleziona Periodo",
        value=(date_min, date_max),
        min_value=date_min,
        max_value=date_max
    )
    
    # Applica i filtri
    if fonte_selezionata != 'Tutte':
        df = df[df['fonte'] == fonte_selezionata]
    df = df[
        (pd.to_datetime(df['data_estrazione']).dt.date >= periodo[0]) &
        (pd.to_datetime(df['data_estrazione']).dt.date <= periodo[1])
    ]
    
    # Layout principale
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Distribuzione per Fonte")
        fig_fonte = px.pie(
            df,
            names='fonte',
            title='Distribuzione Dati per Fonte'
        )
        st.plotly_chart(fig_fonte)
        
        st.subheader("Andamento Temporale")
        df_temporale = df.groupby(
            pd.to_datetime(df['data_estrazione']).dt.date
        ).size().reset_index(name='count')
        fig_temporale = px.line(
            df_temporale,
            x='data_estrazione',
            y='count',
            title='Andamento Temporale dei Dati'
        )
        st.plotly_chart(fig_temporale)
        
    with col2:
        st.subheader("Distribuzione per Tipo di Missione")
        fig_tipo = px.bar(
            df[df['campo'] == 'tipo_missione'],
            x='valore',
            title='Distribuzione per Tipo di Missione'
        )
        st.plotly_chart(fig_tipo)
        
        st.subheader("Distribuzione Geografica")
        df_paesi = df[df['campo'] == 'paese'].groupby('valore').size().reset_index(name='count')
        fig_paesi = px.choropleth(
            df_paesi,
            locations='valore',
            locationmode='country names',
            color='count',
            title='Distribuzione Geografica delle Missioni'
        )
        st.plotly_chart(fig_paesi)
        
    # Tabella dei dati
    st.subheader("Dati Dettagliati")
    st.dataframe(df)
    
    # Statistiche generali
    st.subheader("Statistiche Generali")
    col3, col4, col5 = st.columns(3)
    
    with col3:
        st.metric("Totale Missioni", len(df[df['campo'] == 'nome_missione'].unique()))
    with col4:
        st.metric("Totale Paesi", len(df[df['campo'] == 'paese'].unique()))
    with col5:
        st.metric("Totale Fonti", len(df['fonte'].unique()))

if __name__ == "__main__":
    main() 