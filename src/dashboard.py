import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import yaml

def load_config():
    """Carica la configurazione dal file YAML."""
    config_path = Path('config/config.yaml')
    if not config_path.exists():
        st.error(f"File di configurazione non trovato: {config_path}")
        return None
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        st.error(f"Errore nel caricamento della configurazione: {str(e)}")
        return None

def load_data():
    """Carica i dati dal file Excel."""
    try:
        df = pd.read_excel('data/processed/Matrice dati 1AGG_enriched.xlsx')
        # Converti le date in formato datetime
        date_columns = ['Data Inizio', 'Data Fine']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except Exception as e:
        st.error(f"Errore nel caricamento dei dati: {str(e)}")
        return None

def main():
    st.set_page_config(
        page_title="MIDA - Missioni Internazionali e Dati Analitici",
        page_icon="🌍",
        layout="wide"
    )
    
    st.title("🌍 MIDA - Missioni Internazionali e Dati Analitici")
    
    # Carica i dati
    df = load_data()
    if df is None:
        return
    
    # Mostra le colonne disponibili per debug
    st.sidebar.write("Colonne disponibili:", df.columns.tolist())
    
    # Sidebar per i filtri
    st.sidebar.header("Filtri")
    
    # Filtro per il paese (se la colonna esiste)
    if 'Paese' in df.columns:
        paesi = ['Tutti'] + sorted(df['Paese'].unique().tolist())
        paese_selezionato = st.sidebar.selectbox("Seleziona Paese", paesi)
        if paese_selezionato != 'Tutti':
            df = df[df['Paese'] == paese_selezionato]
    
    # Filtro per il tipo di missione (se la colonna esiste)
    if 'Tipo Missione' in df.columns:
        tipi_missione = ['Tutti'] + sorted(df['Tipo Missione'].unique().tolist())
        tipo_selezionato = st.sidebar.selectbox("Seleziona Tipo Missione", tipi_missione)
        if tipo_selezionato != 'Tutti':
            df = df[df['Tipo Missione'] == tipo_selezionato]
    
    # Layout principale
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Panoramica Missioni")
        
        # Numero totale di missioni
        st.metric("Numero Missioni", len(df))
        
        # Distribuzione per tipo di missione (se la colonna esiste)
        if 'Tipo Missione' in df.columns:
            fig_tipo = px.pie(
                df,
                names='Tipo Missione',
                title='Distribuzione per Tipo di Missione'
            )
            st.plotly_chart(fig_tipo, use_container_width=True)
        
        # Distribuzione per paese (se la colonna esiste)
        if 'Paese' in df.columns:
            fig_paese = px.bar(
                df.groupby('Paese').size().reset_index(name='count'),
                x='Paese',
                y='count',
                title='Numero di Missioni per Paese'
            )
            st.plotly_chart(fig_paese, use_container_width=True)
    
    with col2:
        st.subheader("Dettagli Missioni")
        
        # Timeline delle missioni (se le colonne esistono)
        if all(col in df.columns for col in ['Data Inizio', 'Data Fine', 'Nome Missione']):
            fig_timeline = go.Figure()
            for _, row in df.iterrows():
                fig_timeline.add_trace(go.Scatter(
                    x=[row['Data Inizio'], row['Data Fine']],
                    y=[row['Nome Missione'], row['Nome Missione']],
                    mode='lines+markers',
                    name=row['Nome Missione']
                ))
            
            fig_timeline.update_layout(
                title='Timeline delle Missioni',
                xaxis_title='Data',
                yaxis_title='Missione',
                height=400
            )
            st.plotly_chart(fig_timeline, use_container_width=True)
        
        # Costi delle missioni (se la colonna esiste)
        if 'Costo Totale' in df.columns:
            fig_costi = px.bar(
                df,
                x='Nome Missione',
                y='Costo Totale',
                title='Costi per Missione'
            )
            st.plotly_chart(fig_costi, use_container_width=True)
    
    # Tabella dettagliata
    st.subheader("Dettagli Missioni")
    st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    main() 