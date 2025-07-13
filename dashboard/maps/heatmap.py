import streamlit as st
import folium
from folium.plugins import HeatMap
from streamlit_folium import st_folium
import pandas as pd

def render_heatmap(df: pd.DataFrame):
    """
    Visualizza una mappa di calore della densità del personale impiegato nelle missioni.
    df deve contenere colonne: 'lat', 'lon', 'personale_totale'
    """
    st.subheader('🔥 Mappa di Calore del Personale Impiegato')
    
    # Crea la mappa
    m = folium.Map(location=[30, 10], zoom_start=2, tiles='cartodbpositron')
    
    # Prepara i dati per la heatmap
    heat_data = [
        [row['lat'], row['lon'], row['personale_totale'] or 0]
        for _, row in df.iterrows()
    ]
    
    # Aggiungi la heatmap
    HeatMap(heat_data, radius=25, blur=15, min_opacity=0.3, max_zoom=6).add_to(m)
    
    # Aggiungi leggenda per la heatmap
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 200px; height: 120px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:14px; padding: 10px">
    <p><b>🔥 Densità Personale</b></p>
    <p>🔵 Bassa → 🔴 Alta</p>
    <p>Basata su {total_personnel:,} persone</p>
    </div>
    '''.format(total_personnel=df['personale_totale'].sum())
    
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Mostra statistiche
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("👥 Personale Totale", f"{df['personale_totale'].sum():,}")
    with col2:
        st.metric("📊 Media per Missione", f"{df['personale_totale'].mean():.0f}")
    with col3:
        st.metric("🔥 Zone di Calore", len([d for d in heat_data if d[2] > 0]))
    
    # Mostra la mappa
    st_folium(m, width=900, height=400)
    
    # Informazioni aggiuntive
    st.info("""
    **🔥 Mappa di Calore:**
    - Mostra la densità del personale impiegato
    - Zone più scure = più personale concentrato
    - Utile per identificare aree di maggiore impegno
    - Scala colori: Blu (basso) → Rosso (alto)
    """) 