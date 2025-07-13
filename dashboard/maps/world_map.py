import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd

# Colori per organizzazione
ORG_COLORS = {
    'ONU': '#1f77b4',      # Blu
    'UE': '#ff7f0e',       # Arancione
    'NATO': '#2ca02c',     # Verde
    'ITA': '#d62728',      # Rosso
    'Bilateral': '#ffd700', # Giallo
    'Multinational': '#9467bd', # Viola
}

def render_world_map(df: pd.DataFrame):
    """
    Visualizza una mappa del mondo con marker colorati per organizzazione e leggenda.
    df deve contenere colonne: 'lat', 'lon', 'nome', 'paese', 'tipo_missione', 'personale_totale'
    """
    st.subheader('🌍 Mappa del Mondo delle Missioni')
    
    # Crea la mappa
    m = folium.Map(location=[30, 10], zoom_start=2, tiles='cartodbpositron')
    
    # Aggiungi marker per ogni missione
    for _, row in df.iterrows():
        color = ORG_COLORS.get(row['tipo_missione'], '#808080')  # Grigio per organizzazioni non mappate
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=6 + (row['personale_totale'] or 0) / 1000,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=folium.Popup(
                f"<b>{row['nome']}</b><br>"
                f"<b>Paese:</b> {row['paese']}<br>"
                f"<b>Organizzazione:</b> {row['tipo_missione']}<br>"
                f"<b>Personale:</b> {row['personale_totale']:,}<br>"
                f"<b>Regione:</b> {row.get('regione', 'N/A')}",
                max_width=300
            )
        ).add_to(m)
    
    # Aggiungi leggenda
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 200px; height: 200px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:14px; padding: 10px">
    <p><b>🏛️ Organizzazioni</b></p>
    '''
    
    # Conta missioni per organizzazione
    org_counts = df['tipo_missione'].value_counts()
    
    for org, color in ORG_COLORS.items():
        count = org_counts.get(org, 0)
        legend_html += f'''
        <p><span style="color:{color};">●</span> {org}: {count} missioni</p>
        '''
    
    legend_html += '</div>'
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Mostra statistiche
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("🎯 Missioni Totali", len(df))
    with col2:
        st.metric("🏛️ Organizzazioni", len(df['tipo_missione'].unique()))
    with col3:
        st.metric("🌍 Paesi", len(df['paese'].unique()))
    
    # Mostra la mappa
    st_folium(m, width=900, height=400)
    
    # Informazioni aggiuntive
    st.info("""
    **🎯 Legenda Mappa:**
    - **Colori:** Ogni organizzazione ha un colore distintivo
    - **Dimensioni:** Basate sul numero di personale
    - **Hover:** Mostra dettagli completi della missione
    - **Legenda:** Mostra tutte le organizzazioni con il numero di missioni
    """) 