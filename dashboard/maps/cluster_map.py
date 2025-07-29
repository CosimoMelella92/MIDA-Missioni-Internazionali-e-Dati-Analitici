import streamlit as st
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
import pandas as pd

# Colori per organizzazione (stessi della world map)
ORG_COLORS = {
    'ONU': '#1f77b4',      # Blu
    'UE': '#ff7f0e',       # Arancione
    'NATO': '#2ca02c',     # Verde
    'ITA': '#d62728',      # Rosso
    'Bilateral': '#ffd700', # Giallo
    'Multinational': '#9467bd', # Viola
}

def render_cluster_map(df: pd.DataFrame):
    """
    Visualizza una mappa con marker clusterizzati per missioni vicine.
    df deve contenere colonne: 'lat', 'lon', 'nome', 'paese', 'tipo_missione', 'personale_totale'
    """
    st.subheader('📍 Mappa Cluster delle Missioni')
    
    # Crea la mappa
    m = folium.Map(location=[30, 10], zoom_start=2, tiles='cartodbpositron')
    
    # Crea cluster separati per organizzazione
    clusters = {}
    for org in df['tipo_missione'].unique():
        color = ORG_COLORS.get(org, '#808080')
        clusters[org] = MarkerCluster(
            name=org,
            overlay=True,
            control=True,
            icon_create_function=f'''
            function(cluster) {{
                return L.divIcon({{
                    html: '<div style="background-color: {color}; color: white; border-radius: 50%; width: 30px; height: 30px; display: flex; align-items: center; justify-content: center; font-weight: bold;">' + cluster.getChildCount() + '</div>',
                    className: 'marker-cluster',
                    iconSize: L.point(30, 30)
                }});
            }}
            '''
        ).add_to(m)
    
    # Aggiungi marker ai cluster appropriati
    for _, row in df.iterrows():
        org = row['tipo_missione']
        cluster = clusters.get(org, clusters.get(list(clusters.keys())[0]))
        
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=folium.Popup(
                f"<b>{row['nome']}</b><br>"
                f"<b>Paese:</b> {row['paese']}<br>"
                f"<b>Organizzazione:</b> {org}<br>"
                f"<b>Personale:</b> {row['personale_totale']:,}<br>"
                f"<b>Regione:</b> {row.get('regione', 'N/A')}",
                max_width=300
            ),
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(cluster)
    
    # Aggiungi layer control
    folium.LayerControl().add_to(m)
    
    # Aggiungi leggenda
    legend_html = '''
    <div style="position: fixed; 
                top: 20px; right: 20px; width: 250px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:12px; padding: 15px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
    <p style="margin: 0 0 10px 0; font-weight: bold; font-size: 14px; text-align: center;">📍 Cluster per Organizzazione</p>
    '''
    
    # Conta missioni per organizzazione
    org_counts = df['tipo_missione'].value_counts()
    
    for org, color in ORG_COLORS.items():
        count = org_counts.get(org, 0)
        legend_html += f'''
        <p style="margin: 5px 0; display: flex; align-items: center;">
            <span style="color:{color}; font-size: 16px; margin-right: 8px;">●</span> 
            <span style="flex: 1;">{org}</span>
            <span style="font-weight: bold; color: #666;">{count}</span>
        </p>
        '''
    
    legend_html += '</div>'
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Mostra statistiche
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📍 Cluster Totali", len(df['tipo_missione'].unique()))
    with col2:
        st.metric("🎯 Missioni", len(df))
    with col3:
        st.metric("🌍 Paesi", len(df['paese'].unique()))
    
    # Mostra la mappa
    st_folium(m, width=900, height=400)
    
    # Informazioni aggiuntive
    st.info("""
    **📍 Mappa Cluster:**
    - Raggruppa missioni vicine per organizzazione
    - Layer control per attivare/disattivare organizzazioni
    - Zoom per vedere i dettagli di ogni missione
    - Clicca sui marker per informazioni complete
    """) 