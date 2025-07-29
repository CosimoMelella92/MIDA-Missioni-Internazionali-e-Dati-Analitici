import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd

# Colori per organizzazione (stessi delle altre mappe)
ORG_COLORS = {
    'ONU': '#1f77b4',      # Blu
    'UE': '#ff7f0e',       # Arancione
    'NATO': '#2ca02c',     # Verde
    'ITA': '#d62728',      # Rosso
    'Bilateral': '#ffd700', # Giallo
    'Multinational': '#9467bd', # Viola
}

def render_timeline_map(df: pd.DataFrame):
    """
    Visualizza una mappa con slider temporale per vedere l'evoluzione delle missioni per anno.
    df deve contenere colonne: 'lat', 'lon', 'nome', 'paese', 'tipo_missione', 'personale_totale', 'data_inizio', 'data_fine'
    """
    st.subheader('⏰ Timeline Geografica delle Missioni')
    
    # Calcola range anni
    years = list(range(df['data_inizio'].dt.year.min(), df['data_fine'].dt.year.max() + 1))
    selected_year = st.slider('Seleziona anno', min_value=years[0], max_value=years[-1], value=years[0])
    
    # Filtra missioni attive nell'anno selezionato
    df_active = df[(df['data_inizio'].dt.year <= selected_year) & (df['data_fine'].dt.year >= selected_year)]
    
    # Crea la mappa
    m = folium.Map(location=[30, 10], zoom_start=2, tiles='cartodbpositron')
    
    # Aggiungi marker per missioni attive
    for _, row in df_active.iterrows():
        color = ORG_COLORS.get(row['tipo_missione'], '#808080')
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
                f"<b>Anno:</b> {selected_year}<br>"
                f"<b>Personale:</b> {row['personale_totale']:,}",
                max_width=300
            )
        ).add_to(m)
    
    # Aggiungi leggenda
    legend_html = f'''
    <div style="position: fixed; 
                top: 20px; right: 20px; width: 250px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:12px; padding: 15px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
    <p style="margin: 0 0 10px 0; font-weight: bold; font-size: 14px; text-align: center;">⏰ Missioni Attive {selected_year}</p>
    '''
    
    # Conta missioni per organizzazione nell'anno selezionato
    org_counts = df_active['tipo_missione'].value_counts()
    
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
        st.metric("⏰ Anno Selezionato", selected_year)
    with col2:
        st.metric("🎯 Missioni Attive", len(df_active))
    with col3:
        st.metric("👥 Personale Totale", f"{df_active['personale_totale'].sum():,}")
    
    # Mostra la mappa
    st_folium(m, width=900, height=400)
    
    # Informazioni aggiuntive
    st.info(f"""
    **⏰ Timeline Interattiva:**
    - Mostra l'evoluzione delle missioni nel tempo
    - Anno selezionato: **{selected_year}**
    - Missioni attive: **{len(df_active)}**
    - Colori per organizzazione mantenuti nel tempo
    - Usa lo slider per navigare tra gli anni
    """) 