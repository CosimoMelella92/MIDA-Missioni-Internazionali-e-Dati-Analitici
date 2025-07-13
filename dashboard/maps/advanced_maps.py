import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import numpy as np
from datetime import datetime
import folium
from folium import plugins
import json

def render_3d_world_map(df: pd.DataFrame):
    """Mappa 3D avanzata con PyDeck"""
    st.subheader("🌍 Mappa 3D delle Missioni Internazionali")
    
    # Prepara i dati
    if 'lat' not in df.columns or 'lon' not in df.columns:
        from .geocoding import add_coordinates_to_dataframe
        df = add_coordinates_to_dataframe(df)
    
    # Rimuovi righe senza coordinate
    df_map = df.dropna(subset=['lat', 'lon'])
    
    if len(df_map) == 0:
        st.warning("Nessuna missione con coordinate valide trovata")
        return
    
    # Colori per tipo di missione
    color_map = {
        'ONU': [255, 0, 0],      # Rosso
        'UE': [0, 255, 0],       # Verde
        'NATO': [0, 0, 255],     # Blu
        'ITA': [255, 255, 0],    # Giallo
        'Bilateral': [255, 0, 255], # Magenta
        'Multinational': [0, 255, 255] # Ciano
    }
    
    # Prepara i dati per PyDeck
    df_map['color'] = df_map['tipo_missione'].map(color_map).fillna([128, 128, 128])
    df_map['size'] = np.log(df_map['personale_totale'] + 1) * 1000
    
    # Crea la mappa 3D
    view_state = pdk.ViewState(
        longitude=df_map['lon'].mean(),
        latitude=df_map['lat'].mean(),
        zoom=2,
        pitch=45,
        bearing=0
    )
    
    # Layer per i punti delle missioni
    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=df_map,
        get_position=["lon", "lat"],
        get_color="color",
        get_radius="size",
        pickable=True,
        opacity=0.8,
        stroked=True,
        filled=True,
        radius_scale=6,
        radius_min_pixels=1,
        radius_max_pixels=100,
        line_width_min_pixels=1,
    )
    
    # Layer per il testo delle missioni
    text_layer = pdk.Layer(
        "TextLayer",
        data=df_map.head(20),  # Mostra solo le prime 20 per evitare sovrapposizioni
        get_position=["lon", "lat"],
        get_text="nome",
        get_color=[255, 255, 255],
        get_size=12,
        get_angle=0,
        text_anchor="middle",
        get_pixel_offset=[0, 0],
        pickable=True,
    )
    
    # Crea la mappa
    deck = pdk.Deck(
        layers=[scatter_layer, text_layer],
        initial_view_state=view_state,
        tooltip={
            "html": """
            <b>Missione:</b> {nome}<br/>
            <b>Paese:</b> {paese}<br/>
            <b>Tipo:</b> {tipo_missione}<br/>
            <b>Personale:</b> {personale_totale}<br/>
            <b>Costo:</b> €{costo_totale:,.0f}<br/>
            <b>Periodo:</b> {data_inizio} - {data_fine}
            """,
            "style": {
                "backgroundColor": "steelblue",
                "color": "white",
                "fontSize": "12px",
                "padding": "8px"
            }
        }
    )
    
    st.pydeck_chart(deck, use_container_width=True)
    
    # Statistiche della mappa
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Missioni mappate", len(df_map))
    with col2:
        st.metric("Paesi coinvolti", df_map['paese'].nunique())
    with col3:
        st.metric("Personale totale", f"{df_map['personale_totale'].sum():,.0f}")
    with col4:
        st.metric("Costo totale", f"€{df_map['costo_totale'].sum():,.0f}")

def render_plotly_world_map(df: pd.DataFrame):
    """Mappa mondiale interattiva con Plotly"""
    st.subheader("🗺️ Mappa Interattiva delle Missioni")
    
    # Prepara i dati
    if 'lat' not in df.columns or 'lon' not in df.columns:
        from .geocoding import add_coordinates_to_dataframe
        df = add_coordinates_to_dataframe(df)
    
    df_map = df.dropna(subset=['lat', 'lon'])
    
    if len(df_map) == 0:
        st.warning("Nessuna missione con coordinate valide trovata")
        return
    
    # Crea la mappa con Plotly
    fig = px.scatter_mapbox(
        df_map,
        lat='lat',
        lon='lon',
        color='tipo_missione',
        size='personale_totale',
        hover_name='nome',
        hover_data=['paese', 'personale_totale', 'costo_totale', 'data_inizio', 'data_fine'],
        zoom=2,
        title="Missioni Internazionali Italiane",
        color_discrete_map={
            'ONU': '#FF0000',
            'UE': '#00FF00', 
            'NATO': '#0000FF',
            'ITA': '#FFFF00',
            'Bilateral': '#FF00FF',
            'Multinational': '#00FFFF'
        }
    )
    
    fig.update_layout(
        mapbox_style="carto-positron",
        height=600,
        margin={"r":0,"t":30,"l":0,"b":0}
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Filtri interattivi
    st.subheader("🔍 Filtri Mappa")
    col1, col2 = st.columns(2)
    
    with col1:
        selected_types = st.multiselect(
            "Tipo di missione",
            options=df_map['tipo_missione'].unique(),
            default=df_map['tipo_missione'].unique()
        )
    
    with col2:
        min_personnel = st.slider(
            "Personale minimo",
            min_value=int(df_map['personale_totale'].min()),
            max_value=int(df_map['personale_totale'].max()),
            value=int(df_map['personale_totale'].min())
        )
    
    # Applica filtri
    df_filtered = df_map[
        (df_map['tipo_missione'].isin(selected_types)) &
        (df_map['personale_totale'] >= min_personnel)
    ]
    
    if len(df_filtered) > 0:
        st.success(f"Mostrando {len(df_filtered)} missioni filtrate")
    else:
        st.warning("Nessuna missione corrisponde ai filtri selezionati")

def render_cluster_map(df: pd.DataFrame):
    """Mappa con clustering per gestire molte missioni"""
    st.subheader("🔗 Mappa con Clustering")
    
    # Prepara i dati
    if 'lat' not in df.columns or 'lon' not in df.columns:
        from .geocoding import add_coordinates_to_dataframe
        df = add_coordinates_to_dataframe(df)
    
    df_map = df.dropna(subset=['lat', 'lon'])
    
    if len(df_map) == 0:
        st.warning("Nessuna missione con coordinate valide trovata")
        return
    
    # Crea mappa Folium con clustering
    m = folium.Map(
        location=[df_map['lat'].mean(), df_map['lon'].mean()],
        zoom_start=3,
        tiles='cartodbpositron'
    )
    
    # Aggiungi layer di clustering
    marker_cluster = plugins.MarkerCluster().add_to(m)
    
    # Colori per tipo di missione
    colors = {
        'ONU': 'red',
        'UE': 'green', 
        'NATO': 'blue',
        'ITA': 'orange',
        'Bilateral': 'purple',
        'Multinational': 'darkred'
    }
    
    # Aggiungi marker per ogni missione
    for idx, row in df_map.iterrows():
        color = colors.get(row['tipo_missione'], 'gray')
        
        # Crea popup con informazioni
        popup_html = f"""
        <div style="width: 200px;">
            <h4>{row['nome']}</h4>
            <p><b>Paese:</b> {row['paese']}</p>
            <p><b>Tipo:</b> {row['tipo_missione']}</p>
            <p><b>Personale:</b> {row['personale_totale']:,.0f}</p>
            <p><b>Costo:</b> €{row['costo_totale']:,.0f}</p>
            <p><b>Periodo:</b> {row['data_inizio'].strftime('%Y-%m-%d')} - {row['data_fine'].strftime('%Y-%m-%d')}</p>
        </div>
        """
        
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=folium.Popup(popup_html, max_width=300),
            icon=folium.Icon(color=color, icon='info-sign'),
            tooltip=row['nome']
        ).add_to(marker_cluster)
    
    # Aggiungi layer di controllo
    folium.LayerControl().add_to(m)
    
    # Mostra la mappa
    st.components.v1.html(m._repr_html_(), height=600)
    
    # Statistiche del clustering
    st.subheader("📊 Statistiche Clustering")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Missioni totali", len(df_map))
    with col2:
        st.metric("Paesi unici", df_map['paese'].nunique())
    with col3:
        st.metric("Tipi di missione", df_map['tipo_missione'].nunique())

def render_timeline_map(df: pd.DataFrame):
    """Mappa con timeline delle missioni"""
    st.subheader("⏰ Mappa Timeline delle Missioni")
    
    # Prepara i dati
    if 'lat' not in df.columns or 'lon' not in df.columns:
        from .geocoding import add_coordinates_to_dataframe
        df = add_coordinates_to_dataframe(df)
    
    df_map = df.dropna(subset=['lat', 'lon'])
    
    if len(df_map) == 0:
        st.warning("Nessuna missione con coordinate valide trovata")
        return
    
    # Slider per selezionare l'anno
    min_year = int(df_map['data_inizio'].dt.year.min())
    max_year = int(df_map['data_fine'].dt.year.max())
    
    selected_year = st.slider(
        "Seleziona anno",
        min_value=min_year,
        max_value=max_year,
        value=min_year,
        step=1
    )
    
    # Filtra missioni per l'anno selezionato
    df_year = df_map[
        (df_map['data_inizio'].dt.year <= selected_year) &
        (df_map['data_fine'].dt.year >= selected_year)
    ]
    
    if len(df_year) == 0:
        st.warning(f"Nessuna missione attiva nel {selected_year}")
        return
    
    # Crea mappa per l'anno selezionato
    fig = px.scatter_mapbox(
        df_year,
        lat='lat',
        lon='lon',
        color='tipo_missione',
        size='personale_totale',
        hover_name='nome',
        hover_data=['paese', 'personale_totale', 'data_inizio', 'data_fine'],
        zoom=2,
        title=f"Missioni attive nel {selected_year}",
        color_discrete_map={
            'ONU': '#FF0000',
            'UE': '#00FF00', 
            'NATO': '#0000FF',
            'ITA': '#FFFF00',
            'Bilateral': '#FF00FF',
            'Multinational': '#00FFFF'
        }
    )
    
    fig.update_layout(
        mapbox_style="carto-positron",
        height=500,
        margin={"r":0,"t":30,"l":0,"b":0}
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Statistiche per l'anno
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Missioni attive", len(df_year))
    with col2:
        st.metric("Personale totale", f"{df_year['personale_totale'].sum():,.0f}")
    with col3:
        st.metric("Paesi coinvolti", df_year['paese'].nunique())
    with col4:
        st.metric("Costo totale", f"€{df_year['costo_totale'].sum():,.0f}")

def render_heatmap(df: pd.DataFrame):
    """Mappa di calore delle missioni"""
    st.subheader("🔥 Mappa di Calore delle Missioni")
    
    # Prepara i dati
    if 'lat' not in df.columns or 'lon' not in df.columns:
        from .geocoding import add_coordinates_to_dataframe
        df = add_coordinates_to_dataframe(df)
    
    df_map = df.dropna(subset=['lat', 'lon'])
    
    if len(df_map) == 0:
        st.warning("Nessuna missione con coordinate valide trovata")
        return
    
    # Crea mappa di calore con Plotly
    fig = px.density_mapbox(
        df_map,
        lat='lat',
        lon='lon',
        z='personale_totale',
        radius=20,
        zoom=2,
        title="Densità di Personale per Regione",
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(
        mapbox_style="carto-positron",
        height=500,
        margin={"r":0,"t":30,"l":0,"b":0}
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Analisi delle zone più attive
    st.subheader("📊 Zone più attive")
    
    # Raggruppa per paese
    country_stats = df_map.groupby('paese').agg({
        'personale_totale': 'sum',
        'costo_totale': 'sum',
        'nome': 'count'
    }).reset_index()
    
    country_stats.columns = ['Paese', 'Personale Totale', 'Costo Totale', 'Numero Missioni']
    country_stats = country_stats.sort_values('Personale Totale', ascending=False)
    
    # Mostra top 10 paesi
    fig_bar = px.bar(
        country_stats.head(10),
        x='Paese',
        y='Personale Totale',
        title="Top 10 Paesi per Personale",
        color='Numero Missioni',
        color_continuous_scale='Viridis'
    )
    
    st.plotly_chart(fig_bar, use_container_width=True) 