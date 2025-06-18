"""
Modulo per le funzioni delle mappe interattive
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from folium.plugins import MarkerCluster
import geopandas as gpd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import streamlit as st
import numpy as np
from typing import Dict, List, Optional, Any
import logging

# Configura logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_geo_data() -> pd.DataFrame:
    """Carica i dati geografici dei paesi"""
    # Dati geografici predefiniti per i paesi nelle missioni
    geo_data = {
        'paese': [
            'Libano', 'Mali', 'Kosovo', 'Sahara Occidentale', 'Libia', 'Niger', 
            'Afghanistan', 'Repubblica Centrafricana', 'Sudan del Sud', 'Somalia',
            'Bosnia ed Erzegovina', 'Iraq', 'Kuwait', 'Haiti', 'Timor Est', 
            'Etiopia', 'Eritrea', 'Liberia', 'Repubblica Democratica del Congo',
            'Costa d\'Avorio', 'Sudan', 'Darfur', 'Mediterraneo', 'Abyei'
        ],
        'lat': [
            33.8547, 17.5707, 42.6026, 24.2155, 26.3351, 17.6078,
            33.9391, 6.6111, 6.8770, 5.1521, 43.9159, 33.2232, 29.3117,
            18.9712, -8.8742, 9.1450, 15.1794, 6.4281, -4.0383,
            7.5400, 12.8628, 13.4433, 35.0000, 9.5000
        ],
        'lon': [
            35.8623, -3.9962, 20.9030, -13.2355, 17.2283, 8.0817,
            67.7100, 20.9394, 31.3070, 46.1996, 17.6791, 43.6793, 47.4818,
            -72.2852, 125.7275, 40.4897, 39.7823, -9.4295, 21.7587,
            -5.5471, 30.2176, 25.3500, 18.0000, 28.5000
        ]
    }
    
    return pd.DataFrame(geo_data)

def create_world_map_plotly(df: pd.DataFrame, geo_df: pd.DataFrame) -> go.Figure:
    """Crea una mappa del mondo con Plotly"""
    try:
        # Unisci i dati delle missioni con le coordinate geografiche
        df_with_coords = df.merge(geo_df, on='paese', how='left')
        
        # Rimuovi righe senza coordinate
        df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
        
        if len(df_with_coords) == 0:
            # Fallback: crea una mappa vuota
            fig = go.Figure()
            fig.add_trace(go.Scattergeo())
            fig.update_layout(
                title="Mappa del Mondo - Missioni Internazionali",
                geo=dict(
                    scope='world',
                    showland=True,
                    landcolor='rgb(243, 243, 243)',
                    coastlinecolor='rgb(204, 204, 204)',
                )
            )
            return fig
        
        # Crea la mappa
        fig = go.Figure()
        
        # Aggiungi i marker per ogni missione
        for _, row in df_with_coords.iterrows():
            fig.add_trace(go.Scattergeo(
                lon=[row['lon']],
                lat=[row['lat']],
                mode='markers',
                name=row['nome'],
                text=f"{row['nome']}<br>Paese: {row['paese']}<br>Personale: {row['personale_totale']}<br>Tipo: {row['tipo_partecipazione']}",
                hovertemplate="<b>%{text}</b><extra></extra>",
                marker=dict(
                    size=np.log(row['personale_totale'] + 1) * 5,
                    color=row['personale_totale'],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Personale")
                )
            ))
        
        fig.update_layout(
            title="Mappa del Mondo - Missioni Internazionali",
            geo=dict(
                scope='world',
                showland=True,
                landcolor='rgb(243, 243, 243)',
                coastlinecolor='rgb(204, 204, 204)',
                projection_type='equirectangular'
            ),
            height=600
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa del mondo: {e}")
        # Fallback
        fig = go.Figure()
        fig.add_trace(go.Scattergeo())
        fig.update_layout(
            title="Mappa del Mondo - Errore nel caricamento",
            geo=dict(scope='world')
        )
        return fig

def create_region_map_plotly(df: pd.DataFrame, geo_df: pd.DataFrame) -> go.Figure:
    """Crea una mappa regionale con Plotly"""
    try:
        # Unisci i dati
        df_with_coords = df.merge(geo_df, on='paese', how='left')
        df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
        
        if len(df_with_coords) == 0:
            return create_world_map_plotly(df, geo_df)
        
        # Raggruppa per regione
        region_stats = df_with_coords.groupby('regione').agg({
            'nome': 'count',
            'personale_totale': 'sum',
            'costo_totale': 'sum',
            'lat': 'mean',
            'lon': 'mean'
        }).reset_index()
        
        fig = go.Figure()
        
        for _, row in region_stats.iterrows():
            fig.add_trace(go.Scattergeo(
                lon=[row['lon']],
                lat=[row['lat']],
                mode='markers',
                name=row['regione'],
                text=f"{row['regione']}<br>Missioni: {row['nome']}<br>Personale: {row['personale_totale']:,.0f}",
                hovertemplate="<b>%{text}</b><extra></extra>",
                marker=dict(
                    size=np.log(row['personale_totale'] + 1) * 8,
                    color=row['nome'],
                    colorscale='Plasma',
                    showscale=True,
                    colorbar=dict(title="Numero Missioni")
                )
            ))
        
        fig.update_layout(
            title="Mappa Regionale - Missioni per Regione",
            geo=dict(
                scope='world',
                showland=True,
                landcolor='rgb(243, 243, 243)',
                coastlinecolor='rgb(204, 204, 204)'
            ),
            height=600
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa regionale: {e}")
        return create_world_map_plotly(df, geo_df)

def create_heatmap_plotly(df: pd.DataFrame, geo_df: pd.DataFrame) -> go.Figure:
    """Crea una mappa di calore con Plotly"""
    try:
        # Unisci i dati
        df_with_coords = df.merge(geo_df, on='paese', how='left')
        df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
        
        if len(df_with_coords) == 0:
            return create_world_map_plotly(df, geo_df)
        
        # Crea la mappa di calore
        fig = go.Figure()
        
        fig.add_trace(go.Densitymapbox(
            lat=df_with_coords['lat'],
            lon=df_with_coords['lon'],
            z=df_with_coords['personale_totale'],
            radius=30,
            colorscale='Viridis',
            colorbar=dict(title="Personale Totale")
        ))
        
        fig.update_layout(
            title="Mappa di Calore - Densità Personale",
            mapbox=dict(
                style="carto-positron",
                center=dict(lat=20, lon=0),
                zoom=1
            ),
            height=600
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa di calore: {e}")
        return create_world_map_plotly(df, geo_df)

def create_folium_map(df: pd.DataFrame, geo_df: pd.DataFrame) -> Optional[folium.Map]:
    """Crea una mappa interattiva con Folium"""
    try:
        # Unisci i dati
        df_with_coords = df.merge(geo_df, on='paese', how='left')
        df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
        
        if len(df_with_coords) == 0:
            return None
        
        # Crea la mappa base
        m = folium.Map(
            location=[20, 0],
            zoom_start=2,
            tiles='CartoDB positron'
        )
        
        # Aggiungi marker per ogni missione
        for _, row in df_with_coords.iterrows():
            popup_text = f"""
            <b>{row['nome']}</b><br>
            Paese: {row['paese']}<br>
            Regione: {row['regione']}<br>
            Personale: {row['personale_totale']:,.0f}<br>
            Tipo: {row['tipo_partecipazione']}<br>
            Costo: €{row['costo_totale']:,.0f}
            """
            
            folium.Marker(
                location=[row['lat'], row['lon']],
                popup=folium.Popup(popup_text, max_width=300),
                tooltip=row['nome'],
                icon=folium.Icon(color='red', icon='info-sign')
            ).add_to(m)
        
        return m
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa Folium: {e}")
        return None

def create_timeline_map(df: pd.DataFrame, geo_df: pd.DataFrame) -> go.Figure:
    """Crea una mappa timeline con Plotly"""
    try:
        # Unisci i dati
        df_with_coords = df.merge(geo_df, on='paese', how='left')
        df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
        
        if len(df_with_coords) == 0:
            return create_world_map_plotly(df, geo_df)
        
        # Crea la mappa timeline
        fig = go.Figure()
        
        # Aggiungi slider per gli anni
        years = sorted(df_with_coords['data_inizio'].dt.year.unique())
        
        for year in years:
            year_data = df_with_coords[df_with_coords['data_inizio'].dt.year == year]
            
            fig.add_trace(go.Scattergeo(
                lon=year_data['lon'],
                lat=year_data['lat'],
                mode='markers',
                name=str(year),
                text=year_data['nome'],
                hovertemplate="<b>%{text}</b><br>Anno: " + str(year) + "<extra></extra>",
                marker=dict(
                    size=np.log(year_data['personale_totale'] + 1) * 5,
                    color=year_data['personale_totale'],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Personale")
                ),
                visible=False
            ))
        
        # Rendi visibile solo il primo anno
        if len(fig.data) > 0:
            fig.data[0].visible = True
        
        # Aggiungi slider
        steps = []
        for i, year in enumerate(years):
            step = dict(
                method="update",
                args=[{"visible": [False] * len(fig.data)}],
                label=str(year)
            )
            step["args"][0]["visible"][i] = True
            steps.append(step)
        
        sliders = [dict(
            active=0,
            currentvalue={"prefix": "Anno: "},
            pad={"t": 50},
            steps=steps
        )]
        
        fig.update_layout(
            title="Timeline Geografica - Evoluzione Missioni",
            geo=dict(
                scope='world',
                showland=True,
                landcolor='rgb(243, 243, 243)',
                coastlinecolor='rgb(204, 204, 204)'
            ),
            sliders=sliders,
            height=600
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa timeline: {e}")
        return create_world_map_plotly(df, geo_df)

def create_mission_clusters_map(df: pd.DataFrame, geo_df: pd.DataFrame) -> Optional[folium.Map]:
    """Crea una mappa con cluster di missioni"""
    try:
        # Unisci i dati
        df_with_coords = df.merge(geo_df, on='paese', how='left')
        df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
        
        if len(df_with_coords) == 0:
            return None
        
        # Crea la mappa base
        m = folium.Map(
            location=[20, 0],
            zoom_start=2,
            tiles='CartoDB positron'
        )
        
        # Crea cluster di marker
        marker_cluster = MarkerCluster().add_to(m)
        
        # Aggiungi marker per ogni missione
        for _, row in df_with_coords.iterrows():
            popup_text = f"""
            <b>{row['nome']}</b><br>
            Paese: {row['paese']}<br>
            Regione: {row['regione']}<br>
            Personale: {row['personale_totale']:,.0f}<br>
            Tipo: {row['tipo_partecipazione']}<br>
            Costo: €{row['costo_totale']:,.0f}<br>
            Periodo: {row['data_inizio'].strftime('%Y')} - {row['data_fine'].strftime('%Y')}
            """
            
            # Colore basato sul tipo di partecipazione
            color_map = {'mil': 'red', 'civ': 'blue', 'civmil': 'green'}
            icon_color = color_map.get(row['tipo_partecipazione'], 'gray')
            
            folium.Marker(
                location=[row['lat'], row['lon']],
                popup=folium.Popup(popup_text, max_width=300),
                tooltip=f"{row['nome']} ({row['paese']})",
                icon=folium.Icon(color=icon_color, icon='info-sign')
            ).add_to(marker_cluster)
        
        return m
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa cluster: {e}")
        return None 