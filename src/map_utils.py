"""
Modulo per le funzioni delle mappe interattive
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from folium.plugins import MarkerCluster, HeatMap
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
    """Crea una mappa del mondo SPETTACOLARE con Plotly"""
    try:
        # Unisci i dati delle missioni con le coordinate geografiche
        df_with_coords = df.merge(geo_df, on='paese', how='left')
        
        # Rimuovi righe senza coordinate
        df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
        
        if len(df_with_coords) == 0:
            # Fallback: crea una mappa vuota ma bella
            fig = go.Figure()
            fig.add_trace(go.Scattergeo())
            fig.update_layout(
                title="🌍 Mappa del Mondo - Missioni Internazionali",
                geo=dict(
                    scope='world',
                    showland=True,
                    landcolor='rgb(243, 243, 243)',
                    coastlinecolor='rgb(204, 204, 204)',
                    showocean=True,
                    oceancolor='rgb(230, 230, 250)',
                    showcountries=True,
                    countrycolor='rgb(255, 255, 255)',
                    showframe=False,
                    projection_type='natural earth'
                ),
                height=600,
                margin=dict(l=0, r=0, t=50, b=0)
            )
            return fig
        
        # Colori per organizzazione
        color_map = {
            'ONU': '#1f77b4',      # Blu
            'UE': '#ff7f0e',       # Arancione  
            'NATO': '#2ca02c',     # Verde
            'ITA': '#d62728'       # Rosso
        }
        
        # Crea la mappa
        fig = go.Figure()
        
        # Aggiungi i marker per ogni missione con stile migliorato
        for _, row in df_with_coords.iterrows():
            # Calcola dimensione marker basata sul personale (logaritmica per evitare marker troppo grandi)
            size = max(8, min(25, np.log(row['personale_totale'] + 1) * 3))
            
            # Colore basato sull'organizzazione
            color = color_map.get(row['tipo_missione'], '#9467bd')
            
            # Testo hover ricco
            hover_text = f"""
            <b>🎯 {row['nome']}</b><br>
            📍 Paese: {row['paese']}<br>
            🌍 Regione: {row['regione']}<br>
            👥 Personale: {row['personale_totale']:,.0f}<br>
            💰 Costo: €{row['costo_totale']:,.0f}<br>
            🏛️ Organizzazione: {row['tipo_missione']}<br>
            📅 Periodo: {row['data_inizio'].strftime('%Y')} - {row['data_fine'].strftime('%Y')}<br>
            🎖️ Tipo: {row['tipo_partecipazione']}
            """
            
            fig.add_trace(go.Scattergeo(
                lon=[row['lon']],
                lat=[row['lat']],
                mode='markers',
                name=row['nome'],
                text=hover_text,
                hovertemplate="%{text}<extra></extra>",
                marker=dict(
                    size=size,
                    color=color,
                    line=dict(width=2, color='white'),
                    opacity=0.8,
                    symbol='circle'
                ),
                showlegend=False
            ))
        
        # Aggiungi legenda per organizzazioni
        for org, color in color_map.items():
            fig.add_trace(go.Scattergeo(
                lon=[None],
                lat=[None],
                mode='markers',
                name=f'🏛️ {org}',
                marker=dict(size=10, color=color),
                showlegend=True
            ))
        
        fig.update_layout(
            title={
                'text': "🌍 Mappa del Mondo - Missioni Internazionali Italiane",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 20, 'color': '#2c3e50'}
            },
            geo=dict(
                scope='world',
                showland=True,
                landcolor='rgb(243, 243, 243)',
                coastlinecolor='rgb(204, 204, 204)',
                showocean=True,
                oceancolor='rgb(230, 230, 250)',
                showcountries=True,
                countrycolor='rgb(255, 255, 255)',
                showframe=False,
                projection_type='natural earth',
                center=dict(lat=20, lon=0),
                projection_scale=1.2
            ),
            height=700,
            margin=dict(l=0, r=0, t=80, b=0),
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor='rgba(255, 255, 255, 0.8)',
                bordercolor='rgba(0, 0, 0, 0.2)',
                borderwidth=1
            ),
            hovermode='closest'
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa del mondo: {e}")
        # Fallback
        fig = go.Figure()
        fig.add_trace(go.Scattergeo())
        fig.update_layout(
            title="🌍 Mappa del Mondo - Errore nel caricamento",
            geo=dict(scope='world')
        )
        return fig

def create_region_map_plotly(df: pd.DataFrame, geo_df: pd.DataFrame) -> go.Figure:
    """Crea una mappa regionale migliorata con Plotly"""
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
        
        # Colori per regioni
        region_colors = {
            'Africa': '#e74c3c',
            'Europa': '#3498db', 
            'Medio Oriente': '#f39c12',
            'Asia': '#9b59b6',
            'America': '#2ecc71'
        }
        
        fig = go.Figure()
        
        for _, row in region_stats.iterrows():
            color = region_colors.get(row['regione'], '#95a5a6')
            size = max(15, min(40, np.log(row['personale_totale'] + 1) * 5))
            
            hover_text = f"""
            <b>🌍 {row['regione']}</b><br>
            🎯 Missioni: {row['nome']}<br>
            👥 Personale: {row['personale_totale']:,.0f}<br>
            💰 Costo: €{row['costo_totale']:,.0f}<br>
            📍 Coordinate: {row['lat']:.2f}, {row['lon']:.2f}
            """
            
            fig.add_trace(go.Scattergeo(
                lon=[row['lon']],
                lat=[row['lat']],
                mode='markers',
                name=row['regione'],
                text=hover_text,
                hovertemplate="%{text}<extra></extra>",
                marker=dict(
                    size=size,
                    color=color,
                    line=dict(width=3, color='white'),
                    opacity=0.8,
                    symbol='diamond'
                )
            ))
        
        fig.update_layout(
            title={
                'text': "🌍 Mappa Regionale - Missioni per Regione",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'color': '#2c3e50'}
            },
            geo=dict(
                scope='world',
                showland=True,
                landcolor='rgb(243, 243, 243)',
                coastlinecolor='rgb(204, 204, 204)',
                showocean=True,
                oceancolor='rgb(230, 230, 250)',
                showcountries=True,
                countrycolor='rgb(255, 255, 255)',
                showframe=False,
                projection_type='natural earth'
            ),
            height=600,
            margin=dict(l=0, r=0, t=80, b=0),
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left", 
                x=0.01
            )
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa regionale: {e}")
        return create_world_map_plotly(df, geo_df)

def create_heatmap_plotly(df: pd.DataFrame, geo_df: pd.DataFrame) -> go.Figure:
    """Crea una mappa di calore SPETTACOLARE con Plotly"""
    try:
        # Unisci i dati
        df_with_coords = df.merge(geo_df, on='paese', how='left')
        df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
        
        if len(df_with_coords) == 0:
            return create_world_map_plotly(df, geo_df)
        
        # Crea la mappa di calore con stile migliorato
        fig = go.Figure()
        
        fig.add_trace(go.Densitymapbox(
            lat=df_with_coords['lat'],
            lon=df_with_coords['lon'],
            z=df_with_coords['personale_totale'],
            radius=40,
            colorscale=[
                [0, 'rgba(0, 0, 255, 0.1)'],
                [0.3, 'rgba(0, 255, 255, 0.3)'],
                [0.6, 'rgba(255, 255, 0, 0.5)'],
                [0.8, 'rgba(255, 165, 0, 0.7)'],
                [1, 'rgba(255, 0, 0, 0.9)']
            ],
            colorbar=dict(
                title="👥 Personale Totale",
                thickness=15,
                len=0.5,
                x=0.95
            ),
            hovertemplate="<b>Densità Personale</b><br>" +
                         "Personale: %{z:,.0f}<br>" +
                         "Coordinate: %{lat:.2f}, %{lon:.2f}<extra></extra>"
        ))
        
        fig.update_layout(
            title={
                'text': "🔥 Mappa di Calore - Densità Personale nelle Missioni",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'color': '#2c3e50'}
            },
            mapbox=dict(
                style="carto-positron",
                center=dict(lat=20, lon=0),
                zoom=1.5
            ),
            height=600,
            margin=dict(l=0, r=0, t=80, b=0)
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa di calore: {e}")
        return create_world_map_plotly(df, geo_df)

def create_folium_map(df: pd.DataFrame, geo_df: pd.DataFrame) -> Optional[folium.Map]:
    """Crea una mappa Folium SPETTACOLARE"""
    try:
        # Unisci i dati
        df_with_coords = df.merge(geo_df, on='paese', how='left')
        df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
        
        if len(df_with_coords) == 0:
            return None
        
        # Crea la mappa base con stile migliorato
        m = folium.Map(
            location=[20, 0],
            zoom_start=2,
            tiles='CartoDB positron',
            control_scale=True
        )
        
        # Colori per organizzazione
        color_map = {
            'ONU': 'blue',
            'UE': 'orange',
            'NATO': 'green', 
            'ITA': 'red'
        }
        
        # Crea cluster per organizzazione
        clusters = {}
        for org in color_map.keys():
            clusters[org] = MarkerCluster(
                name=f'🏛️ {org}',
                overlay=True,
                control=True
            ).add_to(m)
        
        # Aggiungi marker per ogni missione
        for _, row in df_with_coords.iterrows():
            org = row['tipo_missione']
            color = color_map.get(org, 'gray')
            
            # Popup HTML ricco e bello
            popup_html = f"""
            <div style="width: 300px; font-family: Arial, sans-serif;">
                <div style="background: linear-gradient(135deg, {color}, {color}dd); 
                            color: white; padding: 10px; border-radius: 5px 5px 0 0; 
                            margin: -10px -10px 10px -10px;">
                    <h3 style="margin: 0; font-size: 16px;">🎯 {row['nome']}</h3>
                </div>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr><td><strong>📍 Paese:</strong></td><td>{row['paese']}</td></tr>
                    <tr><td><strong>🌍 Regione:</strong></td><td>{row['regione']}</td></tr>
                    <tr><td><strong>👥 Personale:</strong></td><td>{row['personale_totale']:,.0f}</td></tr>
                    <tr><td><strong>💰 Costo:</strong></td><td>€{row['costo_totale']:,.0f}</td></tr>
                    <tr><td><strong>🏛️ Organizzazione:</strong></td><td>{row['tipo_missione']}</td></tr>
                    <tr><td><strong>📅 Periodo:</strong></td><td>{row['data_inizio'].strftime('%Y')} - {row['data_fine'].strftime('%Y')}</td></tr>
                    <tr><td><strong>🎖️ Tipo:</strong></td><td>{row['tipo_partecipazione']}</td></tr>
                </table>
            </div>
            """
            
            # Dimensione marker basata sul personale
            size = max(8, min(20, np.log(row['personale_totale'] + 1) * 2))
            
            folium.CircleMarker(
                location=[row['lat'], row['lon']],
                radius=size,
                popup=folium.Popup(popup_html, max_width=350),
                tooltip=f"🎯 {row['nome']} ({row['paese']})",
                color=color,
                fill=True,
                fillOpacity=0.7,
                weight=2
            ).add_to(clusters.get(org, m))
        
        # Aggiungi layer control
        folium.LayerControl(
            position='topright',
            collapsed=False
        ).add_to(m)
        
        return m
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa Folium: {e}")
        return None

def create_timeline_map(df: pd.DataFrame, geo_df: pd.DataFrame) -> go.Figure:
    """Crea una mappa timeline SPETTACOLARE con Plotly"""
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
            
            # Colori per organizzazione
            color_map = {
                'ONU': '#1f77b4',
                'UE': '#ff7f0e',
                'NATO': '#2ca02c',
                'ITA': '#d62728'
            }
            
            fig.add_trace(go.Scattergeo(
                lon=year_data['lon'],
                lat=year_data['lat'],
                mode='markers',
                name=str(year),
                text=year_data['nome'],
                hovertemplate="<b>%{text}</b><br>Anno: " + str(year) + "<extra></extra>",
                marker=dict(
                    size=np.log(year_data['personale_totale'] + 1) * 4,
                    color=[color_map.get(org, '#9467bd') for org in year_data['tipo_missione']],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Personale")
                ),
                visible=False
            ))
        
        # Rendi visibile solo il primo anno
        if len(fig.data) > 0:
            fig.data[0].visible = True
        
        # Aggiungi slider migliorato
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
            currentvalue={"prefix": "📅 Anno: "},
            pad={"t": 50},
            steps=steps,
            bgcolor='rgba(255, 255, 255, 0.8)',
            bordercolor='rgba(0, 0, 0, 0.2)',
            borderwidth=1
        )]
        
        fig.update_layout(
            title={
                'text': "⏰ Timeline Geografica - Evoluzione Missioni nel Tempo",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'color': '#2c3e50'}
            },
            geo=dict(
                scope='world',
                showland=True,
                landcolor='rgb(243, 243, 243)',
                coastlinecolor='rgb(204, 204, 204)',
                showocean=True,
                oceancolor='rgb(230, 230, 250)',
                showcountries=True,
                countrycolor='rgb(255, 255, 255)',
                showframe=False,
                projection_type='natural earth'
            ),
            sliders=sliders,
            height=700,
            margin=dict(l=0, r=0, t=80, b=80)
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa timeline: {e}")
        return create_world_map_plotly(df, geo_df)

def create_mission_clusters_map(df: pd.DataFrame, geo_df: pd.DataFrame) -> Optional[folium.Map]:
    """Crea una mappa con cluster SPETTACOLARE"""
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
            tiles='CartoDB positron',
            control_scale=True
        )
        
        # Crea cluster di marker
        marker_cluster = MarkerCluster(
            name='🎯 Tutte le Missioni',
            overlay=True,
            control=True,
            options={
                'spiderfyOnMaxZoom': True,
                'disableClusteringAtZoom': 7,
                'maxClusterRadius': 50
            }
        ).add_to(m)
        
        # Colori per organizzazione
        color_map = {
            'ONU': 'blue',
            'UE': 'orange',
            'NATO': 'green',
            'ITA': 'red'
        }
        
        # Aggiungi marker per ogni missione
        for _, row in df_with_coords.iterrows():
            org = row['tipo_missione']
            color = color_map.get(org, 'gray')
            
            # Popup HTML ricco
            popup_html = f"""
            <div style="width: 320px; font-family: Arial, sans-serif;">
                <div style="background: linear-gradient(135deg, {color}, {color}dd); 
                            color: white; padding: 12px; border-radius: 8px 8px 0 0; 
                            margin: -12px -12px 12px -12px; text-align: center;">
                    <h3 style="margin: 0; font-size: 18px;">🎯 {row['nome']}</h3>
                    <p style="margin: 5px 0 0 0; font-size: 14px;">🏛️ {row['tipo_missione']}</p>
                </div>
                <div style="padding: 10px;">
                    <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                        <tr style="border-bottom: 1px solid #eee;">
                            <td style="padding: 5px 0;"><strong>📍 Paese:</strong></td>
                            <td style="padding: 5px 0;">{row['paese']}</td>
                        </tr>
                        <tr style="border-bottom: 1px solid #eee;">
                            <td style="padding: 5px 0;"><strong>🌍 Regione:</strong></td>
                            <td style="padding: 5px 0;">{row['regione']}</td>
                        </tr>
                        <tr style="border-bottom: 1px solid #eee;">
                            <td style="padding: 5px 0;"><strong>👥 Personale:</strong></td>
                            <td style="padding: 5px 0;">{row['personale_totale']:,.0f}</td>
                        </tr>
                        <tr style="border-bottom: 1px solid #eee;">
                            <td style="padding: 5px 0;"><strong>💰 Costo:</strong></td>
                            <td style="padding: 5px 0;">€{row['costo_totale']:,.0f}</td>
                        </tr>
                        <tr style="border-bottom: 1px solid #eee;">
                            <td style="padding: 5px 0;"><strong>📅 Periodo:</strong></td>
                            <td style="padding: 5px 0;">{row['data_inizio'].strftime('%Y')} - {row['data_fine'].strftime('%Y')}</td>
                        </tr>
                        <tr>
                            <td style="padding: 5px 0;"><strong>🎖️ Tipo:</strong></td>
                            <td style="padding: 5px 0;">{row['tipo_partecipazione']}</td>
                        </tr>
                    </table>
                </div>
            </div>
            """
            
            # Dimensione marker basata sul personale
            size = max(6, min(18, np.log(row['personale_totale'] + 1) * 2))
            
            folium.CircleMarker(
                location=[row['lat'], row['lon']],
                radius=size,
                popup=folium.Popup(popup_html, max_width=350),
                tooltip=f"🎯 {row['nome']} ({row['paese']}) - {row['tipo_missione']}",
                color=color,
                fill=True,
                fillOpacity=0.7,
                weight=2
            ).add_to(marker_cluster)
        
        # Aggiungi layer control
        folium.LayerControl(
            position='topright',
            collapsed=False
        ).add_to(m)
        
        return m
        
    except Exception as e:
        logger.error(f"Errore nella creazione della mappa cluster: {e}")
        return None 