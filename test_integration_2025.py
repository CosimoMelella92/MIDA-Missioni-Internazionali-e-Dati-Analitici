#!/usr/bin/env python3
"""
Script per verificare l'integrazione delle missioni del 2025
"""

import pandas as pd
import sys
import os

# Aggiungi il percorso src al path
sys.path.append('src')

from missioni_dashboard import load_data

def test_integration_2025():
    """Testa l'integrazione delle missioni del 2025"""
    print("=== Test Integrazione Missioni 2025 ===")
    
    # Carica i dati
    df = load_data()
    print(f"📊 Missioni totali nella dashboard: {len(df)}")
    
    # Missioni attive nel 2025
    active_2025 = df[df['data_fine'] == pd.Timestamp('2025-12-31')]
    print(f"🟢 Missioni attive nel 2025: {len(active_2025)}")
    
    # Nuove missioni aggiunte
    new_missions_2025 = [
        'EUFOR ALTHEA',
        'MIADIT', 
        'EUNAVFOR ATALANTA',
        'Enhanced Vigilance Activities',
        'Forward Land Forces',
        'Sea Guardian',
        'Air Policing',
        'MPCC UE',
        'CRRTs UE'
    ]
    
    print("\n🔍 Verifica nuove missioni:")
    for mission in new_missions_2025:
        if mission in df['nome'].values:
            mission_data = df[df['nome'] == mission].iloc[0]
            print(f"✅ {mission} - {mission_data['paese']} - {mission_data['tipo_missione']}")
        else:
            print(f"❌ {mission} - NON TROVATA")
    
    # Distribuzione per organizzazione
    print("\n🏛️ Distribuzione per organizzazione:")
    org_counts = df['tipo_missione'].value_counts()
    for org, count in org_counts.head(10).items():
        print(f"   {org}: {count} missioni")
    
    # Missioni con dati aggiornati
    print("\n📈 Missioni con dati aggiornati:")
    kfor_data = df[df['nome'] == 'KFOR'].iloc[0]
    print(f"   KFOR: {kfor_data['personale_totale']} unità, €{kfor_data['costo_totale']:,}")
    
    print("\n=== Test Completato ===")

if __name__ == "__main__":
    test_integration_2025() 