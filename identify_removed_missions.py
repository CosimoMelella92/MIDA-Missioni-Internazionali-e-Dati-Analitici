#!/usr/bin/env python3
"""
Script per identificare esattamente quali 6 missioni vengono rimosse dalla dashboard
"""

import pandas as pd
import re

def normalize_name(name):
    """Normalizza il nome come fa la dashboard"""
    return str(name).lower().replace(' ', '').replace('-', '').replace('_', '')

def identify_removed_missions():
    """Identifica le missioni rimosse dalla dashboard"""
    
    print("🔍 Identificazione missioni rimosse dalla dashboard")
    print("=" * 60)
    
    # 1. Carica il file principale
    df_main = pd.read_csv('data/processed/missioni_complete.csv')
    print(f"📊 File principale: {len(df_main)} missioni")
    
    # 2. Carica missions.xlsx
    missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
    print(f"📊 Excel missions.xlsx: {len(missions_df)} righe")
    
    # 3. Simula l'aggiunta di missioni (str.contains)
    new_missions = []
    for _, row in missions_df.iterrows():
        mission_name = str(row['mission']).strip()
        existing = df_main[df_main['nome'].str.contains(mission_name, case=False, na=False)]
        if len(existing) == 0:
            new_missions.append({
                'nome': mission_name,
                'paese': row.get('country', 'Non specificato'),
                'tipo_missione': row.get('organization', 'Non specificato')
            })
    
    print(f"📊 Missioni da aggiungere: {len(new_missions)}")
    
    # 4. Crea il dataset combinato (prima della deduplicazione finale)
    combined_data = []
    
    # Aggiungi missioni dal file principale
    for _, row in df_main.iterrows():
        combined_data.append({
            'nome': row['nome'],
            'paese': row['paese'],
            'tipo_missione': row['tipo_missione'],
            'fonte': 'file_principale'
        })
    
    # Aggiungi nuove missioni
    for mission in new_missions:
        combined_data.append({
            'nome': mission['nome'],
            'paese': mission['paese'],
            'tipo_missione': mission['tipo_missione'],
            'fonte': 'excel_aggiunto'
        })
    
    df_combined = pd.DataFrame(combined_data)
    print(f"📊 Totale dopo aggiunta: {len(df_combined)} missioni")
    
    # 5. Simula la deduplicazione finale della dashboard
    df_combined['nome_normalizzato'] = df_combined['nome'].apply(normalize_name)
    df_combined['paese_normalizzato'] = df_combined['paese'].str.lower()
    
    # Trova duplicati basati su nome normalizzato e paese
    duplicates = df_combined[df_combined.duplicated(subset=['nome_normalizzato', 'paese_normalizzato'], keep=False)]
    
    if len(duplicates) > 0:
        print(f"\n🔍 Duplicati trovati ({len(duplicates)} righe):")
        for _, group in duplicates.groupby(['nome_normalizzato', 'paese_normalizzato']):
            print(f"\n📋 Gruppo duplicato:")
            for _, row in group.iterrows():
                print(f"   - {row['nome']} ({row['paese']}) - {row['tipo_missione']} - Fonte: {row['fonte']}")
    
    # 6. Rimuovi duplicati (mantieni il primo)
    df_final = df_combined.drop_duplicates(subset=['nome_normalizzato', 'paese_normalizzato'], keep='first')
    print(f"\n📊 Dopo deduplicazione: {len(df_final)} missioni")
    
    # 7. Identifica le missioni rimosse
    removed_count = len(df_combined) - len(df_final)
    print(f"📊 Missioni rimosse: {removed_count}")
    
    if removed_count > 0:
        print(f"\n🗑️ Missioni rimosse durante deduplicazione:")
        removed_missions = []
        
        for _, group in duplicates.groupby(['nome_normalizzato', 'paese_normalizzato']):
            if len(group) > 1:
                # Prendi tutte tranne la prima (che viene mantenuta)
                removed = group.iloc[1:]
                for _, row in removed.iterrows():
                    removed_missions.append({
                        'nome': row['nome'],
                        'paese': row['paese'],
                        'tipo_missione': row['tipo_missione'],
                        'fonte': row['fonte']
                    })
        
        for i, mission in enumerate(removed_missions, 1):
            print(f"   {i}. {mission['nome']} ({mission['paese']}) - {mission['tipo_missione']} - Fonte: {mission['fonte']}")
    
    # 8. Verifica con la dashboard reale
    from dashboard.missioni_dashboard import load_data
    df_dashboard = load_data()
    print(f"\n📊 Dashboard reale: {len(df_dashboard)} missioni")
    
    # 9. Confronto
    expected_after_dedup = len(df_main) + len(new_missions) - removed_count
    print(f"📊 Atteso dopo deduplicazione: {expected_after_dedup}")
    print(f"📊 Dashboard reale: {len(df_dashboard)}")
    
    if expected_after_dedup == len(df_dashboard):
        print("✅ I numeri corrispondono!")
    else:
        print(f"❌ Differenza: {expected_after_dedup - len(df_dashboard)}")
    
    return removed_missions

if __name__ == "__main__":
    removed = identify_removed_missions() 