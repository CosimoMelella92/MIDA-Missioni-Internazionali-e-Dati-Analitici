#!/usr/bin/env python3
"""
Script per analizzare tutti i file Excel e identificare da dove viene il conteggio di 218 missioni
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os

def analyze_mission_count():
    """Analizza tutti i file per identificare il conteggio di 218 missioni"""
    
    print("🔍 Analisi completa per identificare il conteggio di 218 missioni")
    print("=" * 70)
    
    # Lista di tutti i file da analizzare
    files_to_check = [
        ('data/processed/missioni_complete.csv', 'CSV principale'),
        ('data/processed/missioni_complete_updated.csv', 'CSV aggiornato'),
        ('data/raw/Excel/missions.xlsx', 'Excel missions.xlsx'),
        ('data/raw/Excel/missions_expenditure_Italy.xlsx', 'Excel expenditure'),
        ('data/raw/Excel/Matrice dati.xlsx', 'Excel Matrice dati'),
        ('data/raw/Excel/f319e1c8-5654-4cd6-b4c7-5722ae437d30-Ukraine_Support_Tracker_Release_21.xlsx', 'Excel Ukraine Support'),
        ('data/raw/Matrice dati 1AGG.xlsx', 'Excel Matrice dati 1AGG'),
        ('data/processed/Matrice dati 1AGG_enriched.xlsx', 'Excel Matrice dati enriched'),
        ('data/final/missioni_internazionali_20250609.csv', 'CSV finale'),
        ('data/final/missioni_internazionali_20250609.xlsx', 'Excel finale')
    ]
    
    all_missions = []
    file_stats = []
    
    for file_path, description in files_to_check:
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path)
            else:
                continue
                
            print(f"\n📁 {description}: {len(df)} righe")
            file_stats.append((description, len(df)))
            
            # Mostra le prime colonne per capire la struttura
            print(f"   Colonne: {list(df.columns[:5])}...")
            
            # Se ha una colonna 'nome' o simile, mostra alcune missioni
            name_columns = [col for col in df.columns if 'nome' in col.lower() or 'mission' in col.lower() or 'name' in col.lower()]
            if name_columns:
                name_col = name_columns[0]
                print(f"   Colonna nome: {name_col}")
                unique_names = df[name_col].dropna().unique()
                print(f"   Missioni uniche: {len(unique_names)}")
                if len(unique_names) <= 10:
                    print(f"   Esempi: {list(unique_names)}")
                else:
                    print(f"   Prime 5: {list(unique_names[:5])}")
                
                # Aggiungi alla lista totale
                for name in unique_names:
                    if pd.notna(name) and str(name).strip():
                        all_missions.append((str(name).strip(), description))
            
        except Exception as e:
            print(f"❌ Errore nel leggere {description}: {e}")
    
    # Analizza le missioni totali
    print(f"\n📊 ANALISI TOTALE:")
    print(f"Missioni trovate in tutti i file: {len(all_missions)}")
    
    # Raggruppa per nome per trovare duplicati
    mission_names = [name for name, source in all_missions]
    unique_missions = set(mission_names)
    print(f"Missioni uniche (dopo rimozione duplicati): {len(unique_missions)}")
    
    # Trova duplicati
    from collections import Counter
    name_counts = Counter(mission_names)
    duplicates = {name: count for name, count in name_counts.items() if count > 1}
    
    if duplicates:
        print(f"\n🔄 DUPLICATI TROVATI ({len(duplicates)} missioni duplicate):")
        for name, count in sorted(duplicates.items()):
            sources = [source for m_name, source in all_missions if m_name == name]
            print(f"   {name} ({count} volte): {', '.join(sources)}")
    
    # Statistiche per file
    print(f"\n📈 STATISTICHE PER FILE:")
    for description, count in sorted(file_stats, key=lambda x: x[1], reverse=True):
        print(f"   {description}: {count} righe")
    
    # Verifica se il totale è 218
    if len(unique_missions) == 218:
        print(f"\n🎯 TROVATO! Il conteggio di 218 missioni viene da {len(unique_missions)} missioni uniche")
    elif len(unique_missions) == 208:
        print(f"\n✅ Il conteggio corretto è 208 missioni uniche")
    else:
        print(f"\n❓ Conteggio inaspettato: {len(unique_missions)} missioni uniche")
    
    return len(unique_missions), duplicates

def check_dashboard_integration():
    """Verifica come la dashboard integra i dati"""
    print(f"\n🔍 VERIFICA INTEGRAZIONE DASHBOARD:")
    
    try:
        # Simula il processo di integrazione della dashboard
        df_existing = pd.read_csv('data/processed/missioni_complete.csv')
        print(f"   CSV principale: {len(df_existing)} missioni")
        
        missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
        print(f"   Excel missions.xlsx: {len(missions_df)} righe")
        
        # Conta missioni non duplicate
        new_missions = []
        for _, row in missions_df.iterrows():
            mission_name = str(row['mission']).strip()
            existing_mission = df_existing[df_existing['nome'].str.contains(mission_name, case=False, na=False)]
            
            if len(existing_mission) == 0:
                new_missions.append(mission_name)
        
        print(f"   Missioni da aggiungere (non duplicate): {len(new_missions)}")
        print(f"   Totale dopo integrazione: {len(df_existing) + len(new_missions)}")
        
        if len(df_existing) + len(new_missions) == 218:
            print(f"   🎯 TROVATO! La dashboard integra {len(new_missions)} nuove missioni per arrivare a 218")
        elif len(df_existing) + len(new_missions) == 208:
            print(f"   ✅ La dashboard arriva correttamente a 208 missioni")
        
    except Exception as e:
        print(f"   ❌ Errore nella verifica dashboard: {e}")

if __name__ == "__main__":
    total_missions, duplicates = analyze_mission_count()
    check_dashboard_integration()
    
    print(f"\n🎯 RISULTATO FINALE:")
    print(f"Missioni uniche trovate: {total_missions}")
    if total_missions == 218:
        print("Il conteggio di 218 missioni è corretto e viene da tutti i file Excel integrati")
    elif total_missions == 208:
        print("Il conteggio corretto è 208 missioni, il 218 potrebbe essere un errore")
    else:
        print(f"Conteggio inaspettato: {total_missions}") 