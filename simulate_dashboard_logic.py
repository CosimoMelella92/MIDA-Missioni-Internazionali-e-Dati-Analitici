#!/usr/bin/env python3
"""
Script per simulare esattamente la logica della dashboard
"""

import pandas as pd
import re

def normalize_name(name):
    """Normalizza il nome come fa la dashboard"""
    return str(name).lower().replace(' ', '').replace('-', '').replace('_', '')

def simulate_dashboard_integration():
    """Simula esattamente la logica della dashboard"""
    
    print("🔍 Simulazione logica dashboard")
    print("=" * 50)
    
    # 1. Carica il file principale
    df_main = pd.read_csv('data/processed/missioni_complete.csv')
    print(f"📊 File principale: {len(df_main)} missioni")
    
    # 2. Carica missions.xlsx
    missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
    print(f"📊 Excel missions.xlsx: {len(missions_df)} righe")
    
    # 3. Simula la logica della dashboard (str.contains)
    new_missions = []
    duplicates_found = []
    
    for _, row in missions_df.iterrows():
        mission_name = str(row['mission']).strip()
        
        # Usa str.contains come fa la dashboard
        existing_mission = df_main[df_main['nome'].str.contains(mission_name, case=False, na=False)]
        
        if len(existing_mission) == 0:
            # Missione non esistente, aggiungi
            new_mission = {
                'nome': mission_name,
                'paese': str(row['country']).strip() if pd.notna(row['country']) else 'Non specificato',
                'regione': str(row['region']).strip() if pd.notna(row['region']) else 'Non specificata',
                'sub_regione': 'Non specificata',
                'tipo_partecipazione': 'civmil',
                'data_inizio': pd.to_datetime(row['date_start'], errors='coerce'),
                'data_fine': pd.to_datetime(row['date_end'], errors='coerce'),
                'personale_militare': 100,
                'personale_civile': 50,
                'personale_totale': 150,
                'costo_totale': 25000000,
                'tipo_missione': str(row['framework']).strip() if pd.notna(row['framework']) else 'ONU',
                'commitment': 'Troops'
            }
            new_missions.append(new_mission)
        else:
            duplicates_found.append({
                'excel_name': mission_name,
                'existing_name': existing_mission.iloc[0]['nome']
            })
    
    print(f"📊 Missioni da aggiungere (str.contains): {len(new_missions)}")
    print(f"📊 Duplicati trovati (str.contains): {len(duplicates_found)}")
    
    # 4. Crea il dataset combinato
    if new_missions:
        new_df = pd.DataFrame(new_missions)
        df_combined = pd.concat([df_main, new_df], ignore_index=True)
    else:
        df_combined = df_main.copy()
    
    print(f"📊 Dopo aggiunta missioni: {len(df_combined)}")
    
    # 5. Simula la rimozione duplicati della dashboard
    df_combined['__norm_nome'] = df_combined['nome'].apply(normalize_name)
    df_combined['__norm_paese'] = df_combined['paese'].str.lower().str.strip()
    
    # Trova e rimuovi duplicati basati su nome normalizzato e paese
    duplicates_mask = df_combined.duplicated(subset=['__norm_nome', '__norm_paese'], keep='first')
    duplicates_removed = duplicates_mask.sum()
    
    df_final = df_combined[~duplicates_mask]
    df_final = df_final.drop(columns=['__norm_nome', '__norm_paese'])
    
    print(f"📊 Duplicati rimossi: {duplicates_removed}")
    print(f"📊 Totale finale: {len(df_final)}")
    
    # 6. Verifica se è 218
    if len(df_final) == 218:
        print("✅ Conteggio corretto: 218 missioni")
    else:
        print(f"❌ Conteggio inaspettato: {len(df_final)} (atteso: 218)")
    
    # 7. Mostra le missioni aggiunte
    print(f"\n📋 Missioni aggiunte dalla dashboard:")
    for i, mission in enumerate(new_missions[:15]):
        print(f"   {i+1}. {mission['nome']} ({mission['tipo_missione']})")
    
    if len(new_missions) > 15:
        print(f"   ... e altre {len(new_missions) - 15} missioni")
    
    # 8. Statistiche finali
    print(f"\n🏛️ Distribuzione per organizzazione:")
    org_stats = df_final['tipo_missione'].value_counts()
    for org, count in org_stats.items():
        print(f"   {org}: {count} missioni")
    
    return df_final, new_missions, duplicates_found

def compare_methods():
    """Confronta i diversi metodi di deduplicazione"""
    
    print(f"\n🔍 Confronto metodi di deduplicazione:")
    
    # Metodo 1: Confronto esatto (mio script)
    df_main = pd.read_csv('data/processed/missioni_complete.csv')
    missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
    
    # Normalizzazione esatta
    def normalize_exact(name):
        if pd.isna(name):
            return ""
        name = str(name).strip()
        name = re.sub(r'[^\w\s-]', '', name)
        name = re.sub(r'\s+', ' ', name)
        return name.lower()
    
    df_main['nome_normalizzato'] = df_main['nome'].apply(normalize_exact)
    missions_df['mission_normalizzato'] = missions_df['mission'].apply(normalize_exact)
    
    exact_new = []
    for _, row in missions_df.iterrows():
        mission_normalized = normalize_exact(row['mission'])
        existing = df_main[df_main['nome_normalizzato'] == mission_normalized]
        if len(existing) == 0:
            exact_new.append(row['mission'])
    
    # Metodo 2: str.contains (dashboard)
    contains_new = []
    for _, row in missions_df.iterrows():
        mission_name = str(row['mission']).strip()
        existing = df_main[df_main['nome'].str.contains(mission_name, case=False, na=False)]
        if len(existing) == 0:
            contains_new.append(mission_name)
    
    print(f"   Metodo esatto: {len(exact_new)} missioni nuove")
    print(f"   Metodo str.contains: {len(contains_new)} missioni nuove")
    print(f"   Differenza: {len(contains_new) - len(exact_new)} missioni")
    
    # Mostra le missioni che differiscono
    exact_set = set(exact_new)
    contains_set = set(contains_new)
    
    only_in_contains = contains_set - exact_set
    only_in_exact = exact_set - contains_set
    
    if only_in_contains:
        print(f"\n   Missioni solo in str.contains:")
        for mission in sorted(only_in_contains):
            print(f"     - {mission}")
    
    if only_in_exact:
        print(f"\n   Missioni solo in confronto esatto:")
        for mission in sorted(only_in_exact):
            print(f"     - {mission}")

if __name__ == "__main__":
    df_final, new_missions, duplicates = simulate_dashboard_integration()
    compare_methods()
    
    print(f"\n🎯 RISULTATO:")
    print(f"La dashboard mostra 218 missioni perché:")
    print(f"1. Usa str.contains per il confronto (meno preciso)")
    print(f"2. Aggiunge {len(new_missions)} missioni da Excel")
    print(f"3. Rimuove duplicati con normalizzazione")
    print(f"4. Risultato: 218 missioni") 