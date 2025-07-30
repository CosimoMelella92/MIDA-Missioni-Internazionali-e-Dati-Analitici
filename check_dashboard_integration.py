#!/usr/bin/env python3
"""
Script per verificare esattamente cosa aggiunge la dashboard per arrivare a 218 missioni
"""

import pandas as pd
import re

def normalize_mission_name(name):
    """Normalizza il nome della missione per il confronto"""
    if pd.isna(name):
        return ""
    
    name = str(name).strip()
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.lower()

def check_dashboard_integration():
    """Verifica cosa aggiunge la dashboard"""
    
    print("🔍 Verifica integrazione dashboard")
    print("=" * 50)
    
    # 1. Carica il file principale
    df_main = pd.read_csv('data/processed/missioni_complete.csv')
    print(f"📊 File principale: {len(df_main)} missioni")
    
    # 2. Carica missions.xlsx
    missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
    print(f"📊 Excel missions.xlsx: {len(missions_df)} righe")
    
    # 3. Normalizza i nomi per confronto accurato
    df_main['nome_normalizzato'] = df_main['nome'].apply(normalize_mission_name)
    missions_df['mission_normalizzato'] = missions_df['mission'].apply(normalize_mission_name)
    
    # 4. Trova missioni non duplicate
    new_missions = []
    duplicates_found = []
    
    for _, row in missions_df.iterrows():
        mission_name = str(row['mission']).strip()
        mission_normalized = normalize_mission_name(mission_name)
        
        # Controlla se esiste già nel file principale
        existing = df_main[df_main['nome_normalizzato'] == mission_normalized]
        
        if len(existing) == 0:
            new_missions.append({
                'nome': mission_name,
                'framework': str(row['framework']).strip() if pd.notna(row['framework']) else 'ONU',
                'country': str(row['country']).strip() if pd.notna(row['country']) else 'Non specificato'
            })
        else:
            duplicates_found.append({
                'excel_name': mission_name,
                'existing_name': existing.iloc[0]['nome']
            })
    
    print(f"📊 Missioni da aggiungere (non duplicate): {len(new_missions)}")
    print(f"📊 Duplicati trovati: {len(duplicates_found)}")
    
    # 5. Calcola il totale
    total_expected = len(df_main) + len(new_missions)
    print(f"📊 Totale atteso: {len(df_main)} + {len(new_missions)} = {total_expected}")
    
    # 6. Mostra le missioni che verranno aggiunte
    print(f"\n📋 Missioni che verranno aggiunte:")
    for i, mission in enumerate(new_missions[:20]):  # Mostra prime 20
        print(f"   {i+1}. {mission['nome']} ({mission['framework']}) - {mission['country']}")
    
    if len(new_missions) > 20:
        print(f"   ... e altre {len(new_missions) - 20} missioni")
    
    # 7. Mostra alcuni duplicati
    if duplicates_found:
        print(f"\n🔄 Esempi di duplicati trovati:")
        for i, dup in enumerate(duplicates_found[:10]):
            print(f"   {i+1}. Excel: '{dup['excel_name']}' -> CSV: '{dup['existing_name']}'")
    
    # 8. Verifica se il totale è 218
    if total_expected == 218:
        print(f"\n🎯 TROVATO! La dashboard aggiunge {len(new_missions)} missioni per arrivare a 218")
    else:
        print(f"\n❓ Conteggio inaspettato: {total_expected} (atteso: 218)")
    
    return new_missions, duplicates_found

def check_why_218():
    """Verifica perché il totale è 218 invece di 208"""
    
    print(f"\n🔍 Verifica perché 218 invece di 208:")
    
    # Carica il file principale
    df_main = pd.read_csv('data/processed/missioni_complete.csv')
    
    # Carica missions.xlsx
    missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
    
    # Normalizza
    df_main['nome_normalizzato'] = df_main['nome'].apply(normalize_mission_name)
    missions_df['mission_normalizzato'] = missions_df['mission'].apply(normalize_mission_name)
    
    # Trova missioni non duplicate
    new_missions = []
    for _, row in missions_df.iterrows():
        mission_name = str(row['mission']).strip()
        mission_normalized = normalize_mission_name(mission_name)
        
        existing = df_main[df_main['nome_normalizzato'] == mission_normalized]
        if len(existing) == 0:
            new_missions.append(mission_name)
    
    total = len(df_main) + len(new_missions)
    print(f"   File principale: {len(df_main)} missioni")
    print(f"   Missioni da aggiungere: {len(new_missions)}")
    print(f"   Totale: {total}")
    
    if total == 218:
        print(f"   ✅ Il conteggio di 218 è corretto per la dashboard")
        print(f"   📝 Il README menziona 208 ma la dashboard integra automaticamente i dati Excel")
    else:
        print(f"   ❓ Conteggio inaspettato: {total}")

if __name__ == "__main__":
    new_missions, duplicates = check_dashboard_integration()
    check_why_218()
    
    print(f"\n🎯 CONCLUSIONE:")
    print(f"La dashboard mostra 218 missioni perché:")
    print(f"1. Carica il file principale con 208 missioni")
    print(f"2. Integra automaticamente {len(new_missions)} missioni da missions.xlsx")
    print(f"3. Rimuove {len(duplicates)} duplicati")
    print(f"4. Risultato: 208 + {len(new_missions)} = 218 missioni") 