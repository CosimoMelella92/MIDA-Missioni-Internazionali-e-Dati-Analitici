#!/usr/bin/env python3
"""
Script per contare le missioni totali dopo integrazione e rimozione duplicati
"""

import pandas as pd
import re

def count_total_missions():
    """Conta le missioni totali come fa la dashboard"""
    
    # 1. Carica il CSV principale
    df_existing = pd.read_csv('data/processed/missioni_complete.csv')
    print(f"📊 CSV principale: {len(df_existing)} missioni")
    
    # 2. Carica missions.xlsx
    missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
    print(f"📊 Excel missions.xlsx: {len(missions_df)} missioni")
    
    # 3. Simula l'integrazione come fa la dashboard
    new_missions = []
    
    for _, row in missions_df.iterrows():
        mission_name = str(row['mission']).strip()
        
        # Controlla se la missione esiste già (come fa la dashboard)
        existing_mission = df_existing[df_existing['nome'].str.contains(mission_name, case=False, na=False)]
        
        if len(existing_mission) == 0:
            # Missione non esistente, aggiungi
            framework = str(row['framework']).strip() if pd.notna(row['framework']) else 'ONU'
            
            new_mission = {
                'nome': mission_name,
                'paese': str(row['country']).strip(),
                'regione': str(row['region']).strip() if pd.notna(row['region']) else 'Non specificata',
                'sub_regione': 'Non specificata',
                'tipo_partecipazione': 'civmil',
                'data_inizio': pd.to_datetime(row['date_start'], errors='coerce'),
                'data_fine': pd.to_datetime(row['date_end'], errors='coerce'),
                'personale_militare': 100,
                'personale_civile': 50,
                'personale_totale': 150,
                'costo_totale': 25000000,
                'tipo_missione': framework,
                'commitment': 'Troops'
            }
            new_missions.append(new_mission)
    
    print(f"📊 Missioni da aggiungere (non duplicate): {len(new_missions)}")
    
    # 4. Combina i dati
    if new_missions:
        new_df = pd.DataFrame(new_missions)
        df_combined = pd.concat([df_existing, new_df], ignore_index=True)
    else:
        df_combined = df_existing
    
    print(f"📊 Dopo integrazione: {len(df_combined)} missioni")
    
    # 5. Rimozione duplicati (come fa la dashboard)
    def normalize_name(name):
        return str(name).lower().replace(' ', '').replace('-', '').replace('_', '')
    
    # Crea colonne normalizzate per il confronto
    df_combined['__norm_nome'] = df_combined['nome'].apply(normalize_name)
    df_combined['__norm_paese'] = df_combined['paese'].str.lower().str.strip()
    
    # Trova e rimuovi duplicati
    duplicates_mask = df_combined.duplicated(subset=['__norm_nome', '__norm_paese'], keep='first')
    duplicates_removed = duplicates_mask.sum()
    
    df_final = df_combined[~duplicates_mask]
    
    print(f"📊 Duplicati rimossi: {duplicates_removed}")
    print(f"📊 Missioni finali (uniche): {len(df_final)}")
    
    # 6. Statistiche per organizzazione
    print("\n🏛️ Distribuzione per organizzazione:")
    print(df_final['tipo_missione'].value_counts())
    
    return len(df_final)

if __name__ == "__main__":
    total_missions = count_total_missions()
    print(f"\n🎯 TOTALE MISSIONI UNICHE: {total_missions}") 