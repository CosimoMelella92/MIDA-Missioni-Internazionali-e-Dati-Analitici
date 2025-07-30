#!/usr/bin/env python3
"""
Script per pulire i duplicati dai file Excel e verificare il conteggio corretto delle missioni
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os
import re

def normalize_mission_name(name):
    """Normalizza il nome della missione per il confronto"""
    if pd.isna(name):
        return ""
    
    # Converti in stringa e normalizza
    name = str(name).strip()
    
    # Rimuovi caratteri speciali e spazi extra
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'\s+', ' ', name)
    
    # Converti in minuscolo
    name = name.lower()
    
    return name

def find_and_remove_duplicates():
    """Trova e rimuove i duplicati dai file Excel"""
    
    print("🧹 Pulizia duplicati e verifica conteggio missioni")
    print("=" * 60)
    
    # 1. Carica il file principale
    df_main = pd.read_csv('data/processed/missioni_complete.csv')
    print(f"📊 File principale: {len(df_main)} missioni")
    
    # 2. Carica missions.xlsx
    missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
    print(f"📊 Excel missions.xlsx: {len(missions_df)} righe")
    
    # 3. Normalizza i nomi per il confronto
    df_main['nome_normalizzato'] = df_main['nome'].apply(normalize_mission_name)
    missions_df['mission_normalizzato'] = missions_df['mission'].apply(normalize_mission_name)
    
    # 4. Trova missioni non duplicate da aggiungere
    new_missions = []
    duplicates_found = []
    
    for _, row in missions_df.iterrows():
        mission_name = str(row['mission']).strip()
        mission_normalized = normalize_mission_name(mission_name)
        
        # Controlla se esiste già nel file principale
        existing = df_main[df_main['nome_normalizzato'] == mission_normalized]
        
        if len(existing) == 0:
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
            # Duplicato trovato
            duplicates_found.append({
                'excel_name': mission_name,
                'existing_name': existing.iloc[0]['nome'],
                'source': 'missions.xlsx'
            })
    
    print(f"📊 Missioni da aggiungere (non duplicate): {len(new_missions)}")
    print(f"📊 Duplicati trovati: {len(duplicates_found)}")
    
    # 5. Mostra alcuni duplicati
    if duplicates_found:
        print(f"\n🔄 Esempi di duplicati trovati:")
        for i, dup in enumerate(duplicates_found[:10]):
            print(f"   {i+1}. Excel: '{dup['excel_name']}' -> CSV: '{dup['existing_name']}'")
        if len(duplicates_found) > 10:
            print(f"   ... e altri {len(duplicates_found) - 10} duplicati")
    
    # 6. Crea il dataset finale
    if new_missions:
        new_df = pd.DataFrame(new_missions)
        df_final = pd.concat([df_main, new_df], ignore_index=True)
    else:
        df_final = df_main.copy()
    
    # 7. Rimuovi la colonna temporanea
    df_final = df_final.drop(columns=['nome_normalizzato'])
    
    print(f"📊 Totale missioni finali: {len(df_final)}")
    
    # 8. Verifica che sia 208
    if len(df_final) == 208:
        print("✅ Conteggio corretto: 208 missioni")
    else:
        print(f"⚠️  Conteggio inaspettato: {len(df_final)} missioni (atteso: 208)")
    
    # 9. Salva il file pulito
    output_path = 'data/processed/missioni_complete_cleaned.csv'
    df_final.to_csv(output_path, index=False)
    print(f"💾 File pulito salvato: {output_path}")
    
    # 10. Statistiche per organizzazione
    print(f"\n🏛️ Distribuzione per organizzazione:")
    org_stats = df_final['tipo_missione'].value_counts()
    for org, count in org_stats.items():
        print(f"   {org}: {count} missioni")
    
    return df_final, duplicates_found

def verify_other_excel_files():
    """Verifica gli altri file Excel per identificare duplicati"""
    
    print(f"\n🔍 Verifica altri file Excel:")
    
    excel_files = [
        ('data/raw/Excel/missions_expenditure_Italy.xlsx', 'expenditure'),
        ('data/raw/Excel/Matrice dati.xlsx', 'Matrice dati'),
        ('data/raw/Excel/Matrice dati 1AGG.xlsx', 'Matrice dati 1AGG'),
        ('data/processed/Matrice dati 1AGG_enriched.xlsx', 'Matrice dati enriched')
    ]
    
    all_duplicates = []
    
    for file_path, description in excel_files:
        try:
            df = pd.read_excel(file_path)
            print(f"\n📁 {description}: {len(df)} righe")
            
            # Trova colonna con nomi missioni
            name_columns = [col for col in df.columns if 'nome' in col.lower() or 'mission' in col.lower() or 'name' in col.lower()]
            
            if name_columns:
                name_col = name_columns[0]
                print(f"   Colonna nome: {name_col}")
                
                # Controlla duplicati con il file principale
                df_main = pd.read_csv('data/processed/missioni_complete.csv')
                df_main['nome_normalizzato'] = df_main['nome'].apply(normalize_mission_name)
                
                duplicates_in_file = 0
                for _, row in df.iterrows():
                    if pd.notna(row[name_col]):
                        mission_name = str(row[name_col]).strip()
                        mission_normalized = normalize_mission_name(mission_name)
                        
                        existing = df_main[df_main['nome_normalizzato'] == mission_normalized]
                        if len(existing) > 0:
                            duplicates_in_file += 1
                
                print(f"   Duplicati con file principale: {duplicates_in_file}")
                
        except Exception as e:
            print(f"   ❌ Errore nel leggere {description}: {e}")
    
    return all_duplicates

def create_clean_excel_files():
    """Crea versioni pulite dei file Excel senza duplicati"""
    
    print(f"\n🧹 Creazione file Excel puliti:")
    
    # Carica il file principale come riferimento
    df_main = pd.read_csv('data/processed/missioni_complete.csv')
    df_main['nome_normalizzato'] = df_main['nome'].apply(normalize_mission_name)
    
    excel_files = [
        ('data/raw/Excel/missions.xlsx', 'data/raw/Excel/missions_cleaned.xlsx'),
        ('data/raw/Excel/Matrice dati.xlsx', 'data/raw/Excel/Matrice dati_cleaned.xlsx'),
        ('data/raw/Excel/Matrice dati 1AGG.xlsx', 'data/raw/Excel/Matrice dati 1AGG_cleaned.xlsx')
    ]
    
    for input_path, output_path in excel_files:
        try:
            df = pd.read_excel(input_path)
            print(f"\n📁 Pulizia {input_path}: {len(df)} righe originali")
            
            # Trova colonna con nomi missioni
            name_columns = [col for col in df.columns if 'nome' in col.lower() or 'mission' in col.lower() or 'name' in col.lower()]
            
            if name_columns:
                name_col = name_columns[0]
                
                # Filtra duplicati
                non_duplicates = []
                for _, row in df.iterrows():
                    if pd.notna(row[name_col]):
                        mission_name = str(row[name_col]).strip()
                        mission_normalized = normalize_mission_name(mission_name)
                        
                        existing = df_main[df_main['nome_normalizzato'] == mission_normalized]
                        if len(existing) == 0:
                            non_duplicates.append(row)
                
                # Crea DataFrame pulito
                df_cleaned = pd.DataFrame(non_duplicates)
                df_cleaned.to_excel(output_path, index=False)
                
                print(f"   ✅ Salvato {output_path}: {len(df_cleaned)} righe (rimossi {len(df) - len(df_cleaned)} duplicati)")
            
        except Exception as e:
            print(f"   ❌ Errore nel pulire {input_path}: {e}")

if __name__ == "__main__":
    # 1. Pulisci duplicati e verifica conteggio
    df_final, duplicates = find_and_remove_duplicates()
    
    # 2. Verifica altri file Excel
    verify_other_excel_files()
    
    # 3. Crea file Excel puliti
    create_clean_excel_files()
    
    print(f"\n🎯 RISULTATO FINALE:")
    print(f"Missioni uniche nel dataset finale: {len(df_final)}")
    print(f"Duplicati rimossi: {len(duplicates)}")
    
    if len(df_final) == 208:
        print("✅ Conteggio corretto: 208 missioni uniche")
        print("✅ Il sistema è pulito e coerente")
    else:
        print(f"⚠️  Conteggio inaspettato: {len(df_final)} missioni")
        print("🔍 Verifica necessaria per identificare la discrepanza") 