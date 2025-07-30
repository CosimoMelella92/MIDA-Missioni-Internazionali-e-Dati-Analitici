#!/usr/bin/env python3
"""
Script per analizzare tutti i file Excel e identificare i dati NATO mancanti dal 1949 al 1992
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os

def analyze_all_excel_files():
    """Analizza tutti i file Excel per identificare dati NATO mancanti"""
    
    print("🔍 Analisi completa di tutti i file Excel - Dati NATO mancanti")
    print("=" * 70)
    
    # Lista di tutti i file Excel da analizzare
    excel_files = [
        'data/raw/Excel/missions.xlsx',
        'data/raw/Excel/missions_expenditure_Italy.xlsx',
        'data/raw/Excel/Matrice dati.xlsx',
        'data/raw/Excel/f319e1c8-5654-4cd6-b4c7-5722ae437d30-Ukraine_Support_Tracker_Release_21.xlsx',
        'data/raw/Matrice dati 1AGG.xlsx',
        'data/processed/Matrice dati 1AGG_enriched.xlsx'
    ]
    
    all_nato_data = []
    
    for excel_file in excel_files:
        if os.path.exists(excel_file):
            print(f"\n📊 Analizzando: {excel_file}")
            print("-" * 50)
            
            try:
                # Carica il file Excel
                df = pd.read_excel(excel_file)
                print(f"   Righe: {len(df)}")
                print(f"   Colonne: {list(df.columns)}")
                
                # Cerca colonne che potrebbero contenere dati NATO
                nato_columns = []
                for col in df.columns:
                    col_lower = str(col).lower()
                    if 'nato' in col_lower or 'framework' in col_lower or 'organization' in col_lower or 'tipo' in col_lower:
                        nato_columns.append(col)
                
                print(f"   Colonne rilevanti: {nato_columns}")
                
                # Analizza i dati NATO se presenti
                nato_data = analyze_nato_in_file(df, excel_file, nato_columns)
                if nato_data:
                    all_nato_data.extend(nato_data)
                
            except Exception as e:
                print(f"   ❌ Errore nel caricamento: {e}")
        else:
            print(f"\n❌ File non trovato: {excel_file}")
    
    # Analisi complessiva
    print("\n" + "=" * 70)
    print("📋 ANALISI COMPLESSIVA DATI NATO")
    print("=" * 70)
    
    if all_nato_data:
        print(f"\n📊 Totale missioni NATO trovate: {len(all_nato_data)}")
        
        # Raggruppa per anno
        years = []
        for data in all_nato_data:
            if 'year' in data and data['year']:
                years.append(data['year'])
        
        if years:
            years.sort()
            print(f"   Anni coperti: {years}")
            print(f"   Anno più antico: {min(years)}")
            print(f"   Anno più recente: {max(years)}")
            
            # Identifica gap
            missing_years = []
            for year in range(1949, 1992):
                if year not in years:
                    missing_years.append(year)
            
            if missing_years:
                print(f"\n❌ Anni mancanti (1949-1991): {missing_years}")
            else:
                print(f"\n✅ Tutti gli anni dal 1949 al 1991 sono coperti")
        
        # Mostra tutte le missioni NATO trovate
        print(f"\n📋 Missioni NATO trovate:")
        for data in all_nato_data:
            print(f"   - {data.get('name', 'N/A')} ({data.get('year', 'N/A')}) - {data.get('file', 'N/A')}")
    
    else:
        print("\n❌ Nessun dato NATO trovato nei file Excel")
    
    # Raccomandazioni
    print("\n💡 RACCOMANDAZIONI:")
    print("   1. Verificare se i file Excel contengono dati NATO storici (1949-1991)")
    print("   2. Controllare le fonti ufficiali NATO per dati mancanti")
    print("   3. Considerare l'aggiunta di missioni NATO della Guerra Fredda")
    print("   4. Aggiornare il dataset con missioni NATO dal 1949")

def analyze_nato_in_file(df, filename, nato_columns):
    """Analizza i dati NATO in un singolo file"""
    nato_data = []
    
    try:
        # Cerca colonne che contengono 'NATO'
        for col in df.columns:
            col_lower = str(col).lower()
            if 'nato' in col_lower:
                print(f"   Colonna NATO trovata: {col}")
                
                # Conta i valori NATO
                nato_values = df[df[col].astype(str).str.contains('NATO', case=False, na=False)]
                print(f"   Valori NATO trovati: {len(nato_values)}")
                
                # Analizza i dati NATO
                for _, row in nato_values.iterrows():
                    nato_info = extract_nato_info(row, filename)
                    if nato_info:
                        nato_data.append(nato_info)
        
        # Cerca anche nella colonna 'framework' se presente
        if 'framework' in df.columns:
            nato_framework = df[df['framework'].astype(str).str.contains('NATO', case=False, na=False)]
            print(f"   Missioni con framework NATO: {len(nato_framework)}")
            
            for _, row in nato_framework.iterrows():
                nato_info = extract_nato_info(row, filename)
                if nato_info:
                    nato_data.append(nato_info)
        
        # Cerca anche nella colonna 'tipo_missione' se presente
        tipo_columns = [col for col in df.columns if 'tipo' in str(col).lower()]
        for col in tipo_columns:
            nato_tipo = df[df[col].astype(str).str.contains('NATO', case=False, na=False)]
            if len(nato_tipo) > 0:
                print(f"   Missioni NATO in {col}: {len(nato_tipo)}")
                
                for _, row in nato_tipo.iterrows():
                    nato_info = extract_nato_info(row, filename)
                    if nato_info:
                        nato_data.append(nato_info)
        
    except Exception as e:
        print(f"   ❌ Errore nell'analisi: {e}")
    
    return nato_data

def extract_nato_info(row, filename):
    """Estrae informazioni NATO da una riga"""
    try:
        # Cerca colonne con date
        date_columns = [col for col in row.index if 'date' in str(col).lower() or 'anno' in str(col).lower() or 'year' in str(col).lower()]
        
        year = None
        for col in date_columns:
            try:
                date_val = pd.to_datetime(row[col], errors='coerce')
                if pd.notna(date_val):
                    year = date_val.year
                    break
            except:
                continue
        
        # Cerca nome missione
        name_columns = [col for col in row.index if 'name' in str(col).lower() or 'mission' in str(col).lower() or 'nome' in str(col).lower()]
        name = None
        for col in name_columns:
            if pd.notna(row[col]) and str(row[col]).strip():
                name = str(row[col]).strip()
                break
        
        if name or year:
            return {
                'name': name,
                'year': year,
                'file': filename,
                'row_data': row.to_dict()
            }
    
    except Exception as e:
        print(f"   ❌ Errore nell'estrazione: {e}")
    
    return None

if __name__ == "__main__":
    analyze_all_excel_files() 