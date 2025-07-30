#!/usr/bin/env python3
"""
Script per analizzare i dati NATO e identificare le missioni mancanti dal 1949 al 1992
"""

import pandas as pd
import numpy as np
from pathlib import Path

def analyze_nato_data():
    """Analizza i dati NATO per identificare missioni mancanti"""
    
    print("🔍 Analisi dati NATO - Missioni mancanti dal 1949 al 1992")
    print("=" * 60)
    
    # 1. Carica il CSV principale
    try:
        df_csv = pd.read_csv('data/processed/missioni_complete.csv')
        print(f"📊 CSV principale: {len(df_csv)} missioni")
        
        # Filtra solo missioni NATO
        nato_csv = df_csv[df_csv['tipo_missione'] == 'NATO']
        print(f"📊 Missioni NATO nel CSV: {len(nato_csv)}")
        
        if len(nato_csv) > 0:
            print("\n📋 Missioni NATO nel CSV:")
            for _, row in nato_csv.iterrows():
                start_year = pd.to_datetime(row['data_inizio']).year
                print(f"   - {row['nome']}: {start_year}")
        
    except Exception as e:
        print(f"❌ Errore nel caricamento CSV: {e}")
        return
    
    # 2. Carica missions.xlsx
    try:
        missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
        print(f"\n📊 Excel missions.xlsx: {len(missions_df)} missioni")
        
        # Filtra solo missioni NATO
        nato_excel = missions_df[missions_df['framework'] == 'NATO']
        print(f"📊 Missioni NATO nell'Excel: {len(nato_excel)}")
        
        if len(nato_excel) > 0:
            print("\n📋 Missioni NATO nell'Excel:")
            for _, row in nato_excel.iterrows():
                start_date = pd.to_datetime(row['date_start'], errors='coerce')
                start_year = start_date.year if pd.notna(start_date) else 'N/A'
                print(f"   - {row['mission']}: {start_year}")
        
    except Exception as e:
        print(f"❌ Errore nel caricamento Excel: {e}")
        return
    
    # 3. Analizza le date
    print("\n📅 Analisi delle date:")
    
    # Date dal CSV
    if len(nato_csv) > 0:
        csv_years = nato_csv['data_inizio'].apply(lambda x: pd.to_datetime(x).year).sort_values()
        print(f"   Anni missioni NATO nel CSV: {list(csv_years)}")
        print(f"   Anno più antico: {csv_years.min()}")
        print(f"   Anno più recente: {csv_years.max()}")
    
    # Date dall'Excel
    if len(nato_excel) > 0:
        excel_years = []
        for _, row in nato_excel.iterrows():
            start_date = pd.to_datetime(row['date_start'], errors='coerce')
            if pd.notna(start_date):
                excel_years.append(start_date.year)
        
        if excel_years:
            excel_years.sort()
            print(f"   Anni missioni NATO nell'Excel: {excel_years}")
            print(f"   Anno più antico: {min(excel_years)}")
            print(f"   Anno più recente: {max(excel_years)}")
    
    # 4. Identifica missioni NATO storiche mancanti (1949-1991)
    print("\n🔍 Missioni NATO storiche che dovrebbero essere presenti (1949-1991):")
    
    # Lista di missioni NATO storiche note
    historical_nato_missions = [
        # Guerra Fredda (1949-1991)
        {"nome": "NATO Air Defense", "periodo": "1949-1991", "descrizione": "Difesa aerea NATO durante la Guerra Fredda"},
        {"nome": "NATO Maritime Patrol", "periodo": "1949-1991", "descrizione": "Pattugliamento marittimo NATO"},
        {"nome": "NATO Ground Forces", "periodo": "1949-1991", "descrizione": "Forze terrestri NATO in Europa"},
        {"nome": "NATO Nuclear Deterrence", "periodo": "1949-1991", "descrizione": "Deterrenza nucleare NATO"},
        
        # Operazioni specifiche
        {"nome": "Operation Anchor Guard", "periodo": "1990-1991", "descrizione": "Operazione NATO durante la Guerra del Golfo"},
        {"nome": "Operation Ace Guard", "periodo": "1990-1991", "descrizione": "Operazione NATO durante la Guerra del Golfo"},
        {"nome": "Operation Desert Shield", "periodo": "1990-1991", "descrizione": "Operazione NATO durante la Guerra del Golfo"},
    ]
    
    for mission in historical_nato_missions:
        print(f"   - {mission['nome']} ({mission['periodo']}): {mission['descrizione']}")
    
    # 5. Verifica se queste missioni sono presenti nei dati
    print("\n✅ Verifica presenza nei dati attuali:")
    
    csv_mission_names = [name.lower() for name in nato_csv['nome'].tolist()]
    excel_mission_names = [name.lower() for name in nato_excel['mission'].tolist()]
    
    for mission in historical_nato_missions:
        mission_name_lower = mission['nome'].lower()
        in_csv = any(mission_name_lower in name for name in csv_mission_names)
        in_excel = any(mission_name_lower in name for name in excel_mission_names)
        
        status = "✅ Presente" if (in_csv or in_excel) else "❌ Mancante"
        print(f"   {status} - {mission['nome']}")
    
    # 6. Raccomandazioni
    print("\n💡 Raccomandazioni:")
    print("   1. Aggiungere le missioni NATO storiche dal 1949 al 1991")
    print("   2. Verificare le fonti ufficiali NATO per missioni mancanti")
    print("   3. Considerare l'aggiunta di missioni NATO di supporto e deterrenza")
    print("   4. Aggiornare il dataset con missioni NATO della Guerra Fredda")

if __name__ == "__main__":
    analyze_nato_data() 