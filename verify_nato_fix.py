#!/usr/bin/env python3
"""
Script per verificare che le missioni NATO storiche siano state aggiunte correttamente
"""

import pandas as pd
import numpy as np

def verify_nato_fix():
    """Verifica che le missioni NATO storiche siano state aggiunte"""
    
    print("🔍 Verifica correzione dati NATO")
    print("=" * 50)
    
    # Carica il file aggiornato
    try:
        df = pd.read_csv('data/processed/missioni_complete_updated.csv')
        print(f"📊 File aggiornato caricato: {len(df)} missioni")
    except:
        try:
            df = pd.read_csv('data/processed/missioni_complete.csv')
            print(f"📊 File originale caricato: {len(df)} missioni")
        except Exception as e:
            print(f"❌ Errore nel caricamento: {e}")
            return
    
    # Filtra solo missioni NATO
    nato_missions = df[df['tipo_missione'] == 'NATO']
    print(f"📊 Missioni NATO totali: {len(nato_missions)}")
    
    # Analizza le date
    nato_missions['data_inizio'] = pd.to_datetime(nato_missions['data_inizio'], errors='coerce')
    nato_missions['anno'] = nato_missions['data_inizio'].dt.year
    
    # Trova l'anno più antico
    min_year = nato_missions['anno'].min()
    max_year = nato_missions['anno'].max()
    
    print(f"📅 Anno più antico: {min_year}")
    print(f"📅 Anno più recente: {max_year}")
    
    # Verifica se ora abbiamo dati dal 1949
    if min_year <= 1949:
        print("✅ SUCCESSO: Ora abbiamo dati NATO dal 1949!")
    else:
        print(f"❌ PROBLEMA: Ancora mancano dati prima del {min_year}")
    
    # Mostra le missioni per decennio
    print(f"\n📋 Missioni NATO per decennio:")
    decades = {}
    for _, row in nato_missions.iterrows():
        if pd.notna(row['anno']):
            decade = (row['anno'] // 10) * 10
            if decade not in decades:
                decades[decade] = []
            decades[decade].append(row['nome'])
    
    for decade in sorted(decades.keys()):
        print(f"   {decade}s: {len(decades[decade])} missioni")
        for mission in decades[decade]:
            print(f"     - {mission}")
    
    # Verifica specifica per il 1949
    missions_1949 = nato_missions[nato_missions['anno'] == 1949]
    if len(missions_1949) > 0:
        print(f"\n✅ Missioni NATO del 1949:")
        for _, mission in missions_1949.iterrows():
            print(f"   - {mission['nome']}")
    else:
        print(f"\n❌ Nessuna missione NATO del 1949 trovata")
    
    # Verifica per gli anni 1950-1991
    missing_years = []
    for year in range(1949, 1992):
        year_missions = nato_missions[nato_missions['anno'] == year]
        if len(year_missions) == 0:
            missing_years.append(year)
    
    if missing_years:
        print(f"\n⚠️  Anni ancora mancanti (1949-1991): {missing_years}")
    else:
        print(f"\n✅ Tutti gli anni dal 1949 al 1991 sono ora coperti!")

if __name__ == "__main__":
    verify_nato_fix() 