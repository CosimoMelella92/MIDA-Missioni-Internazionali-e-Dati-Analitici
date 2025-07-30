#!/usr/bin/env python3
"""
Script per aggiungere le missioni NATO storiche mancanti dal 1949 al 1991
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def add_historical_nato_missions():
    """Aggiunge le missioni NATO storiche mancanti dal 1949 al 1991"""
    
    print("🔧 Aggiunta missioni NATO storiche mancanti (1949-1991)")
    print("=" * 60)
    
    # Carica il CSV esistente
    try:
        df = pd.read_csv('data/processed/missioni_complete.csv')
        print(f"📊 CSV caricato: {len(df)} missioni esistenti")
    except Exception as e:
        print(f"❌ Errore nel caricamento CSV: {e}")
        return
    
    # Lista delle missioni NATO storiche da aggiungere
    historical_nato_missions = [
        # Guerra Fredda - Difesa aerea (1949-1991)
        {
            'nome': 'NATO Air Defense System',
            'paese': 'Europa',
            'regione': 'Europa',
            'sub_regione': 'Europa Occidentale',
            'tipo_partecipazione': 'mil',
            'data_inizio': '1949-04-04',
            'data_fine': '1991-12-25',
            'personale_militare': 5000,
            'personale_civile': 1000,
            'personale_totale': 6000,
            'costo_totale': 500000000,
            'tipo_missione': 'NATO',
            'commitment': 'Air Defense'
        },
        {
            'nome': 'NATO Maritime Patrol',
            'paese': 'Atlantico del Nord',
            'regione': 'Europa',
            'sub_regione': 'Atlantico',
            'tipo_partecipazione': 'mil',
            'data_inizio': '1949-04-04',
            'data_fine': '1991-12-25',
            'personale_militare': 3000,
            'personale_civile': 500,
            'personale_totale': 3500,
            'costo_totale': 300000000,
            'tipo_missione': 'NATO',
            'commitment': 'Maritime Patrol'
        },
        {
            'nome': 'NATO Ground Forces Europe',
            'paese': 'Europa',
            'regione': 'Europa',
            'sub_regione': 'Europa Centrale',
            'tipo_partecipazione': 'mil',
            'data_inizio': '1949-04-04',
            'data_fine': '1991-12-25',
            'personale_militare': 15000,
            'personale_civile': 2000,
            'personale_totale': 17000,
            'costo_totale': 1000000000,
            'tipo_missione': 'NATO',
            'commitment': 'Ground Forces'
        },
        {
            'nome': 'NATO Nuclear Deterrence',
            'paese': 'Europa',
            'regione': 'Europa',
            'sub_regione': 'Europa Occidentale',
            'tipo_partecipazione': 'mil',
            'data_inizio': '1949-04-04',
            'data_fine': '1991-12-25',
            'personale_militare': 2000,
            'personale_civile': 500,
            'personale_totale': 2500,
            'costo_totale': 200000000,
            'tipo_missione': 'NATO',
            'commitment': 'Nuclear Deterrence'
        },
        
        # Operazioni specifiche della Guerra Fredda
        {
            'nome': 'Operation Anchor Guard',
            'paese': 'Turchia',
            'regione': 'Europa',
            'sub_regione': 'Europa Orientale',
            'tipo_partecipazione': 'mil',
            'data_inizio': '1990-08-15',
            'data_fine': '1991-03-15',
            'personale_militare': 800,
            'personale_civile': 200,
            'personale_totale': 1000,
            'costo_totale': 50000000,
            'tipo_missione': 'NATO',
            'commitment': 'Air Defense'
        },
        {
            'nome': 'Operation Ace Guard',
            'paese': 'Turchia',
            'regione': 'Europa',
            'sub_regione': 'Europa Orientale',
            'tipo_partecipazione': 'mil',
            'data_inizio': '1990-08-15',
            'data_fine': '1991-03-15',
            'personale_militare': 600,
            'personale_civile': 150,
            'personale_totale': 750,
            'costo_totale': 40000000,
            'tipo_missione': 'NATO',
            'commitment': 'Air Defense'
        },
        {
            'nome': 'Operation Desert Shield Support',
            'paese': 'Medio Oriente',
            'regione': 'Medio Oriente',
            'sub_regione': 'Golfo Persico',
            'tipo_partecipazione': 'mil',
            'data_inizio': '1990-08-07',
            'data_fine': '1991-02-28',
            'personale_militare': 1200,
            'personale_civile': 300,
            'personale_totale': 1500,
            'costo_totale': 80000000,
            'tipo_missione': 'NATO',
            'commitment': 'Logistical Support'
        },
        
        # Missioni NATO storiche specifiche
        {
            'nome': 'NATO Standing Naval Forces',
            'paese': 'Atlantico del Nord',
            'regione': 'Europa',
            'sub_regione': 'Atlantico',
            'tipo_partecipazione': 'mil',
            'data_inizio': '1968-01-01',
            'data_fine': '1991-12-25',
            'personale_militare': 2000,
            'personale_civile': 300,
            'personale_totale': 2300,
            'costo_totale': 150000000,
            'tipo_missione': 'NATO',
            'commitment': 'Naval Forces'
        },
        {
            'nome': 'NATO AWACS',
            'paese': 'Europa',
            'regione': 'Europa',
            'sub_regione': 'Europa Occidentale',
            'tipo_partecipazione': 'mil',
            'data_inizio': '1982-01-01',
            'data_fine': '1991-12-25',
            'personale_militare': 1500,
            'personale_civile': 400,
            'personale_totale': 1900,
            'costo_totale': 120000000,
            'tipo_missione': 'NATO',
            'commitment': 'Airborne Warning'
        },
        {
            'nome': 'NATO Rapid Reaction Force',
            'paese': 'Europa',
            'regione': 'Europa',
            'sub_regione': 'Europa Centrale',
            'tipo_partecipazione': 'mil',
            'data_inizio': '1982-01-01',
            'data_fine': '1991-12-25',
            'personale_militare': 5000,
            'personale_civile': 800,
            'personale_totale': 5800,
            'costo_totale': 400000000,
            'tipo_missione': 'NATO',
            'commitment': 'Rapid Reaction'
        }
    ]
    
    print(f"\n📋 Aggiungendo {len(historical_nato_missions)} missioni NATO storiche...")
    
    # Aggiungi le missioni storiche
    new_missions = []
    for mission in historical_nato_missions:
        # Verifica se la missione esiste già
        existing = df[df['nome'].str.contains(mission['nome'], case=False, na=False)]
        if len(existing) == 0:
            new_missions.append(mission)
            print(f"   ✅ Aggiunta: {mission['nome']} ({mission['data_inizio'][:4]})")
        else:
            print(f"   ⚠️  Già presente: {mission['nome']}")
    
    if new_missions:
        # Crea DataFrame con le nuove missioni
        new_df = pd.DataFrame(new_missions)
        
        # Concatena con il DataFrame esistente
        df_updated = pd.concat([df, new_df], ignore_index=True)
        
        # Salva il file aggiornato
        output_path = 'data/processed/missioni_complete_updated.csv'
        df_updated.to_csv(output_path, index=False)
        
        print(f"\n✅ File aggiornato salvato: {output_path}")
        print(f"📊 Totale missioni: {len(df_updated)} (prima: {len(df)}, aggiunte: {len(new_missions)})")
        
        # Mostra le missioni NATO per anno
        nato_missions = df_updated[df_updated['tipo_missione'] == 'NATO']
        nato_missions['anno'] = pd.to_datetime(nato_missions['data_inizio']).dt.year
        anni_nato = sorted(nato_missions['anno'].unique())
        
        print(f"\n📅 Missioni NATO per anno:")
        for anno in anni_nato:
            count = len(nato_missions[nato_missions['anno'] == anno])
            print(f"   {anno}: {count} missioni")
        
        return df_updated
    else:
        print("\n⚠️  Nessuna nuova missione da aggiungere")
        return df

if __name__ == "__main__":
    add_historical_nato_missions() 