#!/usr/bin/env python3
"""
Script per aggiungere le nuove missioni bilaterali al dataset esistente
"""

import pandas as pd
from datetime import datetime

def add_bilateral_missions():
    """Aggiunge le nuove missioni bilaterali al dataset"""
    
    # Dati delle nuove missioni bilaterali
    new_missions = [
        {
            'nome': 'Italcon',
            'paese': 'Libano',
            'regione': 'Medio Oriente',
            'sub_regione': 'Levant',
            'tipo_partecipazione': 'mil',
            'data_inizio': '1982-08-24',
            'data_fine': '1984-03-20',
            'personale_militare': 8200.0,
            'personale_civile': 0.0,
            'personale_totale': 8200,
            'costo_totale': 25000000,
            'tipo_missione': 'Bilaterale',
            'commitment': 'Troops (Ground Forces)'
        },
        {
            'nome': 'MIBIL',
            'paese': 'Libano',
            'regione': 'Medio Oriente',
            'sub_regione': 'Levant',
            'tipo_partecipazione': 'civmil',
            'data_inizio': '2015-01-28',
            'data_fine': '2024-12-31',
            'personale_militare': 100.0,
            'personale_civile': 50.0,
            'personale_totale': 150,
            'costo_totale': 25000000,
            'tipo_missione': 'Bilaterale',
            'commitment': 'Advisory/Training'
        },
        {
            'nome': 'MIASIT',
            'paese': 'Libia',
            'regione': 'Nord Africa',
            'sub_regione': 'Nord Africa e Mediterraneo',
            'tipo_partecipazione': 'civmil',
            'data_inizio': '2018-01-01',
            'data_fine': '2024-12-31',
            'personale_militare': 100.0,
            'personale_civile': 50.0,
            'personale_totale': 150,
            'costo_totale': 25000000,
            'tipo_missione': 'Bilaterale',
            'commitment': 'Logistical Support & Advisory'
        },
        {
            'nome': 'MISIN',
            'paese': 'Niger',
            'regione': 'Africa Sub-sahariana',
            'sub_regione': 'Africa Occidentale',
            'tipo_partecipazione': 'civmil',
            'data_inizio': '2018-01-01',
            'data_fine': '2024-12-31',
            'personale_militare': 100.0,
            'personale_civile': 50.0,
            'personale_totale': 150,
            'costo_totale': 25000000,
            'tipo_missione': 'Bilaterale',
            'commitment': 'Advisory/Training'
        },
        {
            'nome': 'Cooperazione tecnica Angola',
            'paese': 'Angola',
            'regione': 'Africa Sub-sahariana',
            'sub_regione': 'Africa Australe',
            'tipo_partecipazione': 'mil',
            'data_inizio': '2017-01-01',
            'data_fine': '2024-12-31',
            'personale_militare': 50.0,
            'personale_civile': 0.0,
            'personale_totale': 50,
            'costo_totale': 25000000,
            'tipo_missione': 'Bilaterale',
            'commitment': 'Advisory/Training'
        }
    ]
    
    # Carica il dataset esistente
    try:
        df_existing = pd.read_csv('data/processed/missioni_complete.csv')
        print(f"✅ Dataset esistente caricato: {len(df_existing)} missioni")
    except FileNotFoundError:
        print("❌ File missioni_complete.csv non trovato")
        return
    
    # Crea DataFrame con le nuove missioni
    df_new = pd.DataFrame(new_missions)
    
    # Verifica se le missioni esistono già
    existing_missions = df_existing['nome'].tolist()
    new_missions_names = df_new['nome'].tolist()
    
    # Filtra solo le missioni che non esistono già
    missions_to_add = []
    for mission in new_missions:
        if mission['nome'] not in existing_missions:
            missions_to_add.append(mission)
        else:
            print(f"⚠️  Missione '{mission['nome']}' già presente nel dataset")
    
    if not missions_to_add:
        print("❌ Tutte le missioni sono già presenti nel dataset")
        return
    
    # Crea DataFrame con le missioni da aggiungere
    df_to_add = pd.DataFrame(missions_to_add)
    
    # Combina i dataset
    df_combined = pd.concat([df_existing, df_to_add], ignore_index=True)
    
    # Salva il dataset aggiornato
    output_file = 'data/processed/missioni_complete.csv'
    df_combined.to_csv(output_file, index=False)
    
    print(f"✅ Aggiunte {len(missions_to_add)} nuove missioni bilaterali:")
    for mission in missions_to_add:
        print(f"   - {mission['nome']} ({mission['paese']})")
    
    print(f"📊 Dataset aggiornato salvato in: {output_file}")
    print(f"📈 Totale missioni nel dataset: {len(df_combined)}")

if __name__ == "__main__":
    add_bilateral_missions() 