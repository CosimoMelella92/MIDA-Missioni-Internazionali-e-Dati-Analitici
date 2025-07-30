#!/usr/bin/env python3
"""
Script per verificare esattamente i numeri e capire perché 208 + 16 non fa 218
"""

import pandas as pd

def verify_numbers():
    """Verifica i numeri esatti"""
    
    print("🔍 Verifica numeri esatti")
    print("=" * 40)
    
    # 1. File principale
    df_main = pd.read_csv('data/processed/missioni_complete.csv')
    print(f"📊 File principale: {len(df_main)} missioni")
    
    # 2. Excel missions.xlsx
    missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
    print(f"📊 Excel missions.xlsx: {len(missions_df)} righe")
    
    # 3. Missioni da aggiungere (str.contains)
    new_missions = []
    for _, row in missions_df.iterrows():
        mission_name = str(row['mission']).strip()
        existing = df_main[df_main['nome'].str.contains(mission_name, case=False, na=False)]
        if len(existing) == 0:
            new_missions.append(mission_name)
    
    print(f"📊 Missioni da aggiungere: {len(new_missions)}")
    
    # 4. Calcoli
    total_expected = len(df_main) + len(new_missions)
    print(f"📊 Totale atteso: {len(df_main)} + {len(new_missions)} = {total_expected}")
    print(f"📊 Dashboard mostra: 218")
    print(f"📊 Differenza: {total_expected} - 218 = {total_expected - 218}")
    
    # 5. Verifica dashboard
    from dashboard.missioni_dashboard import load_data
    df_dashboard = load_data()
    print(f"📊 Dashboard effettiva: {len(df_dashboard)} missioni")
    
    # 6. Analisi
    if total_expected == 218:
        print("✅ I numeri sono corretti")
    elif total_expected > 218:
        print(f"❌ Il totale atteso ({total_expected}) è maggiore di 218")
        print(f"   Questo significa che la dashboard rimuove {total_expected - 218} missioni")
    else:
        print(f"❌ Il totale atteso ({total_expected}) è minore di 218")
        print(f"   Questo significa che la dashboard aggiunge {218 - total_expected} missioni")
    
    return len(df_main), len(new_missions), len(df_dashboard)

if __name__ == "__main__":
    main_count, new_count, dashboard_count = verify_numbers()
    
    print(f"\n🎯 RIEPILOGO:")
    print(f"File principale: {main_count}")
    print(f"Missioni da aggiungere: {new_count}")
    print(f"Totale atteso: {main_count + new_count}")
    print(f"Dashboard effettiva: {dashboard_count}")
    print(f"Differenza: {main_count + new_count - dashboard_count}") 