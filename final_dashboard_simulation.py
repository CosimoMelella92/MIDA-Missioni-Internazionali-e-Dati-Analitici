#!/usr/bin/env python3
"""
Script per simulare esattamente quello che fa la dashboard: carica missioni_complete_updated.csv e poi integra i dati Excel
"""

import pandas as pd
import re

def normalize_name(name):
    """Normalizza il nome come fa la dashboard"""
    return str(name).lower().replace(' ', '').replace('-', '').replace('_', '')

def final_dashboard_simulation():
    """Simula esattamente il processo della dashboard"""
    
    print("🔍 Simulazione finale della dashboard")
    print("=" * 50)
    
    # 1. Carica il file che usa la dashboard (missioni_complete_updated.csv)
    try:
        df_main = pd.read_csv('data/processed/missioni_complete_updated.csv')
        print(f"📊 File caricato dalla dashboard: missioni_complete_updated.csv - {len(df_main)} missioni")
    except:
        df_main = pd.read_csv('data/processed/missioni_complete.csv')
        print(f"📊 File di fallback: missioni_complete.csv - {len(df_main)} missioni")
    
    # 2. Carica missions.xlsx
    missions_df = pd.read_excel('data/raw/Excel/missions.xlsx')
    print(f"📊 Excel missions.xlsx: {len(missions_df)} righe")
    
    # 3. Simula l'aggiunta di missioni (str.contains)
    new_missions = []
    for _, row in missions_df.iterrows():
        mission_name = str(row['mission']).strip()
        existing = df_main[df_main['nome'].str.contains(mission_name, case=False, na=False)]
        if len(existing) == 0:
            new_missions.append({
                'nome': mission_name,
                'paese': row.get('country', 'Non specificato'),
                'tipo_missione': row.get('organization', 'Non specificato')
            })
    
    print(f"📊 Missioni da aggiungere da Excel: {len(new_missions)}")
    
    # 4. Crea il dataset combinato
    combined_data = []
    
    # Aggiungi missioni dal file principale
    for _, row in df_main.iterrows():
        combined_data.append({
            'nome': row['nome'],
            'paese': row['paese'],
            'tipo_missione': row['tipo_missione'],
            'fonte': 'file_principale'
        })
    
    # Aggiungi nuove missioni
    for mission in new_missions:
        combined_data.append({
            'nome': mission['nome'],
            'paese': mission['paese'],
            'tipo_missione': mission['tipo_missione'],
            'fonte': 'excel_aggiunto'
        })
    
    df_combined = pd.DataFrame(combined_data)
    print(f"📊 Totale dopo aggiunta: {len(df_combined)} missioni")
    
    # 5. PRIMA DEDUPLICAZIONE: nome normalizzato + paese
    df_combined['__norm_nome'] = df_combined['nome'].apply(normalize_name)
    df_combined['__norm_paese'] = df_combined['paese'].str.lower().str.strip()
    
    # Trova duplicati per nome + paese
    duplicates_nome_paese = df_combined[df_combined.duplicated(subset=['__norm_nome', '__norm_paese'], keep=False)]
    
    if len(duplicates_nome_paese) > 0:
        print(f"\n🔍 PRIMA DEDUPLICAZIONE - Duplicati nome+paese ({len(duplicates_nome_paese)} righe):")
        for _, group in duplicates_nome_paese.groupby(['__norm_nome', '__norm_paese']):
            print(f"\n📋 Gruppo: '{group.iloc[0]['nome']}' in '{group.iloc[0]['paese']}':")
            for _, row in group.iterrows():
                print(f"   - {row['nome']} ({row['paese']}) - {row['tipo_missione']} - Fonte: {row['fonte']}")
    
    # Rimuovi duplicati nome+paese (mantieni il primo)
    df_after_first = df_combined.drop_duplicates(subset=['__norm_nome', '__norm_paese'], keep='first')
    removed_first = len(df_combined) - len(df_after_first)
    print(f"\n📊 Dopo PRIMA deduplicazione: {len(df_after_first)} missioni (rimosse: {removed_first})")
    
    # 6. SECONDA DEDUPLICAZIONE: solo nome
    duplicates_nome_only = df_after_first[df_after_first.duplicated(subset=['__norm_nome'], keep=False)]
    
    if len(duplicates_nome_only) > 0:
        print(f"\n🔍 SECONDA DEDUPLICAZIONE - Duplicati solo nome ({len(duplicates_nome_only)} righe):")
        for _, group in duplicates_nome_only.groupby(['__norm_nome']):
            print(f"\n📋 Gruppo per nome '{group.iloc[0]['nome']}':")
            for _, row in group.iterrows():
                print(f"   - {row['nome']} ({row['paese']}) - {row['tipo_missione']} - Fonte: {row['fonte']}")
    
    # Rimuovi duplicati solo nome (mantieni il primo)
    df_final = df_after_first.drop_duplicates(subset=['__norm_nome'], keep='first')
    removed_second = len(df_after_first) - len(df_final)
    print(f"\n📊 Dopo SECONDA deduplicazione: {len(df_final)} missioni (rimosse: {removed_second})")
    
    # 7. Totale missioni rimosse
    total_removed = removed_first + removed_second
    print(f"\n📊 Totale missioni rimosse: {total_removed}")
    print(f"📊 Missioni rimosse nella prima deduplicazione: {removed_first}")
    print(f"📊 Missioni rimosse nella seconda deduplicazione: {removed_second}")
    
    # 8. Verifica con la dashboard reale
    from dashboard.missioni_dashboard import load_data
    df_dashboard = load_data()
    print(f"\n📊 Dashboard reale: {len(df_dashboard)} missioni")
    
    # 9. Confronto
    expected_after_both = len(df_main) + len(new_missions) - total_removed
    print(f"📊 Atteso dopo entrambe le deduplicazioni: {expected_after_both}")
    print(f"📊 Dashboard reale: {len(df_dashboard)}")
    
    if expected_after_both == len(df_dashboard):
        print("✅ I numeri corrispondono perfettamente!")
    else:
        print(f"❌ Differenza: {expected_after_both - len(df_dashboard)}")
    
    # 10. Analisi dettagliata
    print(f"\n📋 ANALISI DETTAGLIATA:")
    print(f"   - File principale: {len(df_main)} missioni")
    print(f"   - Missioni aggiunte da Excel: {len(new_missions)}")
    print(f"   - Totale dopo aggiunta: {len(df_main) + len(new_missions)}")
    print(f"   - Missioni rimosse: {total_removed}")
    print(f"   - Risultato finale: {expected_after_both}")
    print(f"   - Dashboard reale: {len(df_dashboard)}")
    
    return total_removed, removed_first, removed_second

if __name__ == "__main__":
    total, first, second = final_dashboard_simulation() 