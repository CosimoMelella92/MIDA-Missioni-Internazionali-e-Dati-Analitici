#!/usr/bin/env python3
"""
Script per controllare duplicati e anomalie tra le missioni e sulle spese
"""
import pandas as pd
import difflib
from collections import defaultdict

# Carica i dati integrati
from src.missioni_dashboard import integrate_excel_data

def similar(a, b):
    return difflib.SequenceMatcher(None, a, b).ratio()

def find_potential_duplicates(df, threshold=0.85):
    """Trova missioni con nomi simili e stesso paese o data"""
    duplicates = []
    checked = set()
    for i, row1 in df.iterrows():
        name1 = str(row1['nome']).lower().strip()
        country1 = str(row1['paese']).lower().strip()
        start1 = str(row1['data_inizio'])[:10]
        for j, row2 in df.iterrows():
            if i >= j:
                continue
            name2 = str(row2['nome']).lower().strip()
            country2 = str(row2['paese']).lower().strip()
            start2 = str(row2['data_inizio'])[:10]
            key = tuple(sorted([i, j]))
            if key in checked:
                continue
            checked.add(key)
            sim = similar(name1, name2)
            if sim > threshold and (country1 == country2 or start1 == start2):
                duplicates.append((i, j, sim, name1, name2, country1, start1))
    return duplicates

def check_expenditure_anomalies(df):
    """Trova missioni con stesso nome/paese/anno ma spese diverse"""
    anomalies = []
    grouped = defaultdict(list)
    for idx, row in df.iterrows():
        key = (str(row['nome']).lower().strip(), str(row['paese']).lower().strip(), str(row['data_inizio'])[:4])
        grouped[key].append((idx, row['costo_totale']))
    for key, values in grouped.items():
        costs = set([v[1] for v in values if pd.notna(v[1])])
        if len(costs) > 1:
            anomalies.append((key, values))
    return anomalies

def main():
    print("=== Controllo Duplicati e Anomalie ===")
    # Carica solo i dati del file principale (senza integrazione Excel)
    df = pd.read_csv('data/processed/missioni_complete.csv')
    print(f"Totale missioni nel file principale: {len(df)}")
    
    # Mostra le organizzazioni presenti
    print("\n📊 Organizzazioni presenti:")
    org_stats = df['tipo_missione'].value_counts()
    for org, count in org_stats.items():
        print(f"  {org}: {count} missioni")

    # Controllo duplicati fuzzy
    print("\n--- Possibili Duplicati (nome simile, stesso paese/data) ---")
    dups = find_potential_duplicates(df)
    if dups:
        for i, j, sim, n1, n2, c, d in dups:
            print(f"  [{i},{j}] SIM={sim:.2f} | {n1} <-> {n2} | {c} | {d}")
    else:
        print("  Nessun duplicato rilevato con soglia alta.")

    # Controllo anomalie sulle spese
    print("\n--- Anomalie sulle Spese (stesso nome/paese/anno, costi diversi) ---")
    anomalies = check_expenditure_anomalies(df)
    if anomalies:
        for key, vals in anomalies:
            print(f"  Missione: {key[0]} | Paese: {key[1]} | Anno: {key[2]}")
            for idx, cost in vals:
                print(f"    - idx {idx}: costo_totale={cost}")
    else:
        print("  Nessuna anomalia sulle spese trovata.")

    print("\n=== Fine controllo ===")

if __name__ == "__main__":
    main() 