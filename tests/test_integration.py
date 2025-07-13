#!/usr/bin/env python3
"""
Test script per verificare l'integrazione dei nuovi dati Excel
"""

import pandas as pd
import sys
import os

# Aggiungi il percorso dashboard al path
sys.path.append('dashboard')

from dashboard.missioni_dashboard import integrate_excel_data, load_data

def test_integration():
    """Testa l'integrazione dei dati Excel"""
    print("=== Test Integrazione Dati Excel ===")
    
    # Carica i dati esistenti
    print("1. Caricamento dati esistenti...")
    df_existing = pd.read_csv('data/processed/missioni_complete.csv')
    print(f"   Record esistenti: {len(df_existing)}")
    
    # Test integrazione
    print("2. Test integrazione nuovi dati Excel...")
    df_integrated = integrate_excel_data(df_existing)
    print(f"   Record dopo integrazione: {len(df_integrated)}")
    
    # Verifica nuovi record
    new_records = len(df_integrated) - len(df_existing)
    print(f"   Nuovi record aggiunti: {new_records}")
    
    if new_records > 0:
        print("   ✅ Integrazione completata con successo!")
        
        # Mostra alcuni esempi di nuovi record
        print("\n3. Esempi di nuovi record:")
        new_data = df_integrated.tail(new_records)
        for idx, row in new_data.iterrows():
            print(f"   - {row['nome']} ({row['paese']}) - {row['tipo_missione']}")
    else:
        print("   ℹ️  Nessun nuovo record aggiunto (possibili duplicati evitati)")
    
    # Test funzione load_data completa
    print("\n4. Test funzione load_data completa...")
    try:
        df_complete = load_data()
        print(f"   Record totali: {len(df_complete)}")
        print("   ✅ Funzione load_data funziona correttamente!")
    except Exception as e:
        print(f"   ❌ Errore in load_data: {str(e)}")
    
    print("\n=== Test Completato ===")

if __name__ == "__main__":
    test_integration() 