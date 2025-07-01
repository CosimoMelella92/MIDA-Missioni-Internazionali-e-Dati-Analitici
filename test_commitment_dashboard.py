#!/usr/bin/env python3
"""
Test script per verificare la dashboard con commitment
"""

import pandas as pd
import sys
import os

# Aggiungi il percorso src al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_data_loading():
    """Test del caricamento dati con commitment"""
    try:
        # Importa la funzione di caricamento
        from missioni_dashboard import load_data
        
        # Carica i dati
        df = load_data()
        
        if df is None:
            print("❌ Errore: Impossibile caricare i dati")
            return False
        
        print(f"✅ Dati caricati con successo: {len(df)} righe")
        
        # Verifica che la colonna commitment esista
        if 'commitment' in df.columns:
            print("✅ Colonna 'commitment' presente")
            print(f"   Valori unici: {df['commitment'].unique()}")
        else:
            print("❌ Colonna 'commitment' mancante")
            return False
        
        # Verifica le date
        min_year = df['data_inizio'].dt.year.min()
        print(f"✅ Date iniziano dal {min_year}")
        
        # Verifica missioni prima del 1991 (solo se sono ancora attive)
        old_missions = df[df['data_inizio'].dt.year < 1991]
        if len(old_missions) > 0:
            print(f"ℹ️  Trovate {len(old_missions)} missioni che iniziano prima del 1991:")
            for _, mission in old_missions.iterrows():
                is_active = mission['data_fine'] > pd.Timestamp.now()
                status = "🟢 ATTIVA" if is_active else "🔴 TERMINATA"
                print(f"   - {mission['nome']}: {mission['data_inizio'].year} ({status})")
        else:
            print("✅ Nessuna missione prima del 1991")
        
        return True
        
    except Exception as e:
        print(f"❌ Errore durante il test: {str(e)}")
        return False

def test_commitment_analysis():
    """Test dell'analisi commitment"""
    try:
        from missioni_dashboard import create_commitment_analysis, load_data
        
        df = load_data()
        if df is None:
            return False
        
        # Test dell'analisi commitment
        commitment_stats = create_commitment_analysis(df)
        
        print(f"✅ Analisi commitment completata: {len(commitment_stats)} categorie")
        print("   Risultati:")
        for _, row in commitment_stats.iterrows():
            print(f"   - {row['Tipo Commitment']}: {row['Numero Missioni']} missioni, "
                  f"{row['Personale Totale']:,.0f} personale, {row['Costo Totale']:,.0f} €")
        
        return True
        
    except Exception as e:
        print(f"❌ Errore nell'analisi commitment: {str(e)}")
        return False

def main():
    """Esegue tutti i test"""
    print("🧪 Test Dashboard con Commitment")
    print("=" * 50)
    
    # Test 1: Caricamento dati
    print("\n1. Test caricamento dati...")
    if not test_data_loading():
        print("❌ Test caricamento dati fallito")
        return
    
    # Test 2: Analisi commitment
    print("\n2. Test analisi commitment...")
    if not test_commitment_analysis():
        print("❌ Test analisi commitment fallito")
        return
    
    print("\n✅ Tutti i test completati con successo!")
    print("\n🚀 Per avviare la dashboard:")
    print("   streamlit run src/missioni_dashboard.py")

if __name__ == "__main__":
    main() 