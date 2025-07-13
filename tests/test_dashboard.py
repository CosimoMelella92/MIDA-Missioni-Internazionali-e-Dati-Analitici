#!/usr/bin/env python3
"""
Test script per verificare il funzionamento della dashboard MIDA
"""

import sys
import os
import pandas as pd
from pathlib import Path

def test_data_files():
    """Testa la presenza e validità dei file di dati"""
    print("🔍 Test file di dati...")
    
    # Verifica file dati
    data_files = [
        "data/processed/missioni_complete.csv",
        "data/processed/missioni.csv"
    ]
    
    for file_path in data_files:
        if Path(file_path).exists():
            print(f"✅ {file_path} - TROVATO")
            try:
                df = pd.read_csv(file_path)
                print(f"   📊 Righe: {len(df)}")
                print(f"   📋 Colonne: {list(df.columns)}")
            except Exception as e:
                print(f"   ❌ Errore lettura: {e}")
        else:
            print(f"❌ {file_path} - NON TROVATO")
    
    return True

def test_dependencies():
    """Testa le dipendenze Python"""
    print("\n🔍 Test dipendenze...")
    
    required_packages = [
        "streamlit",
        "pandas", 
        "plotly",
        "numpy",
        "yaml"
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} - INSTALLATO")
        except ImportError:
            print(f"❌ {package} - MANCANTE")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n⚠️  Pacchetti mancanti: {', '.join(missing_packages)}")
        print("Installa con: pip install " + " ".join(missing_packages))
        return False
    
    return True

def test_dashboard_file():
    """Testa il file della dashboard"""
    print("\n🔍 Test file dashboard...")
    
    dashboard_file = "dashboard/missioni_dashboard.py"
    
    if Path(dashboard_file).exists():
        print(f"✅ {dashboard_file} - TROVATO")
        
        # Verifica sintassi
        try:
            with open(dashboard_file, 'r', encoding='utf-8') as f:
                content = f.read()
            compile(content, dashboard_file, 'exec')
            print("   ✅ Sintassi Python - VALIDA")
        except SyntaxError as e:
            print(f"   ❌ Errore sintassi: {e}")
            return False
    else:
        print(f"❌ {dashboard_file} - NON TROVATO")
        return False
    
    return True

def test_config_files():
    """Testa i file di configurazione"""
    print("\n🔍 Test file configurazione...")
    
    config_files = [
        ".streamlit/config.toml",
        "config/dashboard_config.yaml"
    ]
    
    for config_file in config_files:
        if Path(config_file).exists():
            print(f"✅ {config_file} - TROVATO")
        else:
            print(f"❌ {config_file} - NON TROVATO")
    
    return True

def test_sample_data():
    """Testa la creazione di dati di esempio"""
    print("\n🔍 Test dati di esempio...")
    
    try:
        # Crea dati di esempio
        sample_data = {
            'nome': ['Test Mission 1', 'Test Mission 2'],
            'paese': ['Test Country 1', 'Test Country 2'],
            'regione': ['Test Region', 'Test Region'],
            'sub_regione': ['Test Sub-Region', 'Test Sub-Region'],
            'tipo_partecipazione': ['civmil', 'mil'],
            'data_inizio': ['2020-01-01', '2021-01-01'],
            'data_fine': ['2024-12-31', '2024-12-31'],
            'personale_militare': [100, 200],
            'personale_civile': [50, 0],
            'personale_totale': [150, 200],
            'costo_totale': [1000000, 2000000],
            'tipo_missione': ['ONU', 'NATO']
        }
        
        df = pd.DataFrame(sample_data)
        print(f"✅ Dati di esempio creati - {len(df)} righe")
        
        # Test conversione date
        df['data_inizio'] = pd.to_datetime(df['data_inizio'])
        df['data_fine'] = pd.to_datetime(df['data_fine'])
        print("✅ Conversione date - OK")
        
        # Test calcoli
        total_personnel = df['personale_totale'].sum()
        total_cost = df['costo_totale'].sum()
        print(f"✅ Calcoli - Personale totale: {total_personnel}, Costo totale: {total_cost}")
        
    except Exception as e:
        print(f"❌ Errore test dati: {e}")
        return False
    
    return True

def main():
    """Esegue tutti i test"""
    print("🚀 Test Dashboard MIDA - Missioni Internazionali e Dati Analitici")
    print("=" * 70)
    
    tests = [
        test_data_files,
        test_dependencies,
        test_dashboard_file,
        test_config_files,
        test_sample_data
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Errore durante il test: {e}")
            results.append(False)
    
    print("\n" + "=" * 70)
    print("📊 RISULTATI TEST")
    print("=" * 70)
    
    passed = sum(results)
    total = len(results)
    
    print(f"✅ Test superati: {passed}/{total}")
    print(f"❌ Test falliti: {total - passed}/{total}")
    
    if passed == total:
        print("\n🎉 TUTTI I TEST SUPERATI!")
        print("La dashboard è pronta per essere eseguita.")
        print("\nPer avviare la dashboard:")
        print("  python run_dashboard.py")
        print("  oppure")
        print("  streamlit run dashboard/missioni_dashboard.py")
    else:
        print("\n⚠️  ALCUNI TEST FALLITI")
        print("Risolvi i problemi prima di eseguire la dashboard.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 