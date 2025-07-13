#!/usr/bin/env python3
"""
Script per eseguire la dashboard Streamlit delle missioni internazionali
"""

import subprocess
import sys
import os
from pathlib import Path

def main():
    """Esegue la dashboard Streamlit"""
    
    # Verifica che siamo nella directory corretta
    current_dir = Path.cwd()
    dashboard_file = current_dir / "dashboard" / "missioni_dashboard.py"
    
    if not dashboard_file.exists():
        print(f"❌ File dashboard non trovato: {dashboard_file}")
        print("Assicurati di essere nella directory root del progetto")
        return 1
    
    print("🚀 Avvio dashboard MIDA - Missioni Internazionali e Dati Analitici")
    print(f"📁 Directory corrente: {current_dir}")
    print(f"📊 File dashboard: {dashboard_file}")
    print("\n🌐 La dashboard sarà disponibile su: http://localhost:8501")
    print("⏹️  Premi Ctrl+C per fermare la dashboard\n")
    
    try:
        # Esegui la dashboard Streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            str(dashboard_file),
            "--server.port", "8501",
            "--server.address", "localhost",
            "--browser.gatherUsageStats", "false"
        ], check=True)
        
    except KeyboardInterrupt:
        print("\n👋 Dashboard fermata dall'utente")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"❌ Errore nell'esecuzione della dashboard: {e}")
        return 1
    except Exception as e:
        print(f"❌ Errore imprevisto: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 