#!/usr/bin/env python3
"""
Script per avviare il sistema di estrazione PDF
Avvia l'interfaccia web su porta 5000 (separata dalla dashboard Streamlit)
"""

import subprocess
import sys
import os
from pathlib import Path

def main():
    """Avvia il sistema di estrazione PDF"""
    
    # Verifica che siamo nella directory corretta
    current_dir = Path.cwd()
    app_file = current_dir / "core" / "pdf_extractor" / "web_interface" / "app.py"
    
    if not app_file.exists():
        print(f"❌ File app non trovato: {app_file}")
        print("Assicurati di essere nella directory root del progetto")
        return 1
    
    print("🚀 Avvio PDF Extractor - Sistema di Estrazione Intelligente")
    print(f"📁 Directory corrente: {current_dir}")
    print(f"📊 File app: {app_file}")
    print("\n🌐 L'interfaccia sarà disponibile su: http://localhost:5000")
    print("📊 Dashboard Streamlit: http://localhost:8501")
    print("⏹️  Premi Ctrl+C per fermare il sistema\n")
    
    try:
        # Avvia l'app Flask
        subprocess.run([
            sys.executable, "-m", "flask", "run", 
            "--app", str(app_file),
            "--host", "0.0.0.0",
            "--port", "5000",
            "--debug"
        ], check=True)
        
    except KeyboardInterrupt:
        print("\n👋 PDF Extractor fermato dall'utente")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"❌ Errore nell'esecuzione del PDF Extractor: {e}")
        return 1
    except Exception as e:
        print(f"❌ Errore imprevisto: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 