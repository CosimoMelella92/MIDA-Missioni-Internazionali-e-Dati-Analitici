#!/usr/bin/env python3
"""
Document Extractor - Sistema di Estrazione Intelligente
Supporta PDF e Word (DOCX) files
"""

import os
import sys
import subprocess
import signal
import time
from pathlib import Path

def main():
    """Avvia il Document Extractor"""
    
    # Ottieni il percorso del file corrente
    current_dir = Path(__file__).parent.absolute()
    
    print("🚀 Avvio Document Extractor - Sistema di Estrazione Intelligente")
    print(f"📁 Directory corrente: {current_dir}")
    
    # Percorso dell'app Flask
    app_path = current_dir / "core" / "pdf_extractor" / "web_interface" / "app.py"
    print(f"📊 File app: {app_path}")
    
    # Verifica che il file esista
    if not app_path.exists():
        print(f"❌ File app non trovato: {app_path}")
        return 1
    
    print("🌐 L'interfaccia sarà disponibile su: http://localhost:5000")
    print("📊 Dashboard Streamlit: http://localhost:8501")
    print("⏹️  Premi Ctrl+C per fermare il sistema")
    
    # Imposta la variabile d'ambiente per Flask
    env = os.environ.copy()
    env['FLASK_APP'] = str(app_path)
    env['FLASK_ENV'] = 'development'
    
    try:
        # Avvia Flask con l'app specificata
        process = subprocess.Popen([
            sys.executable, '-m', 'flask', 'run',
            '--host', '0.0.0.0',
            '--port', '5000',
            '--debug'
        ], env=env, cwd=current_dir)
        
        # Attendi che il processo termini
        process.wait()
        
    except KeyboardInterrupt:
        print("\n👋 Document Extractor fermato dall'utente")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"❌ Errore nell'esecuzione del Document Extractor: {e}")
        return 1
    except Exception as e:
        print(f"❌ Errore imprevisto: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 