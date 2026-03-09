#!/usr/bin/env python3
"""
Ultra Fast Document Extractor - Modalità Ultra Veloce
Disabilita completamente NLP e usa solo regex veloci
"""

import os
import subprocess
import sys
from pathlib import Path


def main():
    """Avvia il Document Extractor in modalità ultra veloce"""

    # Imposta variabili d'ambiente per performance massime
    env = os.environ.copy()
    env['DISABLE_SPACY'] = 'true'  # Disabilita spaCy
    env['FAST_MODE'] = 'true'      # Modalità veloce
    env['FLASK_APP'] = str(Path(__file__).parent / "core" / "pdf_extractor" / "web_interface" / "app.py")
    env['FLASK_ENV'] = 'development'

    print("🚀 Avvio Document Extractor - Modalità Ultra Veloce")
    print("⚡ spaCy disabilitato per performance massime")
    print("⚡ Modalità veloce attivata")
    print("📁 Directory corrente:", Path(__file__).parent.absolute())
    print("🌐 Interfaccia: http://localhost:5000")
    print("⏹️  Premi Ctrl+C per fermare")

    try:
        # Avvia Flask con le ottimizzazioni
        subprocess.run([
            sys.executable, "-m", "flask", "run",
            "--host", "0.0.0.0",
            "--port", "5000",
            "--debug"
        ], env=env, cwd=Path(__file__).parent)
    except KeyboardInterrupt:
        print("\n👋 Document Extractor fermato dall'utente")
    except Exception as e:
        print(f"❌ Errore nell'esecuzione del Document Extractor: {e}")

if __name__ == "__main__":
    main()
