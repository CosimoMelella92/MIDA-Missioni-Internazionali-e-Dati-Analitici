#!/usr/bin/env python3
"""
Fast Document Extractor - Modalità Veloce
Disabilita spaCy per estrazione rapida
"""

import os
import subprocess
import sys
from pathlib import Path


def main():
    """Avvia il Document Extractor in modalità veloce"""

    # Imposta variabili d'ambiente per performance
    env = os.environ.copy()
    env['DISABLE_SPACY'] = 'true'  # Disabilita spaCy
    env['FLASK_APP'] = str(Path(__file__).parent / "web_interface" / "app.py")
    env['FLASK_ENV'] = 'development'

    print("🚀 Avvio Document Extractor - Modalità Veloce")
    print("⚡ spaCy disabilitato per performance")
    print("📁 Directory corrente:", Path(__file__).parent.absolute())
    print("🌐 Interfaccia: http://localhost:5000")
    print("⏹️  Premi Ctrl+C per fermare")

    try:
        # Avvia Flask con configurazione veloce
        process = subprocess.Popen([
            sys.executable, '-m', 'flask', 'run',
            '--host', '0.0.0.0',
            '--port', '5000',
            '--debug'
        ], env=env, cwd=Path(__file__).parent.parent.parent)

        process.wait()

    except KeyboardInterrupt:
        print("\n👋 Document Extractor fermato")
        return 0
    except Exception as e:
        print(f"❌ Errore: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
