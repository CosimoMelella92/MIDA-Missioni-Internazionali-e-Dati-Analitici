#!/usr/bin/env python3
"""
Script per avviare il sistema di estrazione PDF
Avvia l'interfaccia web su porta 5000 (separata dalla dashboard Streamlit)
"""

import os
import sys
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
        # Aggiungi il percorso dell'app al sys.path
        app_dir = app_file.parent
        sys.path.insert(0, str(app_dir))

        # Imposta la variabile d'ambiente per Flask
        os.environ['FLASK_APP'] = str(app_file)
        os.environ['FLASK_ENV'] = 'development'

        # Importa e avvia l'app Flask direttamente
        from app import app

        print("✅ Flask app caricata correttamente")
        print("🌐 Avvio server su http://localhost:5000")

        app.run(
            host='0.0.0.0',
            port=5000,
            debug=True,
            use_reloader=False  # Evita problemi di reloading
        )

    except ImportError as e:
        print(f"❌ Errore di importazione: {e}")
        print("Assicurati che Flask sia installato: pip install flask")
        return 1
    except KeyboardInterrupt:
        print("\n👋 PDF Extractor fermato dall'utente")
        return 0
    except Exception as e:
        print(f"❌ Errore imprevisto: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
