#!/usr/bin/env python3
"""
Test script per verificare che tutti gli scrapers aggiornati 
salvino i file nella cartella centralizzata data/documents/
"""

import sys
import os
from pathlib import Path
import logging

# Aggiungi il path del progetto
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.scrapers.smart_document_fetcher import SmartDocumentFetcher
from core.scrapers.sitemap_document_collector import SitemapDocumentCollector
from core.scrapers.camera_scraper import CameraScraper
from core.scrapers.document_scraper import DocumentScraper
from core.scrapers.web_scraper import WebScraper
from core.scrapers.european_document_collector import EuropeanDocumentCollector
from core.scrapers.document_collector import DocumentCollector

def test_scrapers_configuration():
    """Testa che tutti gli scrapers siano configurati correttamente"""
    print("🧪 Test configurazione webscraper...")
    
    # Configurazione di test
    test_config = {
        'sitemap_urls': ['https://example.com/sitemap.xml'],
        'indice_urls': ['https://example.com/indice'],
        'allowed_extensions': ['.pdf', '.doc', '.docx'],
        'sleep_time': 1,
        'max_retries': 2,
        'timeout': 10
    }
    
    scrapers_to_test = [
        ("SmartDocumentFetcher", SmartDocumentFetcher),
        ("SitemapDocumentCollector", SitemapDocumentCollector),
        ("EuropeanDocumentCollector", EuropeanDocumentCollector),
        ("DocumentCollector", DocumentCollector)
    ]
    
    all_passed = True
    
    for name, scraper_class in scrapers_to_test:
        try:
            print(f"  📋 Testando {name}...")
            scraper = scraper_class(test_config)
            
            # Verifica che la cartella sia configurata correttamente
            if hasattr(scraper, 'documents_dir'):
                documents_dir = scraper.documents_dir
            elif hasattr(scraper, 'output_path'):
                documents_dir = Path(scraper.output_path)
            else:
                print(f"    ❌ {name}: Nessuna cartella documenti configurata")
                all_passed = False
                continue
            
            # Verifica che punti alla cartella centralizzata
            expected_path = Path('data/documents')
            if documents_dir.resolve() == expected_path.resolve():
                print(f"    ✅ {name}: Cartella configurata correttamente")
            else:
                print(f"    ❌ {name}: Cartella errata - {documents_dir} invece di {expected_path}")
                all_passed = False
                
        except Exception as e:
            print(f"    ❌ {name}: Errore durante il test - {e}")
            all_passed = False
    
    # Test per gli scraper che non ereditano da BaseCollector
    try:
        print("  📋 Testando CameraScraper...")
        camera_scraper = CameraScraper()
        if hasattr(camera_scraper, 'documents_dir'):
            documents_dir = camera_scraper.documents_dir
            expected_path = Path('data/documents')
            if documents_dir.resolve() == expected_path.resolve():
                print("    ✅ CameraScraper: Cartella configurata correttamente")
            else:
                print(f"    ❌ CameraScraper: Cartella errata - {documents_dir}")
                all_passed = False
        else:
            print("    ❌ CameraScraper: Nessuna cartella documenti configurata")
            all_passed = False
    except Exception as e:
        print(f"    ❌ CameraScraper: Errore durante il test - {e}")
        all_passed = False
    
    try:
        print("  📋 Testando DocumentScraper...")
        doc_scraper = DocumentScraper()
        if hasattr(doc_scraper, 'documents_dir'):
            documents_dir = doc_scraper.documents_dir
            expected_path = Path('data/documents')
            if documents_dir.resolve() == expected_path.resolve():
                print("    ✅ DocumentScraper: Cartella configurata correttamente")
            else:
                print(f"    ❌ DocumentScraper: Cartella errata - {documents_dir}")
                all_passed = False
        else:
            print("    ❌ DocumentScraper: Nessuna cartella documenti configurata")
            all_passed = False
    except Exception as e:
        print(f"    ❌ DocumentScraper: Errore durante il test - {e}")
        all_passed = False
    
    try:
        print("  📋 Testando WebScraper...")
        web_scraper = WebScraper("test", "https://example.com")
        if hasattr(web_scraper, 'documents_dir'):
            documents_dir = web_scraper.documents_dir
            expected_path = Path('data/documents')
            if documents_dir.resolve() == expected_path.resolve():
                print("    ✅ WebScraper: Cartella configurata correttamente")
            else:
                print(f"    ❌ WebScraper: Cartella errata - {documents_dir}")
                all_passed = False
        else:
            print("    ❌ WebScraper: Nessuna cartella documenti configurata")
            all_passed = False
    except Exception as e:
        print(f"    ❌ WebScraper: Errore durante il test - {e}")
        all_passed = False
    
    return all_passed

def test_documents_folder():
    """Verifica che la cartella documents esista e sia accessibile"""
    print("\n📁 Test cartella documents...")
    
    documents_dir = Path('data/documents')
    
    if not documents_dir.exists():
        print(f"  📁 Creando cartella {documents_dir}...")
        documents_dir.mkdir(parents=True, exist_ok=True)
        print("    ✅ Cartella creata")
    else:
        print("    ✅ Cartella già esistente")
    
    # Test scrittura
    test_file = documents_dir / "test_write.txt"
    try:
        test_file.write_text("test")
        test_file.unlink()  # Rimuovi il file di test
        print("    ✅ Permessi di scrittura OK")
        return True
    except Exception as e:
        print(f"    ❌ Errore permessi scrittura: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Test configurazione webscraper MIDA")
    print("=" * 50)
    
    # Test cartella documents
    folder_ok = test_documents_folder()
    
    # Test configurazione scrapers
    scrapers_ok = test_scrapers_configuration()
    
    print("\n" + "=" * 50)
    if folder_ok and scrapers_ok:
        print("✅ TUTTI I TEST SUPERATI!")
        print("🎉 Tutti i webscraper sono configurati correttamente")
        print("📁 I documenti verranno salvati in: data/documents/")
    else:
        print("❌ ALCUNI TEST FALLITI!")
        print("🔧 Controlla la configurazione degli scrapers")
    
    print("=" * 50) 