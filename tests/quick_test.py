#!/usr/bin/env python3
"""
Test Rapido per Document Extractor Ultra Veloce
"""

import requests
import time
import json

def quick_test():
    """Test rapido delle performance"""
    print("🚀 Test Rapido Document Extractor Ultra Veloce")
    print("=" * 50)
    
    start_time = time.time()
    
    try:
        # Test di connessione
        print("📡 Test connessione...")
        response = requests.get('http://localhost:5000/', timeout=5)
        if response.status_code == 200:
            print("✅ Server raggiungibile")
        else:
            print("❌ Server non risponde correttamente")
            return
        
        # Test estrazione veloce
        print("⚡ Test estrazione ultra-veloce...")
        start_extraction = time.time()
        
        response = requests.post(
            'http://localhost:5000/extract',
            json={'action': 'extract_all'},
            timeout=60  # 1 minuto di timeout
        )
        
        extraction_time = time.time() - start_extraction
        
        if response.status_code == 200:
            data = response.json()
            total_time = time.time() - start_time
            
            print("✅ Estrazione completata!")
            print(f"⏱️  Tempo totale: {total_time:.1f} secondi")
            print(f"⚡ Tempo estrazione: {extraction_time:.1f} secondi")
            print(f"📊 File processati: {data.get('total_files', 0)}")
            print(f"📊 Estrazioni riuscite: {data.get('successful_extractions', 0)}")
            
            # Performance analysis
            if extraction_time < 10:
                print("🎉 ECCELLENTE: Estrazione ultra-veloce!")
            elif extraction_time < 30:
                print("✅ BUONO: Estrazione veloce")
            elif extraction_time < 60:
                print("⚠️  ACCETTABILE: Estrazione media")
            else:
                print("❌ LENTO: Estrazione ancora troppo lenta")
                
        else:
            print(f"❌ Errore nell'estrazione: {response.status_code}")
            print(f"Risposta: {response.text}")
            
    except requests.exceptions.Timeout:
        print("⏰ Timeout: L'estrazione sta impiegando troppo tempo")
    except requests.exceptions.ConnectionError:
        print("❌ Errore di connessione: Server non raggiungibile")
    except Exception as e:
        print(f"❌ Errore generico: {e}")

if __name__ == "__main__":
    quick_test() 