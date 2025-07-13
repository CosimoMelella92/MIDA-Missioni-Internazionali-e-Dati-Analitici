#!/usr/bin/env python3
"""
Test script per verificare l'estrazione dei dati
"""

import requests
import json
import time

def test_extraction():
    """Test dell'estrazione dei dati"""
    print("🧪 Test dell'estrazione dei dati...")
    
    try:
        # Test dell'estrazione
        print("📤 Invio richiesta di estrazione...")
        response = requests.post(
            'http://localhost:5000/extract',
            json={'action': 'extract_all'},
            timeout=300  # 5 minuti di timeout
        )
        
        if response.status_code == 200:
            print("✅ Estrazione completata con successo!")
            data = response.json()
            
            # Mostra statistiche
            print(f"📊 File processati: {data.get('total_files', 0)}")
            print(f"📊 Estrazioni riuscite: {data.get('successful_extractions', 0)}")
            print(f"📊 Missioni trovate: {data.get('total_missions', 0)}")
            print(f"📊 Personale totale: {data.get('total_personnel', 0)}")
            print(f"📊 Costi totali: €{data.get('total_costs', 0):,.0f}")
            print(f"📊 Paesi trovati: {len(data.get('countries_found', []))}")
            print(f"📊 Organizzazioni trovate: {len(data.get('organizations_found', []))}")
            
            return True
        else:
            print(f"❌ Errore nell'estrazione: {response.status_code}")
            print(f"Risposta: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Impossibile connettersi al server. Verifica che sia in esecuzione su http://localhost:5000")
        return False
    except requests.exceptions.Timeout:
        print("❌ Timeout durante l'estrazione. Il processo potrebbe richiedere più tempo.")
        return False
    except Exception as e:
        print(f"❌ Errore imprevisto: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Avvio test dell'estrazione...")
    print("⏳ Attendo che il server sia pronto...")
    time.sleep(2)
    
    success = test_extraction()
    
    if success:
        print("\n🎉 Test completato con successo!")
        print("🌐 Puoi ora accedere all'interfaccia web su: http://localhost:5000")
    else:
        print("\n💥 Test fallito. Controlla i log del server per maggiori dettagli.") 