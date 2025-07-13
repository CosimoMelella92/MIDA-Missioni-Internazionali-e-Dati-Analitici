import pandas as pd
import json
import os
from datetime import datetime
import logging
from typing import List, Dict
import hashlib

class DataMerger:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def _genera_id_univoco(self, dato: Dict) -> str:
        """
        Genera un ID univoco per un dato basato sui suoi valori
        """
        # Crea una stringa con i valori rilevanti
        valori = [
            str(dato.get('nome_missione', '')),
            str(dato.get('paese', '')),
            str(dato.get('data_inizio', '')),
            str(dato.get('fonte', ''))
        ]
        stringa = '|'.join(valori)
        
        # Genera un hash MD5
        return hashlib.md5(stringa.encode()).hexdigest()
        
    def _carica_dati(self, directory: str) -> List[Dict]:
        """
        Carica i dati da tutti i file JSON in una directory
        """
        dati = []
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                try:
                    with open(os.path.join(directory, filename), 'r', encoding='utf-8') as f:
                        dati.extend(json.load(f))
                except Exception as e:
                    self.logger.error(f"Errore nel caricamento del file {filename}: {str(e)}")
        return dati
        
    def _normalizza_dati(self, dati: List[Dict]) -> List[Dict]:
        """
        Normalizza i dati per garantire consistenza
        """
        dati_normalizzati = []
        for dato in dati:
            # Aggiungi ID univoco
            dato['id'] = self._genera_id_univoco(dato)
            
            # Normalizza date
            for campo in ['data_inizio', 'data_fine', 'data_estrazione']:
                if campo in dato:
                    try:
                        if isinstance(dato[campo], str):
                            dato[campo] = pd.to_datetime(dato[campo]).isoformat()
                    except:
                        pass
                        
            # Normalizza valori numerici
            for campo in ['personale_totale', 'costo_totale']:
                if campo in dato:
                    try:
                        if isinstance(dato[campo], str):
                            dato[campo] = float(dato[campo].replace(',', ''))
                    except:
                        pass
                        
            dati_normalizzati.append(dato)
            
        return dati_normalizzati
        
    def _deduplica_dati(self, dati: List[Dict]) -> List[Dict]:
        """
        Rimuove i duplicati basandosi sull'ID univoco
        """
        # Crea un dizionario con ID come chiave
        dati_dict = {}
        for dato in dati:
            id_dato = dato['id']
            if id_dato not in dati_dict:
                dati_dict[id_dato] = dato
            else:
                # Se esiste già, aggiorna con i dati più recenti
                if pd.to_datetime(dato.get('data_estrazione', '')) > \
                   pd.to_datetime(dati_dict[id_dato].get('data_estrazione', '')):
                    dati_dict[id_dato] = dato
                    
        return list(dati_dict.values())
        
    def _arricchisci_dati(self, dati: List[Dict]) -> List[Dict]:
        """
        Arricchisce i dati con informazioni aggiuntive
        """
        for dato in dati:
            # Aggiungi metadati
            dato['ultimo_aggiornamento'] = datetime.now().isoformat()
            
            # Aggiungi flag di validazione
            dato['validato'] = all(
                campo in dato for campo in [
                    'nome_missione',
                    'paese',
                    'data_inizio',
                    'fonte'
                ]
            )
            
        return dati
        
    def merge(self, input_dir: str, output_file: str):
        """
        Esegue il merge dei dati da diverse fonti
        """
        try:
            # Carica i dati
            self.logger.info("Caricamento dati...")
            dati = self._carica_dati(input_dir)
            
            # Normalizza i dati
            self.logger.info("Normalizzazione dati...")
            dati = self._normalizza_dati(dati)
            
            # Deduplica i dati
            self.logger.info("Deduplicazione dati...")
            dati = self._deduplica_dati(dati)
            
            # Arricchisci i dati
            self.logger.info("Arricchimento dati...")
            dati = self._arricchisci_dati(dati)
            
            # Salva i dati
            self.logger.info(f"Salvataggio dati in {output_file}...")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(dati, f, ensure_ascii=False, indent=2)
                
            self.logger.info(f"Merge completato. Totale record: {len(dati)}")
            
        except Exception as e:
            self.logger.error(f"Errore durante il merge dei dati: {str(e)}")
            raise

if __name__ == "__main__":
    # Configura il logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Esegui il merge
    merger = DataMerger()
    merger.merge('data/raw', 'data/merged/missioni.json') 