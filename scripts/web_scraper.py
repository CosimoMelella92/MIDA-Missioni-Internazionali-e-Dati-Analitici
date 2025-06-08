import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
import time
import random
from typing import Dict, List, Optional
import json
import yaml

class WebScraper:
    def __init__(self, config_path: str = "config/config.yaml"):
        """Inizializza lo scraper web con la configurazione"""
        self.config = self._carica_configurazione(config_path)
        self.setup_logging()
        self.session = self._setup_session()
        
    def _carica_configurazione(self, config_path: str) -> Dict:
        """Carica il file di configurazione YAML"""
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise Exception(f"Errore nel caricamento della configurazione: {str(e)}")

    def setup_logging(self):
        """Configura il sistema di logging"""
        log_dir = Path(self.config['percorsi']['logs'])
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / f'scraper_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def _setup_session(self) -> requests.Session:
        """Configura una sessione HTTP con i parametri appropriati"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': self.config['parametri_scraping']['user_agent']
        })
        return session

    def _attendi(self):
        """Attende un tempo casuale tra le richieste"""
        delay = random.uniform(
            self.config['parametri_scraping']['delay_min'],
            self.config['parametri_scraping']['delay_max']
        )
        time.sleep(delay)

    def _salva_dati_raw(self, dati: Dict, nome_file: str):
        """Salva i dati grezzi in formato JSON"""
        raw_dir = Path(self.config['percorsi']['raw_data'])
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = raw_dir / f"{nome_file}_{datetime.now().strftime('%Y%m%d')}.json"
        pd.DataFrame(dati).to_json(file_path, orient='records', indent=2)
        self.logger.info(f"Dati grezzi salvati in: {file_path}")

    def _salva_dati_processati(self, df: pd.DataFrame, nome_file: str):
        """Salva i dati processati in formato CSV"""
        processed_dir = Path(self.config['percorsi']['processed_data'])
        processed_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = processed_dir / f"{nome_file}_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8')
        self.logger.info(f"Dati processati salvati in: {file_path}")

    def valida_dati(self, df: pd.DataFrame) -> bool:
        """Valida che il DataFrame contenga tutte le colonne obbligatorie"""
        colonne_obbligatorie = set(self.config['struttura_dati']['colonne_obbligatorie'])
        colonne_presenti = set(df.columns)
        
        if not colonne_obbligatorie.issubset(colonne_presenti):
            mancanti = colonne_obbligatorie - colonne_presenti
            self.logger.error(f"Colonne obbligatorie mancanti: {mancanti}")
            return False
        return True

    def pulisci_dati(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pulisce e standardizza i dati"""
        # Rimuovi spazi extra
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()
        
        # Converti date
        for col in ['data_inizio', 'data_fine', 'ultimo_aggiornamento']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Converti numeri
        for col in ['personale_totale', 'costo_totale']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

    def estrai_dati(self) -> pd.DataFrame:
        """Metodo da implementare nelle classi figlie"""
        raise NotImplementedError("Le classi figlie devono implementare questo metodo") 