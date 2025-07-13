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
import os
import re
from urllib.parse import urljoin, urlparse

class WebScraper:
    def __init__(self, source_name: str, base_url: str, sections: list = None, config_path: str = "config/config.yaml"):
        """Inizializza lo scraper web con la configurazione"""
        self.source_name = source_name
        self.base_url = base_url
        self.sections = sections or []
        self.config = self._carica_configurazione(config_path)
        self.setup_logging()
        self.session = self._setup_session()
        
        # Aggiornato per salvare nella cartella centralizzata
        self.documents_dir = Path('data/documents')
        self.documents_dir.mkdir(parents=True, exist_ok=True)
        
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

    def _scarica_documento(self, url: str) -> Optional[str]:
        """Scarica un documento e lo salva nella cartella centralizzata"""
        try:
            # Assicurati che l'URL sia assoluto
            if not url.startswith(('http://', 'https://')):
                url = urljoin(self.base_url, url)
            
            # Genera un nome file più descrittivo basato sull'URL originale
            original_filename = os.path.basename(urlparse(url).path)
            if not original_filename or original_filename == '':
                original_filename = 'document'
            
            # Rimuovi caratteri problematici dal nome file
            safe_filename = re.sub(r'[<>:"/\\|?*]', '_', original_filename)
            
            # Se il file esiste già, aggiungi un suffisso
            base_name = os.path.splitext(safe_filename)[0]
            counter = 1
            final_filename = safe_filename
            
            while (self.documents_dir / final_filename).exists():
                name, ext_part = os.path.splitext(safe_filename)
                final_filename = f"{name}_{counter}{ext_part}"
                counter += 1
            
            filepath = self.documents_dir / final_filename
            
            # Controlla se il file esiste già
            if filepath.exists():
                self.logger.info(f"Documento già presente in data/documents/: {final_filename}")
                return str(filepath)
            
            # Scarica il documento
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Salva il file nella cartella centralizzata
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            self.logger.info(f"Documento scaricato in data/documents/: {final_filename}")
            return str(filepath)
            
        except Exception as e:
            self.logger.error(f"Errore nel download del documento {url}: {str(e)}")
            return None

    def _trova_documenti(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Trova tutti i link a documenti PDF e DOCX nella pagina"""
        documents = []
        
        # Cerca link a documenti
        for link in soup.find_all('a', href=True):
            href = link['href']
            if any(href.lower().endswith(ext) for ext in ['.pdf', '.doc', '.docx']):
                # Assicurati che l'URL sia assoluto
                if not href.startswith(('http://', 'https://')):
                    href = urljoin(base_url, href)
                
                documents.append({
                    'url': href,
                    'text': link.get_text(strip=True),
                    'title': link.get('title', ''),
                    'source': self.source_name
                })
        
        return documents

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