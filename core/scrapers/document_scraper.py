import requests
import pdfplumber
import pandas as pd
import docx
import openpyxl
from bs4 import BeautifulSoup
from datetime import datetime
from base_scraper import BaseScraper
from pathlib import Path
import re
import time
from typing import Dict, List, Optional, Union
import logging
import json
import os
import yaml
from urllib.parse import urljoin
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class DocumentScraper(BaseScraper):
    """Classe base per l'estrazione di dati da documenti in vari formati."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        super().__init__(config_path)
        self.raw_data_dir = Path(self.config['percorsi']['raw_data'])
        self.max_retries = self.config['parametri_scraping'].get('retry_attempts', 3)
        self.timeout = self.config['parametri_scraping'].get('timeout', 30)
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            'User-Agent': self.config['parametri_scraping']['user_agent'],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
    def _carica_config(self) -> Dict:
        """Carica la configurazione dal file YAML"""
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'config.yaml')
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
            
    def _make_request(self, url: str, method: str = 'GET', **kwargs) -> Optional[requests.Response]:
        """Effettua una richiesta HTTP con gestione degli errori e retry"""
        for attempt in range(self.max_retries):
            try:
                # Aggiungi un delay casuale tra le richieste
                time.sleep(random.uniform(1, 3))
                
                response = self.session.request(
                    method,
                    url,
                    timeout=self.timeout,
                    **kwargs
                )
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Tentativo {attempt + 1} fallito per {url}: {str(e)}")
                if attempt < self.max_retries - 1:
                    # Aumenta il delay esponenzialmente
                    time.sleep(self.config['parametri_scraping']['retry_delay'] * (2 ** attempt))
                else:
                    raise
                    
    def _scarica_documento(self, url: str) -> Optional[str]:
        """Scarica un documento e ne estrae il testo"""
        try:
            # Assicurati che l'URL sia assoluto
            if not url.startswith(('http://', 'https://')):
                url = urljoin(self.config['fonti_dati']['url_base'], url)
                
            response = self._make_request(url)
            if not response:
                return None
            
            content_type = response.headers.get('content-type', '').lower()
            
            if 'application/pdf' in content_type:
                return self._estrai_testo_da_pdf(response.content)
            elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type:
                return self._estrai_testo_da_docx(response.content)
            elif 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type:
                return self._estrai_testo_da_xlsx(response.content)
            else:
                return response.text
                
        except Exception as e:
            self.logger.error(f"Errore nel download del documento {url}: {str(e)}")
            return None
            
    def _scarica_pagina(self, url: str) -> Optional[BeautifulSoup]:
        """Scarica una pagina web e la converte in BeautifulSoup"""
        response = self._make_request(url)
        if not response:
            return None
            
        try:
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            logging.error(f"Errore nella conversione della pagina {url} in BeautifulSoup: {str(e)}")
            return None
            
    def _estrai_testo_da_pdf(self, content: bytes) -> Optional[str]:
        """Estrae il testo da un PDF"""
        try:
            with pdfplumber.open(content) as pdf:
                text = ''
                for page in pdf.pages:
                    text += page.extract_text() + '\n'
                return text
        except Exception as e:
            logging.error(f"Errore nell'estrazione del testo dal PDF: {str(e)}")
            return None
            
    def _estrai_testo_da_docx(self, content: bytes) -> Optional[str]:
        """Estrae il testo da un DOCX"""
        try:
            from docx import Document
            from io import BytesIO
            doc = Document(BytesIO(content))
            return '\n'.join([paragraph.text for paragraph in doc.paragraphs])
        except Exception as e:
            logging.error(f"Errore nell'estrazione del testo dal DOCX: {str(e)}")
            return None
            
    def _estrai_testo_da_xlsx(self, content: bytes) -> Optional[str]:
        """Estrae il testo da un XLSX"""
        try:
            from io import BytesIO
            df = pd.read_excel(BytesIO(content))
            return df.to_string()
        except Exception as e:
            logging.error(f"Errore nell'estrazione del testo dall'XLSX: {str(e)}")
            return None
            
    def _estrai_testo_da_documento(self, file_path: str) -> str:
        """Estrae il testo da un documento in base alla sua estensione."""
        estensione = os.path.splitext(file_path)[1].lower()
        
        if estensione == '.pdf':
            return self._estrai_testo_da_pdf(file_path)
        elif estensione == '.docx':
            return self._estrai_testo_da_docx(file_path)
        elif estensione in ['.xlsx', '.xls']:
            return self._estrai_testo_da_xlsx(file_path)
        else:
            self.logger.error(f"Formato documento non supportato: {estensione}")
            return ""

    def _pulisci_dati_missione(self, missione: dict) -> dict:
        """Pulisce e standardizza i dati estratti."""
        # Rimuovi spazi extra
        for key, value in missione.items():
            if isinstance(value, str):
                missione[key] = value.strip()
        
        # Converti date
        for campo in ['data_inizio', 'data_fine']:
            if missione.get(campo):
                try:
                    data = datetime.strptime(missione[campo], '%d/%m/%Y')
                    missione[campo] = data.strftime('%Y-%m-%d')
                except ValueError:
                    missione[campo] = ""
        
        # Converti numeri
        if missione.get('personale_totale'):
            try:
                missione['personale_totale'] = int(re.sub(r'[^\d]', '', str(missione['personale_totale'])))
            except ValueError:
                missione['personale_totale'] = 0
                
        if missione.get('costo_totale'):
            try:
                # Rimuovi punti e sostituisci virgole con punti
                costo = str(missione['costo_totale']).replace('.', '').replace(',', '.')
                missione['costo_totale'] = float(costo)
            except ValueError:
                missione['costo_totale'] = 0.0
        
        return missione

    def _estrai_dati_da_testo(self, testo: str, patterns: Dict[str, str]) -> Dict:
        """Estrae i dati dal testo usando i pattern regex forniti."""
        dati = {}
        for campo, pattern in patterns.items():
            match = re.search(pattern, testo, re.IGNORECASE | re.MULTILINE)
            dati[campo] = match.group(1).strip() if match else ""
        return dati

    def estrai_dati(self) -> pd.DataFrame:
        """Metodo da implementare nelle classi figlie."""
        raise NotImplementedError("Le classi figlie devono implementare questo metodo") 