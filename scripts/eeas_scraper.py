import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import datetime
from typing import Dict, List
import logging
from .document_scraper import DocumentScraper
import json
import requests
import yaml
import os
from urllib.parse import urljoin

class EeasScraper(DocumentScraper):
    """Scraper per estrarre dati dal sito dell'EEAS sulle missioni internazionali."""
    
    def __init__(self):
        super().__init__()
        self.fonte = "EEAS"
        
        # Carica la configurazione
        with open('config/config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        self.url_base = config['fonti_dati']['eeas']['url_base']
        self.document_urls = config['fonti_dati']['eeas']['document_urls']
        self.sections = config['fonti_dati']['eeas']['sections']
        
        # Pattern regex per l'estrazione dei dati
        self.patterns = {
            'nome_missione': r'Mission:\s*(.*?)(?:\n|$)',
            'paese': r'Country:\s*(.*?)(?:\n|$)',
            'data_inizio': r'Start Date:\s*(\d{1,2}/\d{1,2}/\d{4})',
            'data_fine': r'End Date:\s*(\d{1,2}/\d{1,2}/\d{4})',
            'personale_totale': r'Total Personnel:\s*(\d+)',
            'costo_totale': r'Total Cost:\s*€\s*([\d,.]+)',
            'tipo_missione': r'Mission Type:\s*(.*?)(?:\n|$)',
            'mandato': r'Mandate:\s*(.*?)(?:\n|$)'
        }
        
        self.logger = logging.getLogger(__name__)
        
    def estrai_dati(self) -> List[Dict]:
        """
        Estrae i dati dalle pagine web e dai documenti EEAS
        """
        self.logger.info("Inizio estrazione dati EEAS")
        dati = []
        
        try:
            # Estrai dati dai documenti
            for url in self.document_urls:
                self.logger.info(f"Estrazione da documento: {url}")
                try:
                    doc_data = self._scarica_documento(url)
                    if doc_data:
                        dati.extend(self._estrai_dati_da_testo(doc_data))
                except Exception as e:
                    self.logger.error(f"Errore nell'estrazione dal documento {url}: {str(e)}")
            
            # Estrai dati dalle pagine web
            for section in self.sections:
                self.logger.info(f"Estrazione da {section}")
                try:
                    page_data = self._scarica_pagina(f"{self.url_base}/{section}")
                    if page_data:
                        dati.extend(self._estrai_dati_da_html(page_data))
                except Exception as e:
                    self.logger.error(f"Errore nell'estrazione dalla sezione {section}: {str(e)}")
            
            # Pulisci e valida i dati
            dati = [self.pulisci_dati(d) for d in dati]
            dati = [d for d in dati if self.valida_dati(d)]
            
            if not dati:
                self.logger.warning("Nessun dato estratto da EEAS")
            else:
                self.logger.info(f"Estratti {len(dati)} record da EEAS")
                
        except Exception as e:
            self.logger.error(f"Errore durante l'estrazione dati EEAS: {str(e)}")
            raise
            
        return dati
        
    def _estrai_dati_da_html(self, html_content: str) -> List[Dict]:
        """
        Estrae i dati da una pagina HTML
        """
        dati = []
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Trova tutte le missioni nella pagina
        missioni = self._trova_missioni(soup)
        
        for missione in missioni:
            try:
                dati_missione = self._estrai_dati_missione(missione)
                if dati_missione:
                    dati.append(dati_missione)
            except Exception as e:
                self.logger.error(f"Errore nell'estrazione dati missione: {str(e)}")
                
        return dati
        
    def _trova_missioni(self, soup: BeautifulSoup) -> List:
        """
        Trova tutte le missioni in una pagina
        """
        return soup.find_all('div', class_='mission')
        
    def _estrai_dati_missione(self, missione) -> Dict:
        """
        Estrae i dati da una singola missione
        """
        dati = {
            'nome_missione': None,
            'paese': None,
            'data_inizio': None,
            'data_fine': None,
            'personale_totale': None,
            'costo_totale': None,
            'tipo_missione': None,
            'mandato': None,
            'fonte': self.fonte,
            'ultimo_aggiornamento': None,
            'link_documento': None
        }
        
        # Estrai il nome della missione
        nome_elem = missione.find('h2')
        if nome_elem:
            dati['nome_missione'] = nome_elem.text.strip()
            
        # Estrai il paese
        paese_elem = missione.find('div', class_='country')
        if paese_elem:
            dati['paese'] = paese_elem.text.strip()
            
        # Estrai le date
        date_elem = missione.find('div', class_='dates')
        if date_elem:
            date_text = date_elem.text.strip()
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})', date_text)
            if date_match:
                dati['data_inizio'] = date_match.group(1)
                dati['data_fine'] = date_match.group(2)
                
        # Estrai il personale
        personale_elem = missione.find('div', class_='personnel')
        if personale_elem:
            personale_text = personale_elem.text.strip()
            personale_match = re.search(r'(\d+)', personale_text)
            if personale_match:
                dati['personale_totale'] = int(personale_match.group(1))
                
        # Estrai il costo
        costo_elem = missione.find('div', class_='cost')
        if costo_elem:
            costo_text = costo_elem.text.strip()
            costo_match = re.search(r'€\s*([\d,.]+)', costo_text)
            if costo_match:
                dati['costo_totale'] = float(costo_match.group(1).replace(',', ''))
                
        # Estrai il tipo di missione
        tipo_elem = missione.find('div', class_='type')
        if tipo_elem:
            dati['tipo_missione'] = tipo_elem.text.strip()
            
        # Estrai il mandato
        mandato_elem = missione.find('div', class_='mandate')
        if mandato_elem:
            dati['mandato'] = mandato_elem.text.strip()
            
        # Estrai il link al documento
        link_elem = missione.find('a', href=True)
        if link_elem:
            dati['link_documento'] = urljoin(self.url_base, link_elem['href'])
            
        # Rimuovi i valori None
        return {k: v for k, v in dati.items() if v is not None}

    def _salva_dati_raw(self, dati, nome_file):
        """Salva i dati estratti in formato JSON"""
        file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', f"{nome_file}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(dati, f)

    def _salva_dati_processati(self, df, nome_file):
        """Salva i dati processati in formato CSV"""
        file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', f"{nome_file}.csv")
        df.to_csv(file_path, index=False)

    def _pulisci_dati(self, dati):
        """Pulisce i dati estratti"""
        # Implementa la logica di pulizia dei dati
        return dati

    def _estrai_testo_da_documento(self, local_path):
        """Estrae il testo da un documento"""
        with open(local_path, 'r', encoding='utf-8') as f:
            return f.read()

    def _estrai_dati_da_testo(self, testo, patterns):
        """Estrae i dati dall'estratto di testo"""
        dati = []
        for pattern in patterns.values():
            match = re.search(pattern, testo)
            if match:
                dati.append({pattern.split(':')[0]: match.group(1)})
        return dati

    def _estrai_testo(self, soup, tag, class_=None):
        """Estrae il testo da un tag HTML"""
        element = soup.find(tag, class_=class_)
        return element.text.strip() if element else None

    def _estrai_link(self, soup):
        """Estrae il link da un tag HTML"""
        link = soup.find('a', href=True)
        if link:
            href = link['href']
            if not href.startswith('http'):
                href = f"{self.url_base}/{href}"
            return href
        return None

    def _scarica_documento(self, url):
        """Scarica un documento dal sito"""
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.error(f"Errore nel download del documento {url}: {str(e)}")
            return None

    def _scarica_pagina(self, url):
        """Scarica una pagina web"""
        try:
            response = requests.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            logging.error(f"Errore nel download della pagina {url}: {str(e)}")
            return None

    def valida_dati(self, dati):
        """Valida i dati estratti"""
        # Implementa la logica di validazione dei dati
        return True

    def _make_request(self, url):
        """Effettua una richiesta HTTP"""
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except Exception as e:
            logging.error(f"Errore nella richiesta HTTP: {str(e)}")
            return None 