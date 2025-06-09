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

class UNScraper(DocumentScraper):
    """Scraper per estrarre dati dal sito delle Nazioni Unite sulle missioni internazionali."""
    
    def __init__(self):
        super().__init__()
        self.fonte = "UN"
        
        # Carica la configurazione
        with open('config/config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        # Estrai le configurazioni specifiche per UN
        un_config = config['fonti_dati']['un']
        self.url_base = un_config['url_base']
        self.document_urls = un_config['document_urls']
        self.sections = un_config['sections']
        self.languages = un_config['languages']
        
        # Pattern regex per l'estrazione dei dati in inglese e francese
        self.patterns = {
            'en': {
                'nome_missione': r'Mission\s*:\s*([^\n]+)',
                'paese': r'Country\s*:\s*([^\n]+)',
                'data_inizio': r'Start\s*Date\s*:\s*(\d{1,2}/\d{1,2}/\d{4})',
                'data_fine': r'End\s*Date\s*:\s*(\d{1,2}/\d{1,2}/\d{4})',
                'personale_totale': r'Total\s*Personnel\s*:\s*(\d+)',
                'costo_totale': r'Total\s*Cost\s*:\s*€\s*([\d,.]+)',
                'tipo_missione': r'Mission\s*Type\s*:\s*([^\n]+)',
                'mandato': r'Mandate\s*:\s*([^\n]+)'
            },
            'fr': {
                'nome_missione': r'Mission\s*:\s*([^\n]+)',
                'paese': r'Pays\s*:\s*([^\n]+)',
                'data_inizio': r'Date\s*de\s*début\s*:\s*(\d{1,2}/\d{1,2}/\d{4})',
                'data_fine': r'Date\s*de\s*fin\s*:\s*(\d{1,2}/\d{1,2}/\d{4})',
                'personale_totale': r'Personnel\s*total\s*:\s*(\d+)',
                'costo_totale': r'Coût\s*total\s*:\s*€\s*([\d,.]+)',
                'tipo_missione': r'Type\s*de\s*mission\s*:\s*([^\n]+)',
                'mandato': r'Mandat\s*:\s*([^\n]+)'
            }
        }
        
        # Configura il logger
        self.logger = logging.getLogger(__name__)
        
    def estrai_dati(self) -> List[Dict]:
        """
        Estrae i dati dalle fonti ONU in inglese e francese
        """
        self.logger.info("Inizio estrazione dati ONU")
        dati = []
        
        try:
            # Estrai dati dai documenti per ogni lingua
            for lang in self.languages:
                for url in self.document_urls:
                    if f"/{lang}/" in url:
                        try:
                            testo = self._scarica_documento(url)
                            if testo:
                                dati_documento = self._estrai_dati_da_testo(testo, self.patterns[lang])
                                for dato in dati_documento:
                                    dato['lingua'] = lang
                                dati.extend(dati_documento)
                        except Exception as e:
                            self.logger.error(f"Errore nell'estrazione dati dal documento {url}: {str(e)}")
                    
            # Estrai dati dalle pagine web per ogni lingua
            for lang in self.languages:
                for section in self.sections:
                    try:
                        url = urljoin(self.url_base, f"un/{lang}/{section}")
                        html_content = self._scarica_pagina(url)
                        if html_content:
                            dati_pagina = self._estrai_dati_da_html(html_content, lang)
                            dati.extend(dati_pagina)
                    except Exception as e:
                        self.logger.error(f"Errore nell'estrazione dati dalla sezione {section} ({lang}): {str(e)}")
                    
            # Valida e pulisci i dati
            dati_validi = []
            for dato in dati:
                if self.valida_dati(dato):
                    dato_pulito = self.pulisci_dati(dato)
                    dato_pulito['fonte'] = self.fonte
                    dati_validi.append(dato_pulito)
                    
            return dati_validi
            
        except Exception as e:
            self.logger.error(f"Errore durante l'estrazione dati ONU: {str(e)}")
            raise
        
    def _estrai_dati_da_html(self, html_content: str, lang: str) -> List[Dict]:
        """
        Estrae i dati da una pagina HTML
        """
        dati = []
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Trova tutte le missioni nella pagina
        missioni = self._trova_missioni(soup)
        
        for missione in missioni:
            try:
                dati_missione = self._estrai_dati_missione(missione, lang)
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
        
    def _estrai_dati_missione(self, missione, lang: str) -> Dict:
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
            'lingua': lang,
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

    def _estrai_dati_da_testo(self, testo, patterns):
        """Estrae i dati dal testo utilizzando i pattern"""
        dati = []
        for pattern, regex in patterns.items():
            match = re.search(regex, testo)
            if match:
                dati.append({pattern: match.group(1)})
        return dati

    def _pulisci_dati(self, dati):
        """Pulisce i dati estratti"""
        # Implementa la logica di pulizia dei dati
        return dati

    def _salva_dati_raw(self, dati, nome_file):
        """Salva i dati estratti in un file raw"""
        # Implementa la logica di salvataggio dei dati in un file raw

    def _salva_dati_processati(self, df, nome_file):
        """Salva i dati processati in un file processato"""
        # Implementa la logica di salvataggio dei dati processati in un file processato

    def valida_dati(self, df):
        """Valida i dati estratti"""
        # Implementa la logica di validazione dei dati
        return True

    def _scarica_documento(self, url):
        """Scarica un documento dal sito"""
        # Implementa la logica di scarica di un documento dal sito
        return None

    def _scarica_pagina(self, url):
        """Scarica una pagina web"""
        # Implementa la logica di scarica di una pagina web
        return None

    def _make_request(self, url):
        """Effettua una richiesta HTTP al sito"""
        # Implementa la logica di effettuazione di una richiesta HTTP al sito
        return None 