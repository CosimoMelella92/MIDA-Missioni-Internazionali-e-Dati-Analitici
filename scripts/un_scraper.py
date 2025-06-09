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

class UnScraper(DocumentScraper):
    """Scraper per estrarre dati dal sito delle Nazioni Unite sulle missioni internazionali."""
    
    def __init__(self):
        super().__init__()
        self.fonte = "ONU"
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'config.yaml')
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        self.url_base = config['fonti_dati']['un']['url_base']
        self.document_urls = config['fonti_dati']['un']['document_urls']
        self.sections = config['fonti_dati']['un']['sections']
        
        # Definizione dei pattern regex per l'estrazione dei dati
        self.patterns = {
            'nome_missione': r'Missione:\s*(.*?)(?:\n|$)',
            'paese': r'Paese:\s*(.*?)(?:\n|$)',
            'data_inizio': r'Data inizio:\s*(\d{2}/\d{2}/\d{4})',
            'data_fine': r'Data fine:\s*(\d{2}/\d{2}/\d{4})',
            'personale_totale': r'Personale totale:\s*(\d+)',
            'costo_totale': r'Costo totale:\s*([\d.,]+)\s*€',
            'tipo_missione': r'Tipo missione:\s*(.*?)(?:\n|$)',
            'mandato': r'Mandato:\s*(.*?)(?:\n|$)'
        }

    def estrai_dati(self):
        """Estrae i dati dalle missioni ONU"""
        try:
            logging.info(f"Inizio estrazione dati {self.fonte}")
            dati = []
            
            # Estrazione dati dai documenti
            for url in self.document_urls:
                try:
                    logging.info(f"Estrazione da {url}")
                    testo = self._scarica_documento(url)
                    if testo:
                        dati_documento = self._estrai_dati_da_testo(testo)
                        if dati_documento:
                            dati.extend(dati_documento)
                except Exception as e:
                    logging.error(f"Errore nell'estrazione dal documento {url}: {str(e)}")
            
            # Estrazione dati dalle pagine web
            for section in self.sections:
                try:
                    url = f"{self.url_base}/{section}"
                    logging.info(f"Estrazione da {section}")
                    soup = self._scarica_pagina(url)
                    if soup:
                        missioni = self._trova_missioni(soup)
                        for missione in missioni:
                            dati_missione = self._estrai_dati_missione(missione)
                            if dati_missione:
                                dati.append(dati_missione)
                except Exception as e:
                    logging.error(f"Errore nell'estrazione dalla sezione {section}: {str(e)}")
            
            if not dati:
                logging.error(f"Nessun dato estratto da {self.fonte}")
                return None
                
            return self._pulisci_dati(dati)
            
        except Exception as e:
            logging.error(f"Errore durante l'estrazione dati {self.fonte}: {str(e)}")
            raise

    def _trova_missioni(self, soup):
        """Trova tutte le missioni nella pagina"""
        return soup.find_all('div', class_='mission')
        
    def _estrai_dati_missione(self, missione):
        """Estrae i dati da una singola missione"""
        try:
            dati = {
                'nome_missione': self._estrai_testo(missione, 'h2'),
                'paese': self._estrai_testo(missione, 'span', class_='country'),
                'data_inizio': self._estrai_testo(missione, 'span', class_='start-date'),
                'data_fine': self._estrai_testo(missione, 'span', class_='end-date'),
                'personale_totale': self._estrai_testo(missione, 'span', class_='personnel'),
                'costo_totale': self._estrai_testo(missione, 'span', class_='cost'),
                'tipo_missione': self._estrai_testo(missione, 'span', class_='type'),
                'mandato': self._estrai_testo(missione, 'div', class_='mandate'),
                'fonte': self.fonte,
                'ultimo_aggiornamento': self._estrai_testo(missione, 'span', class_='last-update'),
                'link_documento': self._estrai_link(missione)
            }
            
            # Rimuovi i valori None
            return {k: v for k, v in dati.items() if v is not None}
            
        except Exception as e:
            logging.error(f"Errore nell'estrazione dati missione: {str(e)}")
            return None

    def _estrai_dati_da_testo(self, testo, patterns):
        """Estrae i dati dal testo utilizzando i pattern"""
        dati = []
        for pattern in patterns:
            match = re.search(pattern, testo)
            if match:
                dati.append({pattern: match.group(1)})
        return dati

    def _estrai_testo(self, soup, tag, class_=None):
        """Estrae il testo di un elemento HTML"""
        element = soup.find(tag, class_=class_)
        return element.text.strip() if element else None

    def _estrai_link(self, soup):
        """Estrae il link di un elemento HTML"""
        link = soup.find('a', href=True)
        if link:
            href = link['href']
            if not href.startswith('http'):
                href = f"{self.url_base}/{href}"
            return href
        return None

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

    def _estrai_dati_da_testo(self, testo):
        """Estrae i dati dal testo utilizzando i pattern"""
        return self._estrai_dati_da_testo(testo, self.patterns)

    def _estrai_dati_missione(self, missione):
        """Estrae i dati dettagliati di una missione"""
        return self._estrai_dati_missione(missione) 