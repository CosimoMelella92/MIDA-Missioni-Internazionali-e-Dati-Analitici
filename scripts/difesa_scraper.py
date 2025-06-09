import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import datetime
from typing import Dict, List
import logging
from web_scraper import WebScraper
import json
import requests
import yaml
from base_scraper import BaseScraper

class DifesaScraper(WebScraper):
    """Scraper per estrarre dati dal sito del Ministero della Difesa sulle missioni internazionali."""
    
    def __init__(self):
        """Inizializza lo scraper della Difesa."""
        with open('config/config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        super().__init__(
            source_name="difesa",
            base_url=config['fonti_dati']['difesa']['base_url'],
            sections=config['fonti_dati']['difesa']['sections']
        )
        self.logger = logging.getLogger(__name__)
        self.fonte = "difesa"
        self.url_base = config['fonti_dati']['difesa']['base_url']
        self.sezioni = config['fonti_dati']['difesa']['sections']

    def estrai_dati(self) -> pd.DataFrame:
        """Estrae dati dalle pagine del Ministero della Difesa"""
        self.logger.info("Inizio estrazione dati dal Ministero della Difesa")
        dati = []
        
        try:
            # Estrai dati dalle sezioni configurate
            for sezione in self.sezioni:
                url = f"{self.url_base}/{sezione}"
                self.logger.info(f"Estrazione dati da: {url}")
                
                try:
                    response = self.session.get(url, timeout=self.config['parametri_scraping']['timeout'])
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Trova tutte le missioni
                    missioni = self._trova_missioni(soup)
                    
                    for missione in missioni:
                        try:
                            dati_missione = self._estrai_dati_missione(missione)
                            if dati_missione:
                                dati.append(dati_missione)
                        except Exception as e:
                            self.logger.error(f"Errore nell'estrazione dati dalla missione: {str(e)}")
                            continue
                            
                except Exception as e:
                    self.logger.error(f"Errore nell'accesso alla sezione {sezione}: {str(e)}")
                    continue
                    
                self._attendi()
                
        except Exception as e:
            self.logger.error(f"Errore generale nell'estrazione dati dal Ministero della Difesa: {str(e)}")
            
        if not dati:
            self.logger.error("Nessun dato estratto dal Ministero della Difesa")
            return pd.DataFrame()
            
        df = pd.DataFrame(dati)
        self._salva_dati_raw(dati, "difesa_raw")
        df = self.pulisci_dati(df)
        
        if self.valida_dati(df):
            self._salva_dati_processati(df, "difesa_processed")
            return df
        else:
            self.logger.error("Validazione dati fallita")
            return pd.DataFrame()

    def _trova_missioni(self, soup: BeautifulSoup) -> List[BeautifulSoup]:
        """Trova tutte le missioni nella pagina"""
        # Cerca i div che contengono le missioni
        return soup.find_all('div', class_='missione')

    def _estrai_dati_missione(self, missione: BeautifulSoup) -> Dict:
        """Estrae i dati da una singola missione"""
        try:
            # Estrai il nome della missione
            nome_div = missione.find('div', class_='nome-missione')
            nome_missione = nome_div.text.strip() if nome_div else ""
            
            # Estrai il paese
            paese_div = missione.find('div', class_='paese')
            paese = paese_div.text.strip() if paese_div else ""
            
            # Estrai le date
            date_div = missione.find('div', class_='date')
            data_inizio = ""
            data_fine = ""
            if date_div:
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})', date_div.text)
                if date_match:
                    data_inizio = date_match.group(1)
                    data_fine = date_match.group(2)
            
            # Estrai il personale
            personale_div = missione.find('div', class_='personale')
            personale_totale = 0
            if personale_div:
                personale_match = re.search(r'(\d+)', personale_div.text)
                if personale_match:
                    personale_totale = int(personale_match.group(1))
            
            # Estrai il costo
            costo_div = missione.find('div', class_='costo')
            costo_totale = 0.0
            if costo_div:
                costo_match = re.search(r'€\s*([\d.,]+)', costo_div.text)
                if costo_match:
                    costo = costo_match.group(1).replace('.', '').replace(',', '.')
                    costo_totale = float(costo)
            
            # Estrai il tipo di missione
            tipo_div = missione.find('div', class_='tipo')
            tipo_missione = tipo_div.text.strip() if tipo_div else ""
            
            # Estrai il mandato
            mandato_div = missione.find('div', class_='mandato')
            mandato = mandato_div.text.strip() if mandato_div else ""
            
            # Estrai le note
            note_div = missione.find('div', class_='note')
            note = note_div.text.strip() if note_div else ""
            
            # Estrai il link
            link = missione.find('a', href=True)
            link_documento = link['href'] if link else ""
            
            return {
                'nome_missione': nome_missione,
                'paese': paese,
                'data_inizio': data_inizio,
                'data_fine': data_fine,
                'personale_totale': personale_totale,
                'costo_totale': costo_totale,
                'fonte': self.fonte,
                'ultimo_aggiornamento': datetime.now().strftime('%Y-%m-%d'),
                'tipo_missione': tipo_missione,
                'mandato': mandato,
                'note': note,
                'link_documento': link_documento
            }
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati dalla missione: {str(e)}")
            return None 