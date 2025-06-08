import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import datetime
from typing import Dict, List
import logging
from scripts.web_scraper import WebScraper
import json
import requests
import yaml

class SenatoScraper(WebScraper):
    """Scraper per le missioni del Senato"""
    
    def __init__(self):
        """Inizializza lo scraper del Senato."""
        with open('config/config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        super().__init__(
            source_name="senato",
            base_url=config['senato']['base_url'],
            sections=config['senato']['sections']
        )
        self.logger = logging.getLogger(__name__)
        self.fonte = "senato"
        self.url_base = self.config['fonti_dati']['senato']['url_base']
        self.sezioni = self.config['fonti_dati']['senato']['sezioni']

    def estrai_dati(self) -> pd.DataFrame:
        """Estrae dati dalle pagine del Senato"""
        self.logger.info("Inizio estrazione dati dal Senato")
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
                    
                    # Trova tutti i documenti relativi alle missioni
                    documenti = self._trova_documenti(soup)
                    
                    for doc in documenti:
                        try:
                            missione = self._estrai_dati_documento(doc)
                            if missione:
                                dati.append(missione)
                        except Exception as e:
                            self.logger.error(f"Errore nell'estrazione dati dal documento: {str(e)}")
                            continue
                            
                except Exception as e:
                    self.logger.error(f"Errore nell'accesso alla sezione {sezione}: {str(e)}")
                    continue
                    
                self._attendi()
                
        except Exception as e:
            self.logger.error(f"Errore generale nell'estrazione dati dal Senato: {str(e)}")
            
        if not dati:
            self.logger.error("Nessun dato estratto dal Senato")
            return pd.DataFrame()
            
        df = pd.DataFrame(dati)
        self._salva_dati_raw(dati, "senato_raw")
        df = self.pulisci_dati(df)
        
        if self.valida_dati(df):
            self._salva_dati_processati(df, "senato_processed")
            return df
        else:
            self.logger.error("Validazione dati fallita")
            return pd.DataFrame()

    def _trova_documenti(self, soup: BeautifulSoup) -> List[BeautifulSoup]:
        """Trova tutti i documenti relativi alle missioni"""
        # Cerca i div che contengono i documenti
        return soup.find_all('div', class_='documento-missione')

    def _estrai_dati_documento(self, doc: BeautifulSoup) -> Dict:
        """Estrae i dati da un singolo documento"""
        try:
            # Estrai il titolo che contiene il nome della missione
            titolo = doc.find('h2')
            nome_missione = titolo.text.strip() if titolo else ""
            
            # Estrai il paese dalla descrizione
            descrizione = doc.find('div', class_='descrizione')
            paese = ""
            if descrizione:
                paese_match = re.search(r'(?:in|presso|nel|nella)\s+([A-Za-z\s\-]+)', descrizione.text)
                if paese_match:
                    paese = paese_match.group(1).strip()
            
            # Estrai le date
            date_div = doc.find('div', class_='date')
            data_inizio = ""
            data_fine = ""
            if date_div:
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})', date_div.text)
                if date_match:
                    data_inizio = date_match.group(1)
                    data_fine = date_match.group(2)
            
            # Estrai il personale
            personale_div = doc.find('div', class_='personale')
            personale_totale = 0
            if personale_div:
                personale_match = re.search(r'(\d+)', personale_div.text)
                if personale_match:
                    personale_totale = int(personale_match.group(1))
            
            # Estrai il costo
            costo_div = doc.find('div', class_='costo')
            costo_totale = 0.0
            if costo_div:
                costo_match = re.search(r'€\s*([\d.,]+)', costo_div.text)
                if costo_match:
                    costo = costo_match.group(1).replace('.', '').replace(',', '.')
                    costo_totale = float(costo)
            
            return {
                'nome_missione': nome_missione,
                'paese': paese,
                'data_inizio': data_inizio,
                'data_fine': data_fine,
                'personale_totale': personale_totale,
                'costo_totale': costo_totale,
                'fonte': self.fonte,
                'ultimo_aggiornamento': datetime.now().strftime('%Y-%m-%d'),
                'tipo_missione': self._estrai_tipo_missione(doc),
                'mandato': self._estrai_mandato(doc),
                'note': self._estrai_note(doc),
                'link_documento': self._estrai_link(doc)
            }
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati dal documento: {str(e)}")
            return None

    def _estrai_tipo_missione(self, doc: BeautifulSoup) -> str:
        """Estrae il tipo di missione"""
        tipo_div = doc.find('div', class_='tipo-missione')
        return tipo_div.text.strip() if tipo_div else ""

    def _estrai_mandato(self, doc: BeautifulSoup) -> str:
        """Estrae il mandato della missione"""
        mandato_div = doc.find('div', class_='mandato')
        return mandato_div.text.strip() if mandato_div else ""

    def _estrai_note(self, doc: BeautifulSoup) -> str:
        """Estrae le note aggiuntive"""
        note_div = doc.find('div', class_='note')
        return note_div.text.strip() if note_div else ""

    def _estrai_link(self, doc: BeautifulSoup) -> str:
        """Estrae il link al documento"""
        link = doc.find('a', href=True)
        return link['href'] if link else "" 