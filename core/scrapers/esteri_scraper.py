import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import datetime
from typing import Dict, List
import logging
from document_scraper import DocumentScraper
import json
import requests
import yaml

class EsteriScraper(DocumentScraper):
    """Scraper per estrarre dati dal sito del Ministero degli Esteri sulle missioni internazionali."""
    
    def __init__(self):
        super().__init__()
        self.fonte = "esteri"
        self.url_base = self.config['fonti_dati']['esteri']['url_base']
        self.document_urls = self.config['fonti_dati']['esteri'].get('document_urls', [])
        self.sections = self.config['fonti_dati']['esteri'].get('sections', [])
        
        # Pattern per l'estrazione dei dati
        self.patterns = {
            'nome_missione': r'(?:Missione|Operazione)\s+([A-Za-z\s\-]+)',
            'paese': r'(?:in|presso|nel|nella)\s+([A-Za-z\s\-]+)',
            'data_inizio': r'(?:dal|a partire dal)\s+(\d{1,2}/\d{1,2}/\d{4})',
            'data_fine': r'(?:al|fino al)\s+(\d{1,2}/\d{1,2}/\d{4})',
            'personale_totale': r'(?:personale|effettivi|militari)\s*(?:totale)?\s*:\s*(\d+)',
            'costo_totale': r'(?:costo|spesa)\s*(?:totale)?\s*:\s*€\s*([\d.,]+)',
            'tipo_missione': r'(?:tipo|natura)\s*(?:della missione)?\s*:\s*([A-Za-z\s\-]+)',
            'mandato': r'(?:mandato|risoluzione)\s*(?:ONU)?\s*:\s*([A-Za-z0-9\s\-]+)'
        }

    def estrai_dati(self) -> pd.DataFrame:
        """Estrae i dati dalle pagine del Ministero degli Esteri"""
        self.logger.info("Inizio estrazione dati dal Ministero degli Esteri")
        dati = []
        
        # Estrai dati dai documenti
        for url in self.document_urls:
            try:
                self.logger.info(f"Tentativo di download documento da: {url}")
                local_path = self._scarica_documento(url)
                if local_path:
                    testo = self._estrai_testo_da_documento(local_path)
                    if testo:
                        dati_estratti = self._estrai_dati_da_testo(testo, self.patterns)
                        dati_estratti['fonte'] = self.fonte
                        dati_estratti['ultimo_aggiornamento'] = datetime.now().strftime('%Y-%m-%d')
                        dati_estratti['link_documento'] = url
                        dati.append(dati_estratti)
            except Exception as e:
                self.logger.error(f"Errore nell'elaborazione del documento {url}: {str(e)}")
                continue
        
        # Estrai dati dalle pagine web
        for section in self.sections:
            try:
                url = f"{self.url_base}/{section}"
                self.logger.info(f"Estrazione dati da: {url}")
                
                response = self._make_request(url)
                if not response:
                    continue
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                missioni = self._trova_missioni(soup)
                
                for missione in missioni:
                    try:
                        dati_missione = self._estrai_dati_missione(missione)
                        if dati_missione:
                            dati_missione['fonte'] = self.fonte
                            dati_missione['ultimo_aggiornamento'] = datetime.now().strftime('%Y-%m-%d')
                            dati.append(dati_missione)
                    except Exception as e:
                        self.logger.error(f"Errore nell'estrazione dati dalla missione: {str(e)}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"Errore nell'accesso alla sezione {section}: {str(e)}")
                continue
                
        if not dati:
            self.logger.error("Nessun dato estratto dal Ministero degli Esteri")
            return pd.DataFrame()
            
        df = pd.DataFrame(dati)
        self._salva_dati_raw(dati, "esteri_raw")
        df = self.pulisci_dati(df)
        if self.valida_dati(df):
            self._salva_dati_processati(df, "esteri_processed")
            return df
        else:
            self.logger.error("Validazione dati fallita")
            return pd.DataFrame()

    def _trova_missioni(self, soup: BeautifulSoup) -> List[BeautifulSoup]:
        """Trova tutte le missioni nella pagina"""
        return soup.find_all('div', class_='missione')

    def _estrai_dati_missione(self, missione: BeautifulSoup) -> Dict:
        """Estrae i dati dettagliati di una missione"""
        try:
            dati = {}
            
            # Estrai il nome della missione
            nome = missione.find('h3')
            if nome:
                dati['nome_missione'] = nome.text.strip()
            
            # Estrai il paese
            paese = missione.find('div', class_='location')
            if paese:
                dati['paese'] = paese.text.strip()
            
            # Estrai le date
            date = missione.find('div', class_='dates')
            if date:
                date_text = date.text.strip()
                data_inizio = re.search(r'(?:dal|a partire dal)\s+(\d{1,2}/\d{1,2}/\d{4})', date_text)
                data_fine = re.search(r'(?:al|fino al)\s+(\d{1,2}/\d{1,2}/\d{4})', date_text)
                if data_inizio:
                    dati['data_inizio'] = data_inizio.group(1)
                if data_fine:
                    dati['data_fine'] = data_fine.group(1)
            
            # Estrai il personale
            personale = missione.find('div', class_='personnel')
            if personale:
                personale_text = personale.text.strip()
                match = re.search(r'(\d+)', personale_text)
                if match:
                    dati['personale_totale'] = int(match.group(1))
            
            # Estrai il costo
            costo = missione.find('div', class_='budget')
            if costo:
                costo_text = costo.text.strip()
                match = re.search(r'€\s*([\d.,]+)', costo_text)
                if match:
                    dati['costo_totale'] = match.group(1)
            
            # Estrai il tipo di missione
            tipo = missione.find('div', class_='type')
            if tipo:
                dati['tipo_missione'] = tipo.text.strip()
            
            # Estrai il mandato
            mandato = missione.find('div', class_='mandate')
            if mandato:
                dati['mandato'] = mandato.text.strip()
            
            # Estrai il link al documento
            link = missione.find('a', href=True)
            if link:
                href = link['href']
                if not href.startswith('http'):
                    href = f"{self.url_base}/{href}"
                dati['link_documento'] = href
            
            return dati
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati dalla missione: {str(e)}")
            return None 