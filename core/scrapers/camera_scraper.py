import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from document_scraper import DocumentScraper
from pathlib import Path
import re
import time
from typing import Dict, List
import logging
import json

class CameraScraper(DocumentScraper):
    def __init__(self):
        super().__init__()
        self.fonte = "camera_deputati"
        self.url_base = self.config['fonti_dati']['camera_deputati']['url_base']
        self.document_urls = self.config['fonti_dati']['camera_deputati'].get('document_urls', [])
        
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
        self.logger.info("Inizio estrazione dati documenti Camera dei Deputati")
        dati = []
        
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
                
        if not dati:
            self.logger.error("Nessun dato estratto dai documenti")
            return pd.DataFrame()
            
        df = pd.DataFrame(dati)
        self._salva_dati_raw(dati, f"camera_documenti_raw")
        df = self.pulisci_dati(df)
        if self.valida_dati(df):
            self._salva_dati_processati(df, "camera_documenti_processed")
            return df
        else:
            self.logger.error("Validazione dati fallita")
            return pd.DataFrame()

    def _scarica_documento(self, url: str) -> str:
        """Scarica il documento e restituisce il path locale"""
        nome_file = url.split('/')[-1]
        local_path = self.raw_data_dir / nome_file
        
        if local_path.exists():
            self.logger.info(f"Documento già presente: {local_path}")
            return str(local_path)
            
        for tentativo in range(self.max_retries):
            try:
                self.logger.info(f"Tentativo {tentativo + 1} di download da {url}")
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                    
                self.logger.info(f"Documento scaricato con successo: {local_path}")
                return str(local_path)
                
            except requests.Timeout:
                self.logger.warning(f"Timeout al tentativo {tentativo + 1}")
                if tentativo < self.max_retries - 1:
                    time.sleep(2 ** tentativo)  # Exponential backoff
                continue
                
            except requests.RequestException as e:
                self.logger.error(f"Errore nella richiesta HTTP: {str(e)}")
                if tentativo < self.max_retries - 1:
                    time.sleep(2 ** tentativo)
                continue
                
            except Exception as e:
                self.logger.error(f"Errore imprevisto: {str(e)}")
                return None
                
        self.logger.error(f"Impossibile scaricare il documento dopo {self.max_retries} tentativi")
        return None

    def _estrai_testo_da_documento(self, doc_path: str) -> str:
        """Estrai testo dal documento"""
        try:
            with open(doc_path, 'r') as f:
                return f.read()
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione del testo dal documento {doc_path}: {str(e)}")
            return None

    def _estrai_dati_da_testo(self, testo: str, patterns: dict) -> dict:
        """Estrai dati strutturati dal testo"""
        missione = {}
        for campo, pattern in patterns.items():
            match = re.search(pattern, testo, re.IGNORECASE)
            if match:
                missione[campo] = match.group(1).strip()
            else:
                missione[campo] = ""
        
        # Aggiungi campi obbligatori mancanti
        missione.update({
            'fonte': self.fonte,
            'ultimo_aggiornamento': datetime.now().strftime('%Y-%m-%d'),
            'note': testo[:500],  # salva un estratto del testo per debug
            'link_documento': doc_path
        })
        
        # Pulisci e standardizza i dati
        missione = self._pulisci_dati_missione(missione)
        return missione

    def _pulisci_dati_missione(self, missione: dict) -> dict:
        """Pulisce e standardizza i dati estratti"""
        # Rimuovi spazi extra
        for key, value in missione.items():
            if isinstance(value, str):
                missione[key] = value.strip()
        
        # Converti date
        for campo in ['data_inizio', 'data_fine']:
            if missione[campo]:
                try:
                    data = datetime.strptime(missione[campo], '%d/%m/%Y')
                    missione[campo] = data.strftime('%Y-%m-%d')
                except ValueError:
                    missione[campo] = ""
        
        # Converti numeri
        if missione['personale_totale']:
            try:
                missione['personale_totale'] = int(re.sub(r'[^\d]', '', missione['personale_totale']))
            except ValueError:
                missione['personale_totale'] = 0
                
        if missione['costo_totale']:
            try:
                # Rimuovi punti e sostituisci virgole con punti
                costo = missione['costo_totale'].replace('.', '').replace(',', '.')
                missione['costo_totale'] = float(costo)
            except ValueError:
                missione['costo_totale'] = 0.0
        
        return missione

    def _trova_documenti(self, soup: BeautifulSoup) -> list:
        """Trova tutti i documenti relativi alle missioni"""
        # TODO: Implementare la logica specifica per trovare i documenti
        # Questo dipenderà dalla struttura HTML del sito
        return soup.find_all('div', class_='documento-missione')

    def _estrai_dati_documento(self, doc: BeautifulSoup) -> dict:
        """Estrae i dati da un singolo documento"""
        try:
            # TODO: Implementare la logica specifica per estrarre i dati
            # Questo è un esempio di struttura
            return {
                'nome_missione': self._estrai_nome_missione(doc),
                'paese': self._estrai_paese(doc),
                'data_inizio': self._estrai_data_inizio(doc),
                'data_fine': self._estrai_data_fine(doc),
                'personale_totale': self._estrai_personale(doc),
                'costo_totale': self._estrai_costo(doc),
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

    def _estrai_nome_missione(self, doc: BeautifulSoup) -> str:
        """Estrae il nome della missione"""
        # TODO: Implementare la logica specifica
        return doc.find('h2').text.strip() if doc.find('h2') else ""

    def _estrai_paese(self, doc: BeautifulSoup) -> str:
        """Estrae il paese della missione"""
        # TODO: Implementare la logica specifica
        return doc.find('span', class_='paese').text.strip() if doc.find('span', class_='paese') else ""

    def _estrai_data_inizio(self, doc: BeautifulSoup) -> str:
        """Estrae la data di inizio"""
        # TODO: Implementare la logica specifica
        return doc.find('span', class_='data-inizio').text.strip() if doc.find('span', class_='data-inizio') else ""

    def _estrai_data_fine(self, doc: BeautifulSoup) -> str:
        """Estrae la data di fine"""
        # TODO: Implementare la logica specifica
        return doc.find('span', class_='data-fine').text.strip() if doc.find('span', class_='data-fine') else ""

    def _estrai_personale(self, doc: BeautifulSoup) -> int:
        """Estrae il numero di personale"""
        # TODO: Implementare la logica specifica
        testo = doc.find('span', class_='personale').text.strip() if doc.find('span', class_='personale') else "0"
        return int(re.sub(r'[^\d]', '', testo))

    def _estrai_costo(self, doc: BeautifulSoup) -> float:
        """Estrae il costo totale"""
        # TODO: Implementare la logica specifica
        testo = doc.find('span', class_='costo').text.strip() if doc.find('span', class_='costo') else "0"
        return float(re.sub(r'[^\d.]', '', testo))

    def _estrai_tipo_missione(self, doc: BeautifulSoup) -> str:
        """Estrae il tipo di missione"""
        # TODO: Implementare la logica specifica
        return doc.find('span', class_='tipo').text.strip() if doc.find('span', class_='tipo') else ""

    def _estrai_mandato(self, doc: BeautifulSoup) -> str:
        """Estrae il mandato della missione"""
        # TODO: Implementare la logica specifica
        return doc.find('span', class_='mandato').text.strip() if doc.find('span', class_='mandato') else ""

    def _estrai_note(self, doc: BeautifulSoup) -> str:
        """Estrae le note aggiuntive"""
        # TODO: Implementare la logica specifica
        return doc.find('div', class_='note').text.strip() if doc.find('div', class_='note') else ""

    def _estrai_link(self, doc: BeautifulSoup) -> str:
        """Estrae il link al documento"""
        # TODO: Implementare la logica specifica
        link = doc.find('a', href=True)
        return link['href'] if link else "" 