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

class EEASScraper(WebScraper):
    """Scraper per estrarre dati dal sito EEAS sulle missioni internazionali dell'UE."""
    
    def __init__(self):
        """Inizializza lo scraper EEAS."""
        with open('config/config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        super().__init__(
            source_name="eeas",
            base_url=config['eeas']['base_url'],
            sections=config['eeas']['sections']
        )
        self.logger = logging.getLogger(__name__)
        
    def estrai_dati(self) -> pd.DataFrame:
        """Estrae i dati dalle pagine EEAS"""
        self.logger.info("Inizio estrazione dati EEAS")
        
        try:
            dati_completi = []
            
            for section_name, url in self.sections.items():
                self.logger.info(f"Estrazione da {section_name}")
                
                # Effettua la richiesta HTTP
                response = self._make_request(url)
                if not response:
                    continue
                
                # Parsing della pagina
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Trova tutte le missioni nella sezione
                missioni = self._trova_missioni(soup)
                
                for missione in missioni:
                    try:
                        dati = self._estrai_dati_missione(missione, section_name)
                        if dati:
                            dati_completi.append(dati)
                    except Exception as e:
                        self.logger.error(f"Errore nell'estrazione dati missione: {str(e)}")
                        continue
            
            # Crea il DataFrame
            df = pd.DataFrame(dati_completi)
            
            # Validazione e pulizia
            df = self._valida_dati(df)
            
            # Salva i dati
            self._salva_dati(df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Errore durante l'estrazione dati EEAS: {str(e)}")
            raise
    
    def _trova_missioni(self, soup: BeautifulSoup) -> List[BeautifulSoup]:
        """Trova tutte le missioni nella pagina"""
        # Le missioni sono in div con classe 'mission-card'
        return soup.find_all('div', class_='mission-card')
    
    def _estrai_dati_missione(self, missione: BeautifulSoup, section_name: str) -> Dict:
        """Estrae i dati dettagliati di una missione"""
        try:
            # Estrai il nome della missione
            nome = missione.find('h3').text.strip() if missione.find('h3') else None
            
            # Estrai il paese/area
            paese = missione.find('div', class_='location').text.strip() if missione.find('div', class_='location') else None
            
            # Estrai le date
            date_text = missione.find('div', class_='dates').text.strip() if missione.find('div', class_='dates') else None
            data_inizio, data_fine = self._estrai_date(date_text) if date_text else (None, None)
            
            # Estrai il personale
            personale_text = missione.find('div', class_='personnel').text.strip() if missione.find('div', class_='personnel') else None
            personale_totale = self._estrai_numero(personale_text) if personale_text else None
            
            # Estrai il costo
            costo_text = missione.find('div', class_='budget').text.strip() if missione.find('div', class_='budget') else None
            costo_totale = self._estrai_costo(costo_text) if costo_text else None
            
            # Estrai il tipo di missione
            tipo_missione = self._determina_tipo_missione(section_name)
            
            # Estrai il mandato
            mandato = missione.find('div', class_='mandate').text.strip() if missione.find('div', class_='mandate') else None
            
            # Estrai le note
            note = missione.find('div', class_='description').text.strip() if missione.find('div', class_='description') else None
            
            # Estrai il link al documento
            link = missione.find('a')['href'] if missione.find('a') else None
            if link and not link.startswith('http'):
                link = self.base_url + link
            
            return {
                'nome_missione': nome,
                'paese': paese,
                'data_inizio': data_inizio,
                'data_fine': data_fine,
                'personale_totale': personale_totale,
                'costo_totale': costo_totale,
                'tipo_missione': tipo_missione,
                'mandato': mandato,
                'note': note,
                'link_documento': link,
                'fonte': 'EEAS',
                'data_estrazione': datetime.now().strftime('%Y-%m-%d')
            }
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione dati missione: {str(e)}")
            return None
    
    def _estrai_date(self, text: str) -> tuple:
        """Estrae le date da una stringa"""
        try:
            # Pattern per date nel formato EEAS
            pattern = r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})\s*-\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4}|present)'
            match = re.search(pattern, text)
            
            if match:
                data_inizio = datetime.strptime(match.group(1), '%d %B %Y').strftime('%Y-%m-%d')
                data_fine = 'present' if match.group(2) == 'present' else datetime.strptime(match.group(2), '%d %B %Y').strftime('%Y-%m-%d')
                return data_inizio, data_fine
            return None, None
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione date: {str(e)}")
            return None, None
    
    def _estrai_numero(self, text: str) -> int:
        """Estrae un numero da una stringa"""
        try:
            # Rimuovi spazi e caratteri non numerici
            numero = re.sub(r'[^\d]', '', text)
            return int(numero) if numero else None
        except:
            return None
    
    def _estrai_costo(self, text: str) -> float:
        """Estrae un costo da una stringa"""
        try:
            # Pattern per costi in euro
            pattern = r'€\s*([\d,.]+)'
            match = re.search(pattern, text)
            
            if match:
                # Rimuovi punti e sostituisci virgola con punto
                costo = match.group(1).replace('.', '').replace(',', '.')
                return float(costo)
            return None
            
        except Exception as e:
            self.logger.error(f"Errore nell'estrazione costo: {str(e)}")
            return None
    
    def _determina_tipo_missione(self, section_name: str) -> str:
        """Determina il tipo di missione in base alla sezione"""
        if 'civili' in section_name:
            return 'Civile'
        elif 'militari' in section_name:
            return 'Militare'
        return 'Mista'
    
    def _valida_dati(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida e pulisce i dati"""
        # Rimuovi righe con dati mancanti critici
        df = df.dropna(subset=['nome_missione', 'paese'])
        
        # Converti date in formato datetime
        for col in ['data_inizio', 'data_fine']:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Assicurati che i numeri siano nel formato corretto
        df['personale_totale'] = pd.to_numeric(df['personale_totale'], errors='coerce')
        df['costo_totale'] = pd.to_numeric(df['costo_totale'], errors='coerce')
        
        return df 