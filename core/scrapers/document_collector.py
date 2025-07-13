import httpx
import os
import re
import hashlib
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import List, Dict, Any, Optional
from .base_collector import BaseCollector
import pandas as pd
from pathlib import Path

class DocumentCollector(BaseCollector):
    """Collector specializzato per documenti PDF e DOC da siti istituzionali"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.allowed_extensions = ['.pdf', '.doc', '.docx']
        self.allowed_domains = [
            'difesa.it',
            'esteri.it',
            'senato.it',
            'camera.it',
            'governo.it',
            'carabinieri.it',
            'marina.difesa.it',
            'esercito.difesa.it',
            'aeronautica.difesa.it'
        ]
        
        # Aggiornato per salvare nella cartella centralizzata
        self.output_path = Path('data/documents')
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.google.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
    def _is_allowed_domain(self, url: str) -> bool:
        """Verifica se il dominio è tra quelli consentiti"""
        domain = urlparse(url).netloc.lower()
        return any(allowed in domain for allowed in self.allowed_domains)
        
    def _is_document_url(self, url: str) -> bool:
        """Verifica se l'URL punta a un documento consentito"""
        return any(url.lower().endswith(ext) for ext in self.allowed_extensions)
        
    def _generate_filename(self, original_url: str) -> str:
        """Genera un nome file descrittivo basato sull'URL originale"""
        original_filename = os.path.basename(urlparse(original_url).path)
        if not original_filename or original_filename == '':
            original_filename = 'document'
        
        # Rimuovi caratteri problematici dal nome file
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', original_filename)
        
        # Se il file esiste già, aggiungi un suffisso
        base_name = os.path.splitext(safe_filename)[0]
        counter = 1
        final_filename = safe_filename
        
        while (self.output_path / final_filename).exists():
            name, ext_part = os.path.splitext(safe_filename)
            final_filename = f"{name}_{counter}{ext_part}"
            counter += 1
        
        return final_filename
        
    def _download_file(self, url: str, session: httpx.Client) -> Optional[Dict[str, Any]]:
        """Scarica un file con gestione errori e validazione"""
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            
            if not self._is_document_url(url):
                return None
                
            content = response.content
            filename = self._generate_filename(url)
            filepath = self.output_path / filename
            
            # Salva il file nella cartella centralizzata
            with open(filepath, 'wb') as f:
                f.write(content)
                
            # Estrai metadata
            metadata = {
                'filename': filename,
                'original_url': url,
                'download_date': datetime.now().isoformat(),
                'file_size': len(content),
                'content_type': response.headers.get('content-type', ''),
                'source_domain': urlparse(url).netloc,
                'local_path': str(filepath)
            }
            
            self.logger.info(f"Scaricato in data/documents/: {filename} da {url}")
            return metadata
            
        except Exception as e:
            self.logger.error(f"Errore download {url}: {str(e)}")
            return None
            
    def _extract_links(self, html: str, base_url: str) -> List[str]:
        """Estrae link a documenti dalla pagina HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        for a in soup.find_all('a', href=True):
            href = urljoin(base_url, a['href'])
            if self._is_document_url(href) and self._is_allowed_domain(href):
                links.append(href)
                
        return links
        
    def collect(self) -> pd.DataFrame:
        """Raccoglie documenti da tutte le URL configurate"""
        all_metadata = []
        
        with httpx.Client(headers=self.headers, follow_redirects=True) as session:
            for url in self.config['urls']:
                try:
                    # Scarica pagina iniziale
                    response = session.get(url, timeout=30)
                    response.raise_for_status()
                    
                    # Estrai link a documenti
                    doc_links = self._extract_links(response.text, url)
                    self.logger.info(f"Trovati {len(doc_links)} documenti in {url}")
                    
                    # Scarica ogni documento
                    for doc_url in doc_links:
                        metadata = self._download_file(doc_url, session)
                        if metadata:
                            all_metadata.append(metadata)
                            
                except Exception as e:
                    self.logger.error(f"Errore elaborazione {url}: {str(e)}")
                    continue
                    
        # Converti in DataFrame
        if not all_metadata:
            return pd.DataFrame()
            
        df = pd.DataFrame(all_metadata)
        
        # Salva metadata nella cartella documents
        metadata_file = self.output_path / f"document_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(metadata_file, index=False, encoding='utf-8')
        
        self.logger.info(f"Scaricati {len(df)} documenti in data/documents/")
        return df
        
    def validate(self, data: pd.DataFrame) -> bool:
        """Valida i dati raccolti"""
        if data.empty:
            return False
            
        required_columns = [
            'filename',
            'original_url',
            'download_date',
            'file_size',
            'source_domain'
        ]
        
        return all(col in data.columns for col in required_columns) 