import httpx
import os
import hashlib
import re
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import List, Dict, Any, Optional
from .base_collector import BaseCollector
import pandas as pd

class APICollector(BaseCollector):
    """Collector specializzato per documenti da siti istituzionali europei e italiani"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.keywords = config.get('keywords', [
            "missione italiana",
            "afghanistan",
            "libano",
            "eu navfor",
            "operazione",
            "peacekeeping",
            "missione internazionale",
            "forze armate",
            "difesa",
            "esteri"
        ])
        self.allowed_extensions = config.get('allowed_extensions', ['.pdf', '.doc', '.docx'])
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.google.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
    def _is_document_url(self, url: str) -> bool:
        """Verifica se l'URL punta a un documento consentito"""
        return any(url.lower().endswith(ext) for ext in self.allowed_extensions)
        
    def _hash_content(self, content: bytes) -> str:
        """Genera hash del contenuto per evitare duplicati"""
        return hashlib.sha256(content).hexdigest()
        
    def _download_file(self, url: str, session: httpx.Client) -> Optional[Dict[str, Any]]:
        """Scarica un file con gestione errori e validazione"""
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            
            if not self._is_document_url(url):
                return None
                
            content = response.content
            content_hash = self._hash_content(content)
            ext = os.path.splitext(url)[1].lower()
            filename = f"{content_hash[:12]}{ext}"
            filepath = os.path.join(self.output_path, filename)
            
            # Salva il file
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
                'content_hash': content_hash
            }
            
            self.logger.info(f"Scaricato: {filename} da {url}")
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
            if self._is_document_url(href):
                links.append(href)
                
        return links
        
    def _search_in_site(self, url: str, session: httpx.Client) -> List[Dict[str, Any]]:
        """Cerca documenti in un sito"""
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            
            # Verifica se la pagina contiene keywords
            if not any(k.lower() in response.text.lower() for k in self.keywords):
                self.logger.info(f"Nessuna keyword trovata in {url}")
                return []
                
            self.logger.info(f"Keywords trovate in {url}")
            
            # Estrai e scarica documenti
            doc_links = self._extract_links(response.text, url)
            metadata_list = []
            
            for link in doc_links:
                metadata = self._download_file(link, session)
                if metadata:
                    metadata_list.append(metadata)
                    
            return metadata_list
            
        except Exception as e:
            self.logger.error(f"Errore elaborazione {url}: {str(e)}")
            return []
            
    def collect(self) -> pd.DataFrame:
        """Raccoglie documenti da tutte le URL configurate"""
        all_metadata = []
        
        with httpx.Client(headers=self.headers, follow_redirects=True) as session:
            for url in self.config['urls']:
                try:
                    metadata_list = self._search_in_site(url, session)
                    all_metadata.extend(metadata_list)
                    
                except Exception as e:
                    self.logger.error(f"Errore elaborazione {url}: {str(e)}")
                    continue
                    
        # Converti in DataFrame
        if not all_metadata:
            return pd.DataFrame()
            
        df = pd.DataFrame(all_metadata)
        
        # Salva metadata
        metadata_file = os.path.join(
            self.output_path,
            f"document_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        df.to_csv(metadata_file, index=False, encoding='utf-8')
        
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
            'source_domain',
            'content_hash'
        ]
        
        return all(col in data.columns for col in required_columns)
    
    def _get_headers(self, endpoint_config: Dict[str, Any]) -> Dict[str, str]:
        """Get headers for API request"""
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        if self.api_key:
            headers['Authorization'] = f"Bearer {self.api_key}"
            
        return headers
    
    def _get_params(self, endpoint_config: Dict[str, Any]) -> Dict[str, Any]:
        """Get parameters for API request"""
        return endpoint_config.get('params', {})
    
    def _parse_response(self, data: Any, endpoint_config: Dict[str, Any]) -> pd.DataFrame:
        """Parse API response into DataFrame"""
        data_path = endpoint_config.get('data_path', '')
        if data_path:
            for key in data_path.split('.'):
                data = data[key]
                
        return pd.DataFrame(data)
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process API data"""
        # Add collection timestamp
        data['collection_date'] = datetime.now()
        
        # Standardize column names
        data.columns = [col.lower().replace(' ', '_') for col in data.columns]
        
        return data 