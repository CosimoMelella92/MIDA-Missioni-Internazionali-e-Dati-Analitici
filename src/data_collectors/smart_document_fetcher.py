import httpx
import os
import re
import time
import hashlib
import xml.etree.ElementTree as ET
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import List, Dict, Any, Optional
from .base_collector import BaseCollector
import pandas as pd

class SmartDocumentFetcher(BaseCollector):
    """Collector avanzato per il download di documenti con gestione errori e Wayback Machine"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.sitemap_urls = config.get('sitemap_urls', [])
        self.indice_urls = config.get('indice_urls', [])
        self.allowed_extensions = config.get('allowed_extensions', ['.pdf', '.doc', '.docx'])
        self.sleep_time = config.get('sleep_time', 2)
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 5)
        
        # Headers più realistici
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "https://google.com",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1"
        }
        
        # Crea directory output se non esiste
        os.makedirs(self.output_path, exist_ok=True)
        
    def _is_document_url(self, url: str) -> bool:
        """Verifica se l'URL punta a un documento consentito"""
        return any(url.lower().endswith(ext) for ext in self.allowed_extensions)
        
    def _hash_content(self, content: bytes) -> str:
        """Genera hash del contenuto per evitare duplicati"""
        return hashlib.sha256(content).hexdigest()
        
    def _save_file(self, content: bytes, url: str) -> Optional[Dict[str, Any]]:
        """Salva il file e restituisce i metadata"""
        try:
            content_hash = self._hash_content(content)
            ext = os.path.splitext(url)[1].lower()
            filename = f"{content_hash[:12]}{ext}"
            filepath = os.path.join(self.output_path, filename)
            
            # Salva solo se non esiste già
            if not os.path.exists(filepath):
                with open(filepath, 'wb') as f:
                    f.write(content)
                self.logger.info(f"[OK] Salvato: {filename}")
                
                return {
                    'filename': filename,
                    'original_url': url,
                    'download_date': datetime.now().isoformat(),
                    'file_size': len(content),
                    'content_hash': content_hash,
                    'source_domain': urlparse(url).netloc
                }
            else:
                self.logger.info(f"[SKIP] Duplicato: {filename}")
                return None
                
        except Exception as e:
            self.logger.error(f"[ERROR] Errore salvataggio {url}: {str(e)}")
            return None
            
    def _try_wayback_machine(self, url: str) -> Optional[Dict[str, Any]]:
        """Prova a recuperare il documento dalla Wayback Machine"""
        try:
            archive_url = f"https://web.archive.org/web/{url}"
            with httpx.Client(headers=self.headers, follow_redirects=True, timeout=30) as session:
                response = session.get(archive_url)
                if response.status_code == 200:
                    self.logger.info(f"[WB] Recuperato da Wayback: {url}")
                    return self._save_file(response.content, url)
                else:
                    self.logger.warning(f"[WB] Non trovato in Wayback: {url}")
                    return None
                    
        except Exception as e:
            self.logger.error(f"[WB] Errore Wayback {url}: {str(e)}")
            return None
            
    def _download_with_retry(self, url: str, session: httpx.Client) -> Optional[Dict[str, Any]]:
        """Scarica un documento con retry e fallback su Wayback"""
        for attempt in range(self.max_retries):
            try:
                response = session.get(url, timeout=30)
                
                if response.status_code == 200:
                    return self._save_file(response.content, url)
                    
                elif response.status_code in [403, 404, 410]:
                    self.logger.warning(f"[RETRY] {url} → {response.status_code}")
                    if attempt == self.max_retries - 1:
                        return self._try_wayback_machine(url)
                        
                else:
                    self.logger.warning(f"[WARN] {url} → status {response.status_code}")
                    
            except httpx.RequestError as e:
                self.logger.error(f"[FAIL] {url} → {str(e)}")
                if attempt == self.max_retries - 1:
                    return self._try_wayback_machine(url)
                    
            time.sleep(self.retry_delay * (attempt + 1))
            
        return None
        
    def _extract_from_html(self, url: str, session: httpx.Client) -> List[Dict[str, Any]]:
        """Estrae e scarica documenti da una pagina HTML"""
        metadata_list = []
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml')
                links = [urljoin(url, a['href']) for a in soup.find_all('a', href=True)]
                
                for link in links:
                    if self._is_document_url(link):
                        time.sleep(self.sleep_time)
                        metadata = self._download_with_retry(link, session)
                        if metadata:
                            metadata_list.append(metadata)
                            
        except Exception as e:
            self.logger.error(f"[ERROR] {url} → {str(e)}")
            
        return metadata_list
        
    def _extract_from_sitemap(self, sitemap_url: str, session: httpx.Client) -> List[Dict[str, Any]]:
        """Estrae e scarica documenti da una sitemap XML"""
        metadata_list = []
        try:
            response = session.get(sitemap_url, timeout=30)
            if response.status_code == 200:
                root = ET.fromstring(response.text)
                ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                urls = [el.text for el in root.findall('.//ns:loc', ns)]
                
                for url in urls:
                    if self._is_document_url(url):
                        time.sleep(self.sleep_time)
                        metadata = self._download_with_retry(url, session)
                        if metadata:
                            metadata_list.append(metadata)
                            
        except Exception as e:
            self.logger.error(f"[ERROR] Sitemap {sitemap_url} → {str(e)}")
            
        return metadata_list
        
    def collect(self) -> pd.DataFrame:
        """Raccoglie documenti da tutte le fonti configurate"""
        all_metadata = []
        
        with httpx.Client(
            headers=self.headers,
            follow_redirects=True,
            timeout=30,
            http2=False  # Disabilita HTTP/2 per evitare problemi
        ) as session:
            
            # Processa sitemap
            for sitemap_url in self.sitemap_urls:
                self.logger.info(f"Processando sitemap: {sitemap_url}")
                metadata_list = self._extract_from_sitemap(sitemap_url, session)
                all_metadata.extend(metadata_list)
                
            # Processa pagine indice
            for indice_url in self.indice_urls:
                self.logger.info(f"Processando pagina indice: {indice_url}")
                metadata_list = self._extract_from_html(indice_url, session)
                all_metadata.extend(metadata_list)
                
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