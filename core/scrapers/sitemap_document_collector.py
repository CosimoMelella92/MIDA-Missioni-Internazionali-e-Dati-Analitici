import httpx
import os
import hashlib
import time
import logging
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from datetime import datetime
import pandas as pd
from typing import List, Dict, Any
from .base_collector import BaseCollector
from pathlib import Path
import re

class SitemapDocumentCollector(BaseCollector):
    """Collector che scarica documenti da sitemap.xml di siti istituzionali"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.sitemap_urls = config.get('sitemap_urls', [])
        self.allowed_extensions = config.get('allowed_extensions', ['.pdf', '.doc', '.docx'])
        self.sleep_time = config.get('sleep_time', 2)
        
        # Aggiornato per salvare nella cartella centralizzata
        self.output_path = Path('data/documents')
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://google.com",
            "Accept-Language": "it-IT,it;q=0.9",
            "Connection": "keep-alive",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }
        
    def _get_urls_from_sitemap(self, sitemap_url: str) -> List[str]:
        urls = []
        try:
            with httpx.Client(headers=self.headers, follow_redirects=True, http2=False) as session:
                r = session.get(sitemap_url, timeout=30)
                soup = BeautifulSoup(r.text, "xml")
                for loc in soup.find_all("loc"):
                    urls.append(loc.text)
        except Exception as e:
            self.logger.error(f"Errore parsing sitemap {sitemap_url}: {e}")
        return urls

    def _is_document_url(self, url: str) -> bool:
        return any(url.lower().endswith(ext) for ext in self.allowed_extensions)

    def _hash_content(self, content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    def _download_file(self, url: str, session: httpx.Client) -> Dict[str, Any]:
        try:
            r = session.get(url, timeout=30)
            r.raise_for_status()
            content = r.content
            content_hash = self._hash_content(content)
            ext = os.path.splitext(url)[1].lower()
            
            # Genera un nome file più descrittivo basato sull'URL originale
            original_filename = os.path.basename(urlparse(url).path)
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
            
            filepath = self.output_path / final_filename
            
            with open(filepath, "wb") as f:
                f.write(content)
                
            metadata = {
                'filename': final_filename,
                'original_url': url,
                'download_date': datetime.now().isoformat(),
                'file_size': len(content),
                'content_type': r.headers.get('content-type', ''),
                'source_domain': urlparse(url).netloc,
                'content_hash': content_hash,
                'local_path': str(filepath)
            }
            self.logger.info(f"Scaricato in data/documents/: {final_filename} da {url}")
            return metadata
        except Exception as e:
            self.logger.error(f"Errore download {url}: {e}")
            return {}

    def collect(self) -> pd.DataFrame:
        all_metadata = []
        all_urls = []
        for sitemap_url in self.sitemap_urls:
            urls = self._get_urls_from_sitemap(sitemap_url)
            all_urls.extend(urls)
        doc_urls = [u for u in all_urls if self._is_document_url(u)]
        self.logger.info(f"Trovati {len(doc_urls)} documenti nelle sitemap.")
        
        with httpx.Client(headers=self.headers, follow_redirects=True, http2=False) as session:
            for url in doc_urls:
                metadata = self._download_file(url, session)
                if metadata:
                    all_metadata.append(metadata)
                time.sleep(self.sleep_time)
        if not all_metadata:
            return pd.DataFrame()
        df = pd.DataFrame(all_metadata)
        metadata_file = self.output_path / f"sitemap_document_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(metadata_file, index=False, encoding='utf-8')
        
        self.logger.info(f"Scaricati {len(df)} documenti in data/documents/")
        return df

    def validate(self, data: pd.DataFrame) -> bool:
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