import yaml
import pandas as pd
from typing import Dict, Any, List
import logging
from datetime import datetime
import os
import requests
from urllib.parse import urlparse
import json

from .api_collector import APICollector
from .web_scraper import WebScraper
from .rss_collector import RSSCollector
from .ocr_collector import OCRCollector
from .social_media_collector import SocialMediaCollector
from .database_collector import DatabaseCollector
from .base_collector import BaseCollector
from .document_collector import DocumentCollector
from .european_document_collector import EuropeanDocumentCollector
from .sitemap_document_collector import SitemapDocumentCollector
from .smart_document_fetcher import SmartDocumentFetcher

class CollectorManager:
    """Manager for all data collectors"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = logging.getLogger(__name__)
        self.collectors = self._initialize_collectors()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            return {}
    
    def _initialize_collectors(self) -> Dict[str, BaseCollector]:
        """Initialize all collectors"""
        collectors = {}
        
        # OCR Collector
        if 'ocr_collector' in self.config:
            collectors['ocr'] = OCRCollector(self.config['ocr_collector'])
            
        # Web Scraper
        if 'web_scraper' in self.config:
            collectors['web'] = WebScraper(self.config['web_scraper'])
            
        # RSS Collector
        if 'rss_collector' in self.config:
            collectors['rss'] = RSSCollector(self.config['rss_collector'])
            
        # Database Collector
        if 'database_collector' in self.config:
            collectors['database'] = DatabaseCollector(self.config['database_collector'])
            
        # Social Media Collector
        if 'social_media_collector' in self.config:
            collectors['social'] = SocialMediaCollector(self.config['social_media_collector'])
            
        # API Collector
        if 'api_collector' in self.config:
            collectors['api'] = APICollector(self.config['api_collector'])
            
        # Document Collector
        if 'document_collector' in self.config:
            collectors['document'] = DocumentCollector(self.config['document_collector'])
            
        # European Document Collector
        if 'european_document_collector' in self.config:
            collectors['european_document'] = EuropeanDocumentCollector(self.config['european_document_collector'])
            
        # Sitemap Document Collector
        if 'sitemap_document_collector' in self.config:
            collectors['sitemap_document'] = SitemapDocumentCollector(self.config['sitemap_document_collector'])
            
        # Smart Document Fetcher
        if 'smart_document_fetcher' in self.config:
            collectors['smart_document'] = SmartDocumentFetcher(self.config['smart_document_fetcher'])
            
        return collectors
    
    def _download_file(self, url: str, output_path: str) -> bool:
        """Download a file from URL"""
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True
        except Exception as e:
            self.logger.error(f"Error downloading {url}: {str(e)}")
            return False
    
    def collect_all(self) -> Dict[str, pd.DataFrame]:
        """Collect data from all sources"""
        results = {}
        
        for name, collector in self.collectors.items():
            try:
                self.logger.info(f"Collecting data from {name}")
                data = collector.collect()
                
                if collector.validate(data):
                    results[name] = data
                else:
                    self.logger.error(f"Data validation failed for {name}")
                    
            except Exception as e:
                self.logger.error(f"Error collecting data from {name}: {str(e)}")
                
        return results
    
    def collect_specific(self, collector_name: str) -> pd.DataFrame:
        """Collect data from a specific source"""
        if collector_name in self.collectors:
            try:
                self.logger.info(f"Collecting data from {collector_name}")
                return self.collectors[collector_name].run()
            except Exception as e:
                self.logger.error(f"Error collecting data from {collector_name}: {str(e)}")
                
        return pd.DataFrame()
    
    def save_results(self, results: Dict[str, pd.DataFrame], output_dir: str):
        """Save collected data"""
        os.makedirs(output_dir, exist_ok=True)
        
        for name, data in results.items():
            if not data.empty:
                # Save as CSV
                csv_file = os.path.join(
                    output_dir,
                    f"{name}_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )
                data.to_csv(csv_file, index=False, encoding='utf-8')
                self.logger.info(f"Saved data from {name} to {csv_file}")
                
                # Save as JSON for web visualization
                json_file = os.path.join(
                    output_dir,
                    f"{name}_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                data.to_json(json_file, orient='records', date_format='iso')
                self.logger.info(f"Saved JSON data from {name} to {json_file}")
                
    def merge_results(self, results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Merge data from all sources"""
        all_data = []
        
        for name, data in results.items():
            if not data.empty:
                # Add source column
                data['source'] = name
                all_data.append(data)
                
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    def validate_data(self, data: pd.DataFrame, required_columns: List[str]) -> bool:
        """Validate data has required columns"""
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False
        return True 