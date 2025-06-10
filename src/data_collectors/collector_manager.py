import yaml
import pandas as pd
from typing import Dict, Any, List
import logging
from datetime import datetime
import os

from .api_collector import APICollector
from .web_scraper import WebScraper
from .rss_collector import RSSCollector
from .ocr_collector import OCRCollector
from .social_media_collector import SocialMediaCollector
from .database_collector import DatabaseCollector

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
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            return {}
    
    def _initialize_collectors(self) -> Dict[str, Any]:
        """Initialize all collectors"""
        collectors = {}
        
        # API Collector
        if 'api_collector' in self.config:
            collectors['api'] = APICollector(self.config['api_collector'])
            
        # Web Scraper
        if 'web_scraper' in self.config:
            collectors['web'] = WebScraper(self.config['web_scraper'])
            
        # RSS Collector
        if 'rss_collector' in self.config:
            collectors['rss'] = RSSCollector(self.config['rss_collector'])
            
        # OCR Collector
        if 'ocr_collector' in self.config:
            collectors['ocr'] = OCRCollector(self.config['ocr_collector'])
            
        # Social Media Collector
        if 'social_media_collector' in self.config:
            collectors['social'] = SocialMediaCollector(self.config['social_media_collector'])
            
        # Database Collector
        if 'database_collector' in self.config:
            collectors['database'] = DatabaseCollector(self.config['database_collector'])
            
        return collectors
    
    def collect_all(self) -> Dict[str, pd.DataFrame]:
        """Collect data from all sources"""
        results = {}
        
        for name, collector in self.collectors.items():
            try:
                self.logger.info(f"Collecting data from {name}")
                data = collector.run()
                if not data.empty:
                    results[name] = data
                    
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
                output_file = os.path.join(
                    output_dir,
                    f"{name}_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )
                data.to_csv(output_file, index=False)
                self.logger.info(f"Saved data from {name} to {output_file}")
                
    def merge_results(self, results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Merge data from all sources"""
        all_data = []
        
        for name, data in results.items():
            if not data.empty:
                # Add source column
                data['source'] = name
                all_data.append(data)
                
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame() 