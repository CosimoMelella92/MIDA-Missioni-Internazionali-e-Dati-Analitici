import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import Dict, Any, List
from .base_collector import BaseCollector
import logging
from datetime import datetime
import time
import random

class WebScraper(BaseCollector):
    """Collector for web scraping"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.urls = config.get('urls', [])
        self.selectors = config.get('selectors', {})
        self.delay = config.get('delay', 1)
        
    def collect(self) -> pd.DataFrame:
        """Collect data from configured websites"""
        all_data = []
        
        for url in self.urls:
            try:
                # Add random delay to avoid being blocked
                time.sleep(self.delay + random.random())
                
                response = requests.get(url, headers=self._get_headers())
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                data = self._extract_data(soup)
                all_data.extend(data)
                
            except Exception as e:
                self.logger.error(f"Error scraping {url}: {str(e)}")
                
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate scraped data"""
        if data.empty:
            return False
            
        required_columns = self.config.get('required_columns', [])
        return all(col in data.columns for col in required_columns)
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for web request"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def _extract_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract data from BeautifulSoup object"""
        data = []
        
        for selector_name, selector_config in self.selectors.items():
            elements = soup.select(selector_config['css'])
            
            for element in elements:
                item_data = {}
                for field, field_config in selector_config['fields'].items():
                    try:
                        if field_config['type'] == 'text':
                            item_data[field] = element.select_one(field_config['css']).text.strip()
                        elif field_config['type'] == 'attribute':
                            item_data[field] = element.select_one(field_config['css'])[field_config['attr']]
                        elif field_config['type'] == 'list':
                            item_data[field] = [item.text.strip() for item in element.select(field_config['css'])]
                    except Exception as e:
                        self.logger.warning(f"Error extracting {field}: {str(e)}")
                        item_data[field] = None
                        
                data.append(item_data)
                
        return data
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process scraped data"""
        # Add collection timestamp
        data['collection_date'] = datetime.now()
        
        # Clean text data
        for col in data.select_dtypes(include=['object']).columns:
            data[col] = data[col].str.strip() if data[col].dtype == 'object' else data[col]
            
        return data 