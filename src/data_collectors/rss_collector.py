import feedparser
import pandas as pd
from typing import Dict, Any, List
from .base_collector import BaseCollector
import logging
from datetime import datetime
import time

class RSSCollector(BaseCollector):
    """Collector for RSS feeds"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.feeds = config.get('feeds', [])
        self.delay = config.get('delay', 1)
        
    def collect(self) -> pd.DataFrame:
        """Collect data from configured RSS feeds"""
        all_data = []
        
        for feed_url in self.feeds:
            try:
                # Add delay to avoid overwhelming servers
                time.sleep(self.delay)
                
                feed = feedparser.parse(feed_url)
                data = self._parse_feed(feed)
                all_data.extend(data)
                
            except Exception as e:
                self.logger.error(f"Error parsing feed {feed_url}: {str(e)}")
                
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate RSS feed data"""
        if data.empty:
            return False
            
        required_columns = self.config.get('required_columns', [])
        return all(col in data.columns for col in required_columns)
    
    def _parse_feed(self, feed: Any) -> List[Dict[str, Any]]:
        """Parse RSS feed into list of dictionaries"""
        data = []
        
        for entry in feed.entries:
            item_data = {
                'title': entry.get('title', ''),
                'link': entry.get('link', ''),
                'description': entry.get('description', ''),
                'published': entry.get('published', ''),
                'author': entry.get('author', ''),
                'source': feed.feed.get('title', ''),
                'source_url': feed.feed.get('link', '')
            }
            
            # Add any additional fields from the feed
            for key, value in entry.items():
                if key not in item_data:
                    item_data[key] = value
                    
            data.append(item_data)
            
        return data
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process RSS feed data"""
        # Add collection timestamp
        data['collection_date'] = datetime.now()
        
        # Convert published dates to datetime
        if 'published' in data.columns:
            data['published'] = pd.to_datetime(data['published'], errors='coerce')
            
        # Clean text data
        for col in data.select_dtypes(include=['object']).columns:
            data[col] = data[col].str.strip() if data[col].dtype == 'object' else data[col]
            
        return data 