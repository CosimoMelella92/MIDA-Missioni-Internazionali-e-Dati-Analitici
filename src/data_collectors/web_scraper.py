import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import logging
from typing import Dict, Any, List
import concurrent.futures
from urllib.parse import urljoin
import re
from fake_useragent import UserAgent
import cloudscraper

class WebScraper:
    """Web scraper for collecting data from multiple sources"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.ua = UserAgent()
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        
    def _get_headers(self) -> Dict[str, str]:
        """Generate random headers"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }
        
    def _make_request(self, url: str, retries: int = 3) -> requests.Response:
        """Make HTTP request with retries and random delays"""
        for i in range(retries):
            try:
                # Random delay between requests
                time.sleep(random.uniform(1, 3))
                
                # Make request with rotating headers
                response = self.scraper.get(
                    url,
                    headers=self._get_headers(),
                    timeout=30
                )
                response.raise_for_status()
                return response
                
            except Exception as e:
                self.logger.warning(f"Attempt {i+1} failed for {url}: {str(e)}")
                if i == retries - 1:
                    raise
                time.sleep(random.uniform(2, 5))
                
    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract all relevant links from page"""
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Convert relative URLs to absolute
            full_url = urljoin(base_url, href)
            # Filter out non-HTML links and external domains
            if re.match(r'^https?://', full_url) and not re.match(r'\.(pdf|doc|docx|xls|xlsx|zip|rar)$', full_url):
                links.append(full_url)
        return list(set(links))
        
    def _scrape_page(self, url: str) -> List[Dict[str, Any]]:
        """Scrape a single page"""
        try:
            response = self._make_request(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract data based on selectors
            data = []
            for element in soup.select(self.config['selectors']['mission_list']['css']):
                item = {}
                for field, field_config in self.config['selectors']['mission_list']['fields'].items():
                    try:
                        if field_config['type'] == 'text':
                            value = element.select_one(field_config['css']).get_text(strip=True)
                        elif field_config['type'] == 'attribute':
                            value = element.select_one(field_config['css'])[field_config['attr']]
                        item[field] = value
                    except Exception as e:
                        self.logger.warning(f"Error extracting {field} from {url}: {str(e)}")
                        item[field] = None
                if item:
                    data.append(item)
                    
            # Extract and follow links
            links = self._extract_links(soup, url)
            for link in links[:5]:  # Limit to first 5 links to avoid infinite crawling
                try:
                    sub_data = self._scrape_page(link)
                    data.extend(sub_data)
                except Exception as e:
                    self.logger.warning(f"Error scraping {link}: {str(e)}")
                    
            return data
            
        except Exception as e:
            self.logger.error(f"Error scraping {url}: {str(e)}")
            return []
            
    def run(self) -> pd.DataFrame:
        """Run the scraper on all configured URLs"""
        all_data = []
        
        # Use ThreadPoolExecutor for parallel scraping
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {
                executor.submit(self._scrape_page, url): url 
                for url in self.config['urls']
            }
            
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    all_data.extend(data)
                except Exception as e:
                    self.logger.error(f"Error processing {url}: {str(e)}")
                    
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        
        # Validate required columns
        missing_columns = [
            col for col in self.config['required_columns'] 
            if col not in df.columns
        ]
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return pd.DataFrame()
            
        return df 