import requests
import pandas as pd
from typing import Dict, Any, List
from .base_collector import BaseCollector
import logging
from datetime import datetime

class APICollector(BaseCollector):
    """Collector for API data sources"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_key = config.get('api_key')
        self.base_url = config.get('base_url')
        self.endpoints = config.get('endpoints', {})
        
    def collect(self) -> pd.DataFrame:
        """Collect data from configured APIs"""
        all_data = []
        
        for endpoint_name, endpoint_config in self.endpoints.items():
            try:
                url = f"{self.base_url}/{endpoint_config['path']}"
                headers = self._get_headers(endpoint_config)
                params = self._get_params(endpoint_config)
                
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                df = self._parse_response(data, endpoint_config)
                all_data.append(df)
                
            except Exception as e:
                self.logger.error(f"Error collecting data from {endpoint_name}: {str(e)}")
                
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate collected API data"""
        if data.empty:
            return False
            
        required_columns = self.config.get('required_columns', [])
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