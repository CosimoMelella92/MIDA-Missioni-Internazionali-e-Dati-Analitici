import requests
import pandas as pd
from typing import Dict, Any, List
from .base_collector import BaseCollector
import logging
from datetime import datetime
import time
import json
import os

class APICollector(BaseCollector):
    """Collector for API data sources"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_key = config.get('api_key')
        self.base_url = config.get('base_url')
        self.endpoints = config.get('endpoints', {})
        
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make API request with error handling and retries"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Add API key to headers
                headers = {
                    'Authorization': f'Bearer {self.api_key}',
                    'Accept': 'application/json'
                }
                
                # Make request
                response = requests.get(
                    f"{self.base_url}/{endpoint}",
                    params=params,
                    headers=headers,
                    timeout=30
                )
                
                # Check response
                response.raise_for_status()
                
                # Parse JSON
                data = response.json()
                
                # Extract data using data_path
                if 'data_path' in self.config['endpoints'][endpoint]:
                    data_path = self.config['endpoints'][endpoint]['data_path']
                    for key in data_path.split('.'):
                        data = data[key]
                
                return data
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    raise
            except (KeyError, json.JSONDecodeError) as e:
                self.logger.error(f"Error parsing response: {str(e)}")
                raise
            except Exception as e:
                self.logger.error(f"Unexpected error: {str(e)}")
                raise

    def collect(self) -> pd.DataFrame:
        """Collect data from all configured endpoints"""
        all_data = []
        
        for endpoint, config in self.config['endpoints'].items():
            try:
                # Get data from endpoint
                data = self._make_request(endpoint, config.get('params'))
                
                # Convert to DataFrame
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                elif isinstance(data, dict):
                    df = pd.DataFrame([data])
                else:
                    self.logger.warning(f"Unexpected data type from {endpoint}: {type(data)}")
                    continue
                
                # Add metadata
                df['source'] = endpoint
                df['collection_date'] = datetime.now()
                
                # Clean column names
                df.columns = [col.lower().replace(' ', '_') for col in df.columns]
                
                # Convert date columns
                date_columns = [col for col in df.columns if 'date' in col.lower()]
                for col in date_columns:
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except:
                        pass
                
                all_data.append(df)
                
            except Exception as e:
                self.logger.error(f"Error collecting data from {endpoint}: {str(e)}")
                continue
        
        if not all_data:
            return pd.DataFrame()
        
        # Combine all DataFrames
        result = pd.concat(all_data, ignore_index=True)
        
        # Save to file
        output_file = os.path.join(
            self.output_path,
            f"api_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        result.to_csv(output_file, index=False, encoding='utf-8')
        self.logger.info(f"Saved API data to {output_file}")
        
        return result
    
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