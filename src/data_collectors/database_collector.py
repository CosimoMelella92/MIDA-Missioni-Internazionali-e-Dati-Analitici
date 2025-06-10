import pandas as pd
from typing import Dict, Any, List
from .base_collector import BaseCollector
import logging
from datetime import datetime
import requests
import io
import zipfile
import os

class DatabaseCollector(BaseCollector):
    """Collector for public databases"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.databases = config.get('databases', {})
        self.output_path = config.get('output_path')
        
    def collect(self) -> pd.DataFrame:
        """Collect data from configured databases"""
        all_data = []
        
        for db_name, db_config in self.databases.items():
            try:
                data = self._collect_database(db_name, db_config)
                if not data.empty:
                    all_data.append(data)
                    
            except Exception as e:
                self.logger.error(f"Error collecting data from {db_name}: {str(e)}")
                
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate database data"""
        if data.empty:
            return False
            
        required_columns = self.config.get('required_columns', [])
        return all(col in data.columns for col in required_columns)
    
    def _collect_database(self, db_name: str, db_config: Dict[str, Any]) -> pd.DataFrame:
        """Collect data from a specific database"""
        db_type = db_config.get('type', 'csv')
        
        if db_type == 'csv':
            return self._collect_csv(db_config)
        elif db_type == 'excel':
            return self._collect_excel(db_config)
        elif db_type == 'zip':
            return self._collect_zip(db_config)
        else:
            self.logger.error(f"Unsupported database type: {db_type}")
            return pd.DataFrame()
    
    def _collect_csv(self, db_config: Dict[str, Any]) -> pd.DataFrame:
        """Collect data from CSV source"""
        url = db_config.get('url')
        if not url:
            return pd.DataFrame()
            
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            # Try different encodings
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                try:
                    return pd.read_csv(io.StringIO(response.text), encoding=encoding)
                except UnicodeDecodeError:
                    continue
                    
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error reading CSV: {str(e)}")
            return pd.DataFrame()
    
    def _collect_excel(self, db_config: Dict[str, Any]) -> pd.DataFrame:
        """Collect data from Excel source"""
        url = db_config.get('url')
        if not url:
            return pd.DataFrame()
            
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            return pd.read_excel(io.BytesIO(response.content))
            
        except Exception as e:
            self.logger.error(f"Error reading Excel: {str(e)}")
            return pd.DataFrame()
    
    def _collect_zip(self, db_config: Dict[str, Any]) -> pd.DataFrame:
        """Collect data from ZIP source"""
        url = db_config.get('url')
        if not url:
            return pd.DataFrame()
            
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                # Find the first CSV or Excel file
                for file_name in zip_file.namelist():
                    if file_name.endswith('.csv'):
                        return pd.read_csv(zip_file.open(file_name))
                    elif file_name.endswith(('.xlsx', '.xls')):
                        return pd.read_excel(zip_file.open(file_name))
                        
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error reading ZIP: {str(e)}")
            return pd.DataFrame()
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process database data"""
        # Add collection timestamp
        data['collection_date'] = datetime.now()
        
        # Clean column names
        data.columns = [col.lower().replace(' ', '_') for col in data.columns]
        
        # Save processed data
        if self.output_path:
            output_file = os.path.join(
                self.output_path,
                f"database_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            data.to_csv(output_file, index=False)
            
        return data 