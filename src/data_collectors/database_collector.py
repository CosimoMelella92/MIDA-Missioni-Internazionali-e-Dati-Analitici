import pandas as pd
import requests
import logging
from typing import Dict, Any, List
import concurrent.futures
import time
import random
from io import BytesIO
import zipfile
import json
from datetime import datetime
import os

class DatabaseCollector:
    """Collector for database files"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.output_path = config.get('output_path', 'data/processed/databases')
        os.makedirs(self.output_path, exist_ok=True)
        
    def _download_file(self, url: str, retries: int = 3) -> bytes:
        """Download file with retries and random delays"""
        for i in range(retries):
            try:
                # Random delay between requests
                time.sleep(random.uniform(1, 3))
                
                response = requests.get(url, stream=True)
                response.raise_for_status()
                return response.content
                
            except Exception as e:
                self.logger.warning(f"Attempt {i+1} failed for {url}: {str(e)}")
                if i == retries - 1:
                    raise
                time.sleep(random.uniform(2, 5))
                
    def _process_csv(self, content: bytes, encoding: str = 'utf-8') -> pd.DataFrame:
        """Process CSV content"""
        try:
            # Try different encodings
            encodings = [encoding, 'utf-8', 'latin1', 'cp1252']
            for enc in encodings:
                try:
                    df = pd.read_csv(BytesIO(content), encoding=enc)
                    # Clean column names
                    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
                    return df
                except UnicodeDecodeError:
                    continue
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error reading CSV: {str(e)}")
            return pd.DataFrame()
            
    def _process_excel(self, content: bytes, sheet_name: str = None) -> pd.DataFrame:
        """Process Excel content"""
        try:
            # Try different sheet names
            if sheet_name:
                sheet_names = [sheet_name, 'Data', 'Sheet1', 0]
            else:
                sheet_names = ['Data', 'Sheet1', 0]
            
            for sheet in sheet_names:
                try:
                    df = pd.read_excel(BytesIO(content), sheet_name=sheet)
                    # Clean column names
                    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
                    return df
                except Exception:
                    continue
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error reading Excel: {str(e)}")
            return pd.DataFrame()
            
    def _process_json(self, content: bytes) -> pd.DataFrame:
        """Process JSON content"""
        try:
            data = json.loads(content)
            # Handle different JSON structures
            if isinstance(data, dict):
                if 'data' in data:
                    data = data['data']
                elif 'results' in data:
                    data = data['results']
            elif isinstance(data, list):
                pass
            else:
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            # Clean column names
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            return df
        except Exception as e:
            self.logger.error(f"Error reading JSON: {str(e)}")
            return pd.DataFrame()
            
    def _process_zip(self, content: bytes, file_type: str) -> pd.DataFrame:
        """Process ZIP content"""
        try:
            with zipfile.ZipFile(BytesIO(content)) as z:
                # Find all files of specified type
                matching_files = [f for f in z.namelist() if f.endswith(f'.{file_type}')]
                
                if not matching_files:
                    return pd.DataFrame()
                
                # Process first matching file
                with z.open(matching_files[0]) as f:
                    if file_type == 'csv':
                        df = pd.read_csv(f)
                    elif file_type == 'xlsx':
                        df = pd.read_excel(f)
                    elif file_type == 'json':
                        df = pd.DataFrame(json.load(f))
                    
                    # Clean column names
                    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
                    return df
                
        except Exception as e:
            self.logger.error(f"Error reading ZIP: {str(e)}")
            return pd.DataFrame()
            
    def _process_database(self, name: str, config: Dict[str, Any]) -> pd.DataFrame:
        """Process a single database"""
        try:
            content = self._download_file(config['url'])
            
            if config['type'] == 'csv':
                df = self._process_csv(content, config.get('encoding', 'utf-8'))
            elif config['type'] == 'excel':
                df = self._process_excel(content, config.get('sheet_name'))
            elif config['type'] == 'json':
                df = self._process_json(content)
            elif config['type'] == 'zip':
                df = self._process_zip(content, config.get('file_type', 'csv'))
            else:
                self.logger.error(f"Unsupported database type: {config['type']}")
                return pd.DataFrame()
                
            if not df.empty:
                # Add metadata
                df['source'] = name
                df['collection_date'] = datetime.now()
                
                # Convert date columns
                date_columns = [col for col in df.columns if 'date' in col.lower()]
                for col in date_columns:
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except:
                        pass
                    
                # Save to file
                output_file = os.path.join(
                    self.output_path,
                    f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )
                df.to_csv(output_file, index=False, encoding='utf-8')
                self.logger.info(f"Saved {name} data to {output_file}")
                
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing {name}: {str(e)}")
            return pd.DataFrame()
            
    def run(self) -> pd.DataFrame:
        """Run the collector on all configured databases"""
        all_data = []
        
        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_db = {
                executor.submit(self._process_database, name, config): name
                for name, config in self.config['databases'].items()
            }
            
            for future in concurrent.futures.as_completed(future_to_db):
                name = future_to_db[future]
                try:
                    df = future.result()
                    if not df.empty:
                        all_data.append(df)
                except Exception as e:
                    self.logger.error(f"Error processing {name}: {str(e)}")
                    
        # Combine all data
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            
            # Validate required columns
            missing_columns = [
                col for col in self.config['required_columns']
                if col not in final_df.columns
            ]
            if missing_columns:
                self.logger.error(f"Missing required columns: {missing_columns}")
                return pd.DataFrame()
                
            return final_df
            
        return pd.DataFrame() 