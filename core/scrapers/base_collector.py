from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, List
import logging

class BaseCollector(ABC):
    """Base class for all data collectors"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @abstractmethod
    def collect(self) -> pd.DataFrame:
        """Collect data from the source"""
        pass
    
    @abstractmethod
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate collected data"""
        pass
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process collected data"""
        return data
    
    def save(self, data: pd.DataFrame, path: str):
        """Save collected data"""
        data.to_csv(path, index=False)
        
    def run(self) -> pd.DataFrame:
        """Run the complete collection process"""
        try:
            data = self.collect()
            if self.validate(data):
                data = self.process(data)
                return data
            else:
                self.logger.error("Data validation failed")
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error in data collection: {str(e)}")
            return pd.DataFrame() 