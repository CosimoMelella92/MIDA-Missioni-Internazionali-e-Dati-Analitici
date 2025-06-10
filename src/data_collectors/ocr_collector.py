import pytesseract
from PIL import Image
import pdf2image
import pandas as pd
from typing import Dict, Any, List
from .base_collector import BaseCollector
import logging
from datetime import datetime
import os

class OCRCollector(BaseCollector):
    """Collector for OCR processing"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.input_path = config.get('input_path')
        self.output_path = config.get('output_path')
        self.supported_formats = ['.pdf', '.png', '.jpg', '.jpeg', '.tiff']
        
    def collect(self) -> pd.DataFrame:
        """Collect data from documents using OCR"""
        all_data = []
        
        for root, _, files in os.walk(self.input_path):
            for file in files:
                if any(file.lower().endswith(fmt) for fmt in self.supported_formats):
                    try:
                        file_path = os.path.join(root, file)
                        data = self._process_file(file_path)
                        if data:
                            all_data.append(data)
                            
                    except Exception as e:
                        self.logger.error(f"Error processing {file}: {str(e)}")
                        
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate OCR data"""
        if data.empty:
            return False
            
        required_columns = self.config.get('required_columns', [])
        return all(col in data.columns for col in required_columns)
    
    def _process_file(self, file_path: str) -> Dict[str, Any]:
        """Process a single file with OCR"""
        file_data = {
            'filename': os.path.basename(file_path),
            'file_path': file_path,
            'processed_date': datetime.now()
        }
        
        if file_path.lower().endswith('.pdf'):
            # Convert PDF to images
            images = pdf2image.convert_from_path(file_path)
            text = []
            for i, image in enumerate(images):
                text.append(f"--- Page {i+1} ---\n")
                text.append(self._extract_text(image))
            file_data['text'] = '\n'.join(text)
            
        else:
            # Process image directly
            image = Image.open(file_path)
            file_data['text'] = self._extract_text(image)
            
        return file_data
    
    def _extract_text(self, image: Image.Image) -> str:
        """Extract text from image using Tesseract"""
        # Convert image to grayscale
        if image.mode != 'L':
            image = image.convert('L')
            
        # Apply OCR
        text = pytesseract.image_to_string(image)
        return text.strip()
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process OCR data"""
        # Clean text data
        if 'text' in data.columns:
            data['text'] = data['text'].str.strip()
            
        # Save processed files
        if self.output_path:
            for _, row in data.iterrows():
                output_file = os.path.join(
                    self.output_path,
                    f"{os.path.splitext(row['filename'])[0]}_processed.txt"
                )
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(row['text'])
                    
        return data 