"""
PDF Parser Module
Base functionality for PDF text extraction and OCR
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import io
import re

class PDFParser:
    """Base PDF parser with OCR support"""
    
    def __init__(self, tesseract_path: Optional[str] = None):
        """
        Initialize PDF parser
        
        Args:
            tesseract_path: Path to tesseract executable (optional)
        """
        self.logger = logging.getLogger(__name__)
        
        # Configure tesseract if path provided
        if tesseract_path:
            pytesseract.pytesseract.tesseract_cmd = tesseract_path
        
        # Common patterns for mission data
        self.mission_patterns = {
            'mission_name': r'(?:missione|mission|operazione|operation)\s*[:\-]?\s*([A-Z\s\-]+)',
            'country': r'(?:paese|country|stato|state)\s*[:\-]?\s*([A-Z\s\-]+)',
            'personnel': r'(?:personale|personnel|militari|military)\s*[:\-]?\s*(\d+)',
            'cost': r'(?:costo|cost|spesa|expense)\s*[:\-]?\s*([\d,\.]+)\s*(?:euro|€|eur)',
            'date': r'(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})',
            'year': r'(?:anno|year)\s*[:\-]?\s*(\d{4})'
        }
    
    def extract_text_from_pdf(self, pdf_path: str) -> Dict[str, any]:
        """
        Extract text from PDF using multiple methods
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            Dictionary with extracted text and metadata
        """
        pdf_path = Path(pdf_path)
        
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        self.logger.info(f"Processing PDF: {pdf_path.name}")
        
        try:
            # Open PDF with PyMuPDF
            doc = fitz.open(str(pdf_path))
            
            extracted_data = {
                'filename': pdf_path.name,
                'pages': len(doc),
                'text_by_page': [],
                'full_text': '',
                'tables': [],
                'images': [],
                'metadata': doc.metadata
            }
            
            # Extract text from each page
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                
                # Extract text
                text = page.get_text()
                
                # If text is minimal, try OCR (only if tesseract is available)
                if len(text.strip()) < 100:
                    try:
                        # Quick check if tesseract is available
                        pytesseract.get_tesseract_version()
                        self.logger.info(f"Page {page_num + 1}: Using OCR")
                        text = self._ocr_page(page)
                    except Exception:
                        # Skip OCR if tesseract not available
                        self.logger.debug(f"Page {page_num + 1}: Skipping OCR (tesseract not available)")
                        pass
                
                extracted_data['text_by_page'].append({
                    'page': page_num + 1,
                    'text': text,
                    'length': len(text)
                })
                
                extracted_data['full_text'] += text + '\n'
                
                # Extract tables if present
                tables = self._extract_tables(page)
                if tables:
                    extracted_data['tables'].extend(tables)
                
                # Extract images if present
                images = self._extract_images(page)
                if images:
                    extracted_data['images'].extend(images)
            
            doc.close()
            
            self.logger.info(f"Extracted {len(extracted_data['full_text'])} characters from {pdf_path.name}")
            return extracted_data
            
        except Exception as e:
            self.logger.error(f"Error processing PDF {pdf_path}: {str(e)}")
            raise
    
    def _ocr_page(self, page) -> str:
        """Extract text from page using OCR"""
        try:
            # Convert page to image
            pix = page.get_pixmap()
            img_data = pix.tobytes("png")
            
            # Use PIL to open image
            img = Image.open(io.BytesIO(img_data))
            
            # OCR with tesseract
            text = pytesseract.image_to_string(img, lang='ita+eng')
            
            return text
            
        except Exception as e:
            self.logger.warning(f"OCR failed: {str(e)}")
            return ""
    
    def _extract_tables(self, page) -> List[Dict]:
        """Extract tables from page"""
        tables = []
        
        try:
            # Get table blocks
            table_blocks = page.get_text("dict")["blocks"]
            
            for block in table_blocks:
                if "lines" in block:
                    table_data = []
                    for line in block["lines"]:
                        row = []
                        for span in line["spans"]:
                            row.append(span["text"])
                        if row:
                            table_data.append(row)
                    
                    if table_data:
                        tables.append({
                            'data': table_data,
                            'rows': len(table_data),
                            'columns': len(table_data[0]) if table_data else 0
                        })
        
        except Exception as e:
            self.logger.warning(f"Table extraction failed: {str(e)}")
        
        return tables
    
    def _extract_images(self, page) -> List[Dict]:
        """Extract images from page"""
        images = []
        
        try:
            image_list = page.get_images()
            
            for img_index, img in enumerate(image_list):
                xref = img[0]
                pix = fitz.Pixmap(page.parent, xref)
                
                if pix.n - pix.alpha < 4:  # GRAY or RGB
                    images.append({
                        'index': img_index,
                        'width': pix.width,
                        'height': pix.height,
                        'size': len(pix.tobytes())
                    })
                
                pix = None
        
        except Exception as e:
            self.logger.warning(f"Image extraction failed: {str(e)}")
        
        return images
    
    def extract_mission_data(self, text: str) -> Dict[str, any]:
        """
        Extract mission-specific data from text
        
        Args:
            text: Extracted text from PDF
            
        Returns:
            Dictionary with extracted mission data
        """
        mission_data = {
            'mission_name': None,
            'country': None,
            'personnel': None,
            'cost': None,
            'dates': [],
            'years': [],
            'confidence': 0.0
        }
        
        # Apply patterns to extract data
        for field, pattern in self.mission_patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            
            if matches:
                if field == 'mission_name':
                    mission_data['mission_name'] = matches[0].strip()
                elif field == 'country':
                    mission_data['country'] = matches[0].strip()
                elif field == 'personnel':
                    mission_data['personnel'] = int(matches[0])
                elif field == 'cost':
                    # Clean cost string and convert to float
                    cost_str = matches[0].replace(',', '').replace('.', '')
                    try:
                        mission_data['cost'] = float(cost_str)
                    except ValueError:
                        pass
                elif field == 'date':
                    mission_data['dates'].extend(matches)
                elif field == 'year':
                    mission_data['years'].extend([int(y) for y in matches])
        
        # Calculate confidence based on extracted fields
        filled_fields = sum(1 for v in mission_data.values() if v is not None and v != [])
        mission_data['confidence'] = filled_fields / len(self.mission_patterns)
        
        return mission_data
    
    def process_pdf_directory(self, directory_path: str) -> List[Dict]:
        """
        Process all PDFs in a directory
        
        Args:
            directory_path: Path to directory containing PDFs
            
        Returns:
            List of extracted data from all PDFs
        """
        directory = Path(directory_path)
        results = []
        
        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory_path}")
        
        pdf_files = list(directory.glob("*.pdf"))
        self.logger.info(f"Found {len(pdf_files)} PDF files in {directory_path}")
        
        for pdf_file in pdf_files:
            try:
                extracted_data = self.extract_text_from_pdf(str(pdf_file))
                mission_data = self.extract_mission_data(extracted_data['full_text'])
                
                results.append({
                    'file': pdf_file.name,
                    'extracted_data': extracted_data,
                    'mission_data': mission_data
                })
                
            except Exception as e:
                self.logger.error(f"Error processing {pdf_file.name}: {str(e)}")
                results.append({
                    'file': pdf_file.name,
                    'error': str(e)
                })
        
        return results 