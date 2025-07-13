#!/usr/bin/env python3
"""
Test script per debuggare l'estrazione da un singolo file PDF
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.pdf_extractor.pdf_parser import PDFParser
from core.pdf_extractor.data_extractor import IntelligentDataExtractor
import logging

# Configura logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_single_file():
    """Test extraction on a single PDF file"""
    
    # Inizializza i parser
    pdf_parser = PDFParser()
    data_extractor = IntelligentDataExtractor()
    
    # Cerca file PDF nella cartella documents
    documents_dir = "data/documents"
    pdf_files = [f for f in os.listdir(documents_dir) if f.endswith('.pdf')]
    
    if not pdf_files:
        logger.error("Nessun file PDF trovato in data/documents")
        return
    
    # Testa il primo file PDF
    test_file = os.path.join(documents_dir, pdf_files[0])
    logger.info(f"Testing extraction on: {test_file}")
    
    try:
        # Estrai testo dal PDF
        logger.info("Extracting text from PDF...")
        pdf_result = pdf_parser.extract_text_from_pdf(test_file)
        
        if 'error' in pdf_result:
            logger.error(f"Error extracting text: {pdf_result['error']}")
            return
        
        # Ottieni il testo estratto
        if 'full_text' in pdf_result:
            text = pdf_result['full_text']
        elif 'extracted_data' in pdf_result and 'full_text' in pdf_result['extracted_data']:
            text = pdf_result['extracted_data']['full_text']
        else:
            text = pdf_result.get('text', '') or pdf_result.get('content', '') or ''
            if not text and 'pages' in pdf_result:
                text = '\n'.join([page.get('text', '') for page in pdf_result['pages']])
        
        logger.info(f"Extracted text length: {len(text)} characters")
        logger.info(f"First 1000 characters: {text[:1000]}...")
        
        # Estrai dati strutturati
        logger.info("Extracting structured data...")
        structured_data = data_extractor.extract_structured_data(text)
        
        # Mostra risultati
        logger.info("=== EXTRACTION RESULTS ===")
        logger.info(f"Missions found: {len(structured_data.get('missions', []))}")
        for mission in structured_data.get('missions', []):
            logger.info(f"  - {mission.get('name', 'Unknown')} (confidence: {mission.get('confidence', 0.0):.2f})")
        
        logger.info(f"Countries found: {len(structured_data.get('countries', []))}")
        for country in structured_data.get('countries', []):
            logger.info(f"  - {country.get('name', 'Unknown')} (confidence: {country.get('confidence', 0.0):.2f})")
        
        logger.info(f"Personnel entries: {len(structured_data.get('personnel', []))}")
        for personnel in structured_data.get('personnel', []):
            logger.info(f"  - {personnel.get('number', 0)} personnel (confidence: {personnel.get('confidence', 0.0):.2f})")
        
        logger.info(f"Costs found: {len(structured_data.get('costs', []))}")
        for cost in structured_data.get('costs', []):
            logger.info(f"  - €{cost.get('amount', 0):,.0f} (confidence: {cost.get('confidence', 0.0):.2f})")
        
        logger.info(f"Overall confidence: {structured_data.get('confidence', 0.0):.2f}")
        
    except Exception as e:
        logger.error(f"Error during test: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_single_file() 