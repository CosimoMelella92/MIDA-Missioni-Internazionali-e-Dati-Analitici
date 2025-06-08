import os
import pdfplumber
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scraping.log'),
        logging.StreamHandler()
    ]
)

class MissioniScraper:
    def __init__(self):
        self.raw_data_dir = Path('data/raw')
        self.processed_data_dir = Path('data/processed')
        self.setup_directories()
        
    def setup_directories(self):
        """Create necessary directories if they don't exist"""
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
        
    def extract_from_pdf(self, pdf_path):
        """Extract data from a single PDF file"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = ""
                for page in pdf.pages:
                    text += page.extract_text()
                # TODO: Implement specific extraction logic based on PDF structure
                return text
        except Exception as e:
            logging.error(f"Error processing {pdf_path}: {str(e)}")
            return None

    def process_pdfs(self):
        """Process all PDFs in the raw data directory"""
        pdf_files = list(self.raw_data_dir.glob('**/*.pdf'))
        if not pdf_files:
            logging.warning("No PDF files found in raw data directory")
            return None
            
        all_data = []
        for pdf_file in tqdm(pdf_files, desc="Processing PDFs"):
            data = self.extract_from_pdf(pdf_file)
            if data:
                # TODO: Parse the extracted text into structured data
                all_data.append({
                    'file_name': pdf_file.name,
                    'raw_text': data
                })
                
        return pd.DataFrame(all_data)

    def save_to_csv(self, df):
        """Save processed data to CSV"""
        if df is not None and not df.empty:
            timestamp = datetime.now().strftime('%Y_%m_%d')
            output_file = self.processed_data_dir / f'extracted_missioni_{timestamp}.csv'
            df.to_csv(output_file, index=False)
            logging.info(f"Data saved to {output_file}")
        else:
            logging.warning("No data to save")

def main():
    scraper = MissioniScraper()
    df = scraper.process_pdfs()
    scraper.save_to_csv(df)

if __name__ == "__main__":
    main() 