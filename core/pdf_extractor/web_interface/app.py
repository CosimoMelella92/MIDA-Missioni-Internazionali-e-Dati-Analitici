"""
Flask Web Interface for PDF & DOCX Extractor
Provides web interface to view extraction results for both PDF and Word
"""

from flask import Flask, render_template, request, jsonify, send_file
import json
import os
from pathlib import Path
import logging
from datetime import datetime
import pandas as pd

# Import our PDF/Word extractor modules
import sys
sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent.parent.parent))

from pdf_parser import PDFParser
from docx_parser import DOCXParser
from data_extractor import IntelligentDataExtractor

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables to store extraction results
extraction_results = []
aggregated_data = {}

@app.route('/')
def index():
    """Main page showing extraction overview"""
    return render_template('index.html', 
                         results=extraction_results,
                         aggregated=aggregated_data)

@app.route('/extract', methods=['POST'])
def extract_documents():
    """Extract data from PDFs and DOCX in documents folder"""
    global extraction_results, aggregated_data
    
    try:
        # Get documents directory
        docs_dir = Path('data/documents')
        
        if not docs_dir.exists():
            return jsonify({'error': 'Documents directory not found'}), 404
        
        # Initialize extractors
        pdf_parser = PDFParser()
        docx_parser = DOCXParser()
        data_extractor = IntelligentDataExtractor()
        
        # Process all PDFs
        logger.info("Starting PDF extraction...")
        pdf_results = pdf_parser.process_pdf_directory(str(docs_dir))
        # Process all DOCX
        logger.info("Starting DOCX extraction...")
        docx_results = docx_parser.process_docx_directory(str(docs_dir))
        
        # Merge results
        all_results = []
        for result in pdf_results:
            if 'error' not in result:
                structured_data = data_extractor.extract_structured_data(
                    result['extracted_data']['full_text']
                )
                all_results.append({
                    'file': result['file'],
                    'type': 'pdf',
                    'structured_data': structured_data,
                    'raw_data': result['extracted_data']
                })
        for result in docx_results:
            if 'error' not in result:
                structured_data = data_extractor.extract_structured_data(
                    result['extracted_data']['full_text']
                )
                all_results.append({
                    'file': result['file'],
                    'type': 'docx',
                    'structured_data': structured_data,
                    'raw_data': result['extracted_data']
                })
        extraction_results = all_results
        
        # Aggregate results (solo PDF per ora, ma si può estendere)
        aggregated_data = data_extractor.process_pdf_results(pdf_results + docx_results)
        
        logger.info(f"Extraction completed: {len(extraction_results)} files processed")
        
        return jsonify({
            'success': True,
            'files_processed': len(extraction_results),
            'total_missions': aggregated_data.get('total_missions', 0),
            'total_personnel': aggregated_data.get('total_personnel', 0),
            'total_costs': aggregated_data.get('total_costs', 0)
        })
        
    except Exception as e:
        logger.error(f"Extraction error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/results')
def view_results():
    """View detailed extraction results"""
    return render_template('results.html', results=extraction_results)

@app.route('/api/results')
def api_results():
    """API endpoint for extraction results"""
    return jsonify({
        'results': extraction_results,
        'aggregated': aggregated_data,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/export/csv')
def export_csv():
    """Export results as CSV"""
    if not extraction_results:
        return jsonify({'error': 'No results to export'}), 404
    
    # Create DataFrame from results
    data_rows = []
    for result in extraction_results:
        structured = result['structured_data']
        
        # Extract missions
        missions = structured.get('missions', [])
        for mission in missions:
            data_rows.append({
                'file': result['file'],
                'type': result.get('type', ''),
                'mission_name': mission.get('name', ''),
                'mission_confidence': mission.get('confidence', 0.0),
                'countries': ', '.join([c['name'] for c in structured.get('countries', [])]),
                'personnel': sum([p['number'] for p in structured.get('personnel', [])]),
                'costs': sum([c['amount'] for c in structured.get('costs', [])]),
                'organizations': ', '.join([o['name'] for o in structured.get('organizations', [])]),
                'mission_types': ', '.join([mt['type'] for mt in structured.get('mission_types', [])]),
                'overall_confidence': structured.get('confidence', 0.0)
            })
    
    if not data_rows:
        # If no missions found, create row with file info
        for result in extraction_results:
            structured = result['structured_data']
            data_rows.append({
                'file': result['file'],
                'type': result.get('type', ''),
                'mission_name': 'N/A',
                'mission_confidence': 0.0,
                'countries': ', '.join([c['name'] for c in structured.get('countries', [])]),
                'personnel': sum([p['number'] for p in structured.get('personnel', [])]),
                'costs': sum([c['amount'] for c in structured.get('costs', [])]),
                'organizations': ', '.join([o['name'] for o in structured.get('organizations', [])]),
                'mission_types': ', '.join([mt['type'] for mt in structured.get('mission_types', [])]),
                'overall_confidence': structured.get('confidence', 0.0)
            })
    
    df = pd.DataFrame(data_rows)
    
    # Save to temporary file
    output_path = Path('data/processed/document_extraction_results.csv')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    
    return send_file(output_path, as_attachment=True, 
                    download_name=f'document_extraction_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')

@app.route('/stats')
def view_stats():
    """View extraction statistics"""
    return render_template('stats.html', aggregated=aggregated_data)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 