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
from report_generator import AdvancedReportGenerator

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables to store extraction results
extraction_results = []
aggregated_data = {}
report_data = {}

# Progress tracking variables
extraction_progress = {
    'status': 'idle',  # idle, running, completed, error
    'current_step': '',
    'current_file': '',
    'files_processed': 0,
    'total_files': 0,
    'percentage': 0,
    'start_time': None,
    'end_time': None,
    'error_message': ''
}

@app.route('/')
def index():
    """Main page showing extraction overview"""
    return render_template('index.html', 
                         results=extraction_results,
                         aggregated=aggregated_data,
                         report_data=report_data)

@app.route('/api/progress')
def get_progress():
    """Get current extraction progress"""
    global extraction_progress
    
    # Calculate elapsed time if running
    elapsed_time = None
    if extraction_progress['start_time']:
        if extraction_progress['end_time']:
            elapsed_time = (extraction_progress['end_time'] - extraction_progress['start_time']).total_seconds()
        else:
            elapsed_time = (datetime.now() - extraction_progress['start_time']).total_seconds()
    
    return jsonify({
        **extraction_progress,
        'elapsed_time': elapsed_time,
        'estimated_remaining': None  # Could be calculated based on progress
    })

@app.route('/extract', methods=['POST'])
def extract_documents():
    """Extract data from PDFs and DOCX in documents folder"""
    global extraction_results, aggregated_data, report_data, extraction_progress
    
    try:
        # Initialize progress
        extraction_progress.update({
            'status': 'running',
            'current_step': 'Inizializzazione...',
            'current_file': '',
            'files_processed': 0,
            'total_files': 0,
            'percentage': 0,
            'start_time': datetime.now(),
            'end_time': None,
            'error_message': ''
        })
        
        # Get documents directory
        docs_dir = Path('data/documents')
        
        if not docs_dir.exists():
            extraction_progress.update({
                'status': 'error',
                'error_message': 'Documents directory not found'
            })
            return jsonify({'error': 'Documents directory not found'}), 404
        
        # Initialize extractors
        extraction_progress['current_step'] = 'Caricamento modelli NLP...'
        pdf_parser = PDFParser()
        docx_parser = DOCXParser()
        data_extractor = IntelligentDataExtractor()
        report_generator = AdvancedReportGenerator()
        
        # Count total files
        pdf_files = list(docs_dir.glob('*.pdf'))
        docx_files = list(docs_dir.glob('*.docx'))
        extraction_progress['total_files'] = len(pdf_files) + len(docx_files)
        
        # Process all PDFs
        logger.info("Starting PDF extraction...")
        extraction_progress['current_step'] = 'Elaborazione PDF...'
        
        pdf_results = []
        for i, pdf_file in enumerate(pdf_files):
            extraction_progress.update({
                'current_file': pdf_file.name,
                'files_processed': i,
                'percentage': int((i / extraction_progress['total_files']) * 100)
            })
            
            try:
                result = pdf_parser.extract_text_from_pdf(str(pdf_file))
                pdf_results.append(result)
            except Exception as e:
                logger.error(f"Error processing {pdf_file.name}: {str(e)}")
                pdf_results.append({'file': pdf_file.name, 'error': str(e)})
        
        # Process all DOCX
        logger.info("Starting DOCX extraction...")
        extraction_progress['current_step'] = 'Elaborazione DOCX...'
        
        docx_results = []
        for i, docx_file in enumerate(docx_files):
            extraction_progress.update({
                'current_file': docx_file.name,
                'files_processed': len(pdf_files) + i,
                'percentage': int(((len(pdf_files) + i) / extraction_progress['total_files']) * 100)
            })
            
            try:
                result = docx_parser.extract_text_from_docx(str(docx_file))
                docx_results.append(result)
            except Exception as e:
                logger.error(f"Error processing {docx_file.name}: {str(e)}")
                docx_results.append({'file': docx_file.name, 'error': str(e)})
        
        logger.info(f"File processing completed: {len(pdf_results)} PDFs, {len(docx_results)} DOCXs")
        
        # Merge results
        extraction_progress['current_step'] = 'Analisi dati estratti...'
        all_results = []
        successful_extractions = 0
        total_files = 0
        
        for result in pdf_results + docx_results:
            total_files += 1
            if 'error' not in result:
                try:
                    # Handle large texts gracefully
                    # Check if result has the expected structure
                    if 'full_text' in result:
                        text = result['full_text']
                    elif 'extracted_data' in result and 'full_text' in result['extracted_data']:
                        text = result['extracted_data']['full_text']
                    else:
                        # Try to find text in the result structure
                        text = result.get('text', '') or result.get('content', '') or ''
                        if not text and 'pages' in result:
                            # Combine text from all pages
                            text = '\n'.join([page.get('text', '') for page in result['pages']])
                    
                    if len(text) > 2000000:  # 2M characters
                        logger.warning(f"Text too large ({len(text)} chars), truncating for processing")
                        text = text[:2000000]  # Truncate to 2M chars
                    
                    structured_data = data_extractor.extract_structured_data(text)
                    all_results.append({
                        'file': result.get('filename', result.get('file', 'unknown')),
                        'type': result.get('type', 'pdf'),
                        'structured_data': structured_data,
                        'raw_data': result
                    })
                    successful_extractions += 1
                except Exception as e:
                    logger.error(f"Error processing {result.get('filename', result.get('file', 'unknown'))}: {str(e)}")
                    all_results.append({
                        'file': result.get('filename', result.get('file', 'unknown')),
                        'type': result.get('type', 'pdf'),
                        'error': str(e),
                        'structured_data': {},
                        'raw_data': result
                    })
            else:
                all_results.append(result)
        
        extraction_results = all_results
        
        # Aggregate results
        extraction_progress['current_step'] = 'Aggregazione risultati...'
        try:
            aggregated_data = data_extractor.process_pdf_results(pdf_results + docx_results)
        except Exception as e:
            logger.error(f"Error aggregating results: {str(e)}")
            aggregated_data = {
                'total_missions': 0,
                'total_personnel': 0,
                'total_costs': 0,
                'countries': [],
                'mission_types': []
            }
        
        # Generate comprehensive report
        extraction_progress['current_step'] = 'Generazione report...'
        logger.info("Generating comprehensive report...")
        try:
            report_data = report_generator.generate_comprehensive_report(extraction_results)
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            report_data = {}
        
        # Update progress to completed
        extraction_progress.update({
            'status': 'completed',
            'current_step': 'Completato!',
            'current_file': '',
            'files_processed': total_files,
            'percentage': 100,
            'end_time': datetime.now()
        })
        
        logger.info(f"Extraction completed: {successful_extractions}/{total_files} files processed successfully")
        
        return jsonify({
            'success': True,
            'files_processed': successful_extractions,
            'total_files': total_files,
            'total_missions': aggregated_data.get('total_missions', 0),
            'total_personnel': aggregated_data.get('total_personnel', 0),
            'total_costs': aggregated_data.get('total_costs', 0),
            'report_generated': bool(report_data),
            'report_path': report_data.get('main_report', '') if report_data else ''
        })
        
    except Exception as e:
        logger.error(f"Extraction error: {str(e)}")
        extraction_progress.update({
            'status': 'error',
            'error_message': str(e),
            'end_time': datetime.now()
        })
        return jsonify({'error': str(e)}), 500

@app.route('/results')
def view_results():
    """View detailed extraction results"""
    return render_template('results.html', results=extraction_results)

@app.route('/report')
def view_report():
    """View comprehensive extraction report"""
    if not report_data:
        return jsonify({'error': 'No report available. Run extraction first.'}), 404
    
    return render_template('report.html', report_data=report_data)

@app.route('/api/results')
def api_results():
    """API endpoint for extraction results"""
    return jsonify({
        'results': extraction_results,
        'aggregated': aggregated_data,
        'report_data': report_data,
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

@app.route('/export/report/<report_type>')
def export_report(report_type):
    """Export specific report files"""
    if not report_data or 'reports' not in report_data:
        return jsonify({'error': 'No report available'}), 404
    
    reports = report_data['reports']
    
    if report_type not in reports:
        return jsonify({'error': f'Report type {report_type} not found'}), 404
    
    report_path = Path(reports[report_type])
    
    if not report_path.exists():
        return jsonify({'error': 'Report file not found'}), 404
    
    return send_file(report_path, as_attachment=True)

@app.route('/stats')
def view_stats():
    """View extraction statistics"""
    return render_template('stats.html', aggregated=aggregated_data, report_data=report_data)

@app.route('/quality')
def view_quality():
    """View quality assessment"""
    if not report_data or 'analysis' not in report_data:
        return jsonify({'error': 'No quality data available'}), 404
    
    quality_metrics = report_data['analysis'].get('quality_metrics', {})
    return render_template('quality.html', quality_metrics=quality_metrics, report_data=report_data)

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    """Upload e processa un singolo file PDF o DOCX"""
    result = None
    error = None
    preview = None
    # Conta e lista file nella cartella centrale
    docs_dir = Path('data/documents')
    file_list = []
    num_pdfs = 0
    num_docxs = 0
    if docs_dir.exists():
        file_list = [f.name for f in docs_dir.iterdir() if f.is_file() and f.suffix.lower() in ['.pdf', '.docx']]
        num_pdfs = sum(1 for f in file_list if f.lower().endswith('.pdf'))
        num_docxs = sum(1 for f in file_list if f.lower().endswith('.docx'))
    if request.method == 'POST':
        file = request.files.get('file')
        if not file:
            error = 'Nessun file selezionato.'
        else:
            filename = file.filename
            ext = filename.lower().split('.')[-1]
            try:
                if ext == 'pdf':
                    parser = PDFParser()
                    parsed = parser.process_pdf_file(file)
                elif ext == 'docx':
                    parser = DOCXParser()
                    parsed = parser.process_docx_file(file)
                else:
                    error = 'Formato non supportato. Usa PDF o DOCX.'
                    return render_template('upload.html', result=None, error=error, preview=None, num_pdfs=num_pdfs, num_docxs=num_docxs, file_list=file_list)
                # Estrazione testo
                if 'full_text' in parsed:
                    text = parsed['full_text']
                elif 'extracted_data' in parsed and 'full_text' in parsed['extracted_data']:
                    text = parsed['extracted_data']['full_text']
                else:
                    text = parsed.get('text', '') or parsed.get('content', '') or ''
                
                preview = text[:2000] + ('...' if len(text) > 2000 else '')
                # Estrazione strutturata
                extractor = IntelligentDataExtractor()
                result = extractor.extract_structured_data(text)
            except Exception as e:
                error = f'Errore durante l\'estrazione: {e}'
    return render_template('upload.html', result=result, error=error, preview=preview, num_pdfs=num_pdfs, num_docxs=num_docxs, file_list=file_list)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 