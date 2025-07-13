#!/usr/bin/env python3
"""
Performance Benchmark per Document Extractor
Testa la velocità di elaborazione dei documenti
"""

import time
import os
from pathlib import Path
import logging

# Import our modules
from core.pdf_extractor.pdf_parser import PDFParser
from core.pdf_extractor.docx_parser import DOCXParser
from core.pdf_extractor.data_extractor import IntelligentDataExtractor

def benchmark_extraction():
    """Benchmark delle performance di estrazione"""
    
    print("🚀 Benchmark Performance Document Extractor")
    print("=" * 50)
    
    # Initialize components
    pdf_parser = PDFParser()
    docx_parser = DOCXParser()
    data_extractor = IntelligentDataExtractor()
    
    # Get documents directory
    docs_dir = Path('data/documents')
    
    if not docs_dir.exists():
        print("❌ Directory documents non trovata")
        return
    
    # Find all files
    pdf_files = list(docs_dir.glob('*.pdf'))
    docx_files = list(docs_dir.glob('*.docx'))
    
    print(f"📁 File trovati: {len(pdf_files)} PDF, {len(docx_files)} DOCX")
    
    total_start_time = time.time()
    
    # Benchmark PDF processing
    if pdf_files:
        print(f"\n📊 Benchmark PDF ({len(pdf_files)} files)")
        pdf_start = time.time()
        
        for i, pdf_file in enumerate(pdf_files, 1):
            file_start = time.time()
            print(f"  [{i}/{len(pdf_files)}] Processing {pdf_file.name}...")
            
            try:
                # Extract text
                text_data = pdf_parser.extract_text_from_pdf(str(pdf_file))
                text_time = time.time() - file_start
                
                # Extract structured data
                data_start = time.time()
                structured_data = data_extractor.extract_structured_data(text_data['full_text'])
                data_time = time.time() - data_start
                
                total_time = time.time() - file_start
                
                print(f"    ✅ Text: {text_time:.2f}s, Data: {data_time:.2f}s, Total: {total_time:.2f}s")
                print(f"    📊 Chars: {len(text_data['full_text']):,}, Missions: {len(structured_data.get('missions', []))}")
                
            except Exception as e:
                print(f"    ❌ Error: {e}")
        
        pdf_total = time.time() - pdf_start
        print(f"  📈 PDF Total: {pdf_total:.2f}s ({pdf_total/len(pdf_files):.2f}s per file)")
    
    # Benchmark DOCX processing
    if docx_files:
        print(f"\n📊 Benchmark DOCX ({len(docx_files)} files)")
        docx_start = time.time()
        
        for i, docx_file in enumerate(docx_files, 1):
            file_start = time.time()
            print(f"  [{i}/{len(docx_files)}] Processing {docx_file.name}...")
            
            try:
                # Extract text
                text_data = docx_parser.extract_text_from_docx(str(docx_file))
                text_time = time.time() - file_start
                
                # Extract structured data
                data_start = time.time()
                structured_data = data_extractor.extract_structured_data(text_data['full_text'])
                data_time = time.time() - data_start
                
                total_time = time.time() - file_start
                
                print(f"    ✅ Text: {text_time:.2f}s, Data: {data_time:.2f}s, Total: {total_time:.2f}s")
                print(f"    📊 Chars: {len(text_data['full_text']):,}, Missions: {len(structured_data.get('missions', []))}")
                
            except Exception as e:
                print(f"    ❌ Error: {e}")
        
        docx_total = time.time() - docx_start
        print(f"  📈 DOCX Total: {docx_total:.2f}s ({docx_total/len(docx_files):.2f}s per file)")
    
    total_time = time.time() - total_start_time
    total_files = len(pdf_files) + len(docx_files)
    
    print(f"\n🎯 RISULTATI FINALI")
    print(f"=" * 30)
    print(f"📁 File totali: {total_files}")
    print(f"⏱️  Tempo totale: {total_time:.2f}s")
    print(f"🚀 Velocità media: {total_time/total_files:.2f}s per file")
    print(f"📊 Throughput: {total_files/total_time:.2f} file/secondo")
    
    # Performance recommendations
    print(f"\n💡 RACCOMANDAZIONI")
    print(f"=" * 30)
    
    if total_time > 60:
        print("⚠️  Sistema lento - Considera:")
        print("  • Installare Tesseract per OCR")
        print("  • Ridurre dimensione documenti")
        print("  • Usare SSD per storage")
        print("  • Aumentare RAM disponibile")
    elif total_time > 30:
        print("⚡ Performance accettabile")
        print("  • Ottimizzazioni minori possibili")
    else:
        print("🚀 Performance eccellente!")
        print("  • Sistema ottimizzato")

if __name__ == "__main__":
    benchmark_extraction() 