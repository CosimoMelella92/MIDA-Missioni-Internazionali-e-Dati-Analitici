import logging
from pathlib import Path
from data_processor import DataProcessor
from document_processor import DocumentProcessor
import yaml
import os
import sys
from typing import Dict, Optional

def setup_logging() -> None:
    """Configura il logging."""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / 'data_processing.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_config() -> Dict:
    """Carica la configurazione dal file YAML."""
    config_path = Path('config/config.yaml')
    if not config_path.exists():
        raise FileNotFoundError(f"File di configurazione non trovato: {config_path}")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        raise RuntimeError(f"Errore nel caricamento della configurazione: {str(e)}")

def process_documents(config: dict, doc_processor: DocumentProcessor) -> Dict:
    """Processa tutti i documenti nella cartella specificata."""
    documents_data = {}
    docs_dir = Path(config['configurazione']['documenti'])
    
    if not docs_dir.exists():
        logging.error(f"Cartella documenti non trovata: {docs_dir}")
        return documents_data
    
    for file_path in docs_dir.glob('**/*'):
        if file_path.suffix.lower() in ['.pdf', '.docx', '.doc']:
            logging.info(f"Processando documento: {file_path}")
            try:
                doc_data = doc_processor.process_document(str(file_path))
                if doc_data:
                    documents_data[file_path.name] = doc_data
            except Exception as e:
                logging.error(f"Errore nel processare il documento {file_path}: {str(e)}")
    
    return documents_data

def main() -> Optional[int]:
    """Funzione principale."""
    try:
        # Setup
        setup_logging()
        logging.info("Avvio elaborazione dati")
        
        # Carica configurazione
        config = load_config()
        logging.info("Configurazione caricata con successo")
        
        # Verifica percorsi
        excel_path = Path(config['configurazione']['excel_path'])
        if not excel_path.exists():
            raise FileNotFoundError(f"File Excel non trovato: {excel_path}")
        
        # Inizializza i processor
        data_processor = DataProcessor(str(excel_path))
        doc_processor = DocumentProcessor()
        
        # Carica i dati Excel
        data_processor.load_data()
        logging.info("Dati Excel caricati con successo")
        
        # Processa i documenti
        documents_data = process_documents(config, doc_processor)
        logging.info(f"Processati {len(documents_data)} documenti")
        
        # Aggiorna i dati Excel con le informazioni estratte
        updated_missions = 0
        for mission_name in data_processor.get_all_missions():
            # Cerca corrispondenze nei documenti
            for doc_name, doc_data in documents_data.items():
                if mission_name.lower() in doc_name.lower():
                    data_processor.add_mission_data(mission_name, doc_data['mission_data'])
                    updated_missions += 1
                    break
        
        logging.info(f"Aggiornate {updated_missions} missioni con dati dai documenti")
        
        # Salva i dati arricchiti
        output_path = Path(config['configurazione']['processed_data']) / f"{excel_path.stem}_enriched.xlsx"
        output_path.parent.mkdir(exist_ok=True)
        data_processor.save_data(str(output_path))
        
        logging.info("Elaborazione completata con successo")
        return 0
        
    except FileNotFoundError as e:
        logging.error(f"Errore: {str(e)}")
        return 1
    except Exception as e:
        logging.error(f"Errore durante l'elaborazione: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 