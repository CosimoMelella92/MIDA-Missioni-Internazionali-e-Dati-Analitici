import os
import logging
from data_collectors.collector_manager import CollectorManager

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    # Inizializza il manager
    config_path = os.path.join('config', 'collectors_config.yaml')
    manager = CollectorManager(config_path)
    
    # Test collezione dati da tutte le fonti
    logging.info("Iniziando raccolta dati da tutte le fonti...")
    results = manager.collect_all()
    
    # Salva i risultati
    output_dir = os.path.join('data', 'collected')
    manager.save_results(results, output_dir)
    
    # Unisci i dati
    merged_data = manager.merge_results(results)
    
    # Stampa statistiche
    logging.info("\nStatistiche raccolta dati:")
    for source, data in results.items():
        logging.info(f"{source}: {len(data)} righe")
    
    if not merged_data.empty:
        logging.info(f"\nDati totali: {len(merged_data)} righe")
        logging.info("\nColonne disponibili:")
        for col in merged_data.columns:
            logging.info(f"- {col}")
    else:
        logging.warning("Nessun dato raccolto!")

if __name__ == "__main__":
    main() 