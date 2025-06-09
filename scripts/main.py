import logging
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys
import os

# Aggiungi la directory scripts al PYTHONPATH
scripts_dir = Path(__file__).parent
sys.path.append(str(scripts_dir))

from camera_scraper import CameraScraper
from senato_scraper import SenatoScraper
from difesa_scraper import DifesaScraper
from esteri_scraper import EsteriScraper
from eeas_scraper import EEASScraper
from nato_scraper import NATOScraper
from un_scraper import UNScraper
from data_validator import DataValidator

def setup_logging():
    """Configura il sistema di logging."""
    with open('config/config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Crea directory logs se non esiste
    Path('logs').mkdir(parents=True, exist_ok=True)
    
    # Configura logging
    logging.basicConfig(
        level=config['logging']['level'],
        format=config['logging']['format'],
        filename=config['logging']['file']
    )
    
    # Aggiungi handler per console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter(config['logging']['format'])
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

def estrai_dati_fonte(scraper, nome_fonte: str, validator: DataValidator) -> pd.DataFrame:
    """Estrae e valida i dati da una singola fonte."""
    logger = logging.getLogger(__name__)
    logger.info(f"Avvio estrazione dati {nome_fonte}")
    
    try:
        df = scraper.estrai_dati()
        
        if df.empty:
            logger.warning(f"Nessun dato estratto da {nome_fonte}")
            return pd.DataFrame()
        
        # Valida dati
        is_valid, errori = validator.valida_dataframe(df, nome_fonte)
        if not is_valid:
            logger.warning(f"Errori di validazione per {nome_fonte}: {errori}")
            # Salva i dati non validi per analisi
            df.to_csv(f'data/raw/{nome_fonte}_invalid_{datetime.now().strftime("%Y%m%d")}.csv', index=False)
            return pd.DataFrame()
        
        logger.info(f"Estrazione dati {nome_fonte} completata: {len(df)} missioni trovate")
        return df
        
    except Exception as e:
        logger.error(f"Errore durante l'estrazione dati {nome_fonte}: {str(e)}")
        return pd.DataFrame()

def unisci_dati(lista_df: list) -> pd.DataFrame:
    """Unisce i dati da tutte le fonti."""
    logger = logging.getLogger(__name__)
    logger.info("Unione dei dati da tutte le fonti")
    
    try:
        # Filtra DataFrame vuoti
        lista_df = [df for df in lista_df if not df.empty]
        
        if not lista_df:
            logger.warning("Nessun dato da unire")
            return pd.DataFrame()
        
        # Unisci tutti i DataFrame
        df_unito = pd.concat(lista_df, ignore_index=True)
        
        # Rimuovi duplicati basati su nome_missione e paese
        df_unito = df_unito.drop_duplicates(subset=['nome_missione', 'paese'], keep='first')
        
        # Aggiungi timestamp
        df_unito['ultimo_aggiornamento'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        logger.info(f"Unione completata: {len(df_unito)} missioni totali")
        return df_unito
        
    except Exception as e:
        logger.error(f"Errore durante l'unione dei dati: {str(e)}")
        return pd.DataFrame()

def classifica_missione(row):
    """Classifica una missione in base alle sue caratteristiche."""
    fonte = str(row.get('fonte', '')).lower()
    tipo = str(row.get('tipo_missione', '')).lower()
    nome = str(row.get('nome_missione', '')).lower()
    note = str(row.get('note', '')).lower()
    mandato = str(row.get('mandato', '')).lower()
    
    # UE
    if 'eeas' in fonte or 'csdp' in tipo or 'pesd' in tipo or 'eu' in nome:
        if 'milit' in tipo or 'eutm' in nome or 'navfor' in nome or 'eunavfor' in nome:
            return 'UE_CSDP_MILITARE'
        if 'civ' in tipo or 'eupol' in nome or 'eubam' in nome or 'eulex' in nome:
            return 'UE_CSDP_CIVILE'
        return 'UE_CSDP_ALTRO'
    
    # NATO
    if 'nato' in fonte or 'kfor' in nome or 'isaf' in nome or 'resolute support' in nome:
        if 'training' in tipo or 'train' in nome:
            return 'NATO_TRAINING'
        if 'security' in tipo or 'security' in nome:
            return 'NATO_SECURITY'
        return 'NATO_PEACEKEEPING'
    
    # ONU
    if 'onu' in fonte or 'un' in nome or 'peacekeeping' in tipo or 'unifil' in nome or 'minurso' in nome or 'unsmis' in nome:
        if 'observation' in tipo or 'observer' in nome:
            return 'ONU_OBSERVATION'
        return 'ONU_PEACEKEEPING'
    
    # ITA Bilaterale
    if 'camera' in fonte or 'senato' in fonte or 'difesa' in fonte or 'esteri' in fonte:
        if 'bilateral' in note or 'bilaterale' in note or 'misin' in nome or 'libia' in nome or 'niger' in nome:
            return 'BILATERALE_ITA'
        if 'umanit' in tipo or 'sanitar' in tipo or 'ospedale' in nome or 'mozambico' in nome:
            return 'UMANITARIA_ITA'
        if 'antiterrorismo' in tipo or 'marittima' in tipo or 'golfo' in nome:
            return 'SICUREZZA_ITA'
        return 'ALTRO_ITA'
    
    # Multilaterale/Ibrida
    if ('bosnia' in nome and ('nato' in fonte or 'ue' in fonte)) or ('althea' in nome and 'nato' in fonte):
        return 'MULTILATERALE_IBRIDA'
    if 'unifil' in nome and 'onu' in fonte and ('ita' in note or 'italia' in note):
        return 'MULTILATERALE_IBRIDA'
    
    return 'ALTRO'

def classifica_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Classifica tutte le missioni nel dataset."""
    if df.empty:
        return df
        
    df['tipo_missione'] = df.apply(classifica_missione, axis=1)
    return df

def salva_dati_finali(df: pd.DataFrame):
    """Salva il dataset finale in vari formati."""
    logger = logging.getLogger(__name__)
    
    if df.empty:
        logger.warning("Nessun dato da salvare")
        return
    
    try:
        # Classifica prima di salvare
        df = classifica_dataset(df)
        
        # Crea directory se non esiste
        final_dir = Path('data/final')
        final_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Salva in CSV
        csv_path = final_dir / f'missioni_internazionali_{timestamp}.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"Dataset salvato in CSV: {csv_path}")
        
        # Salva in Excel
        excel_path = final_dir / f'missioni_internazionali_{timestamp}.xlsx'
        df.to_excel(excel_path, index=False)
        logger.info(f"Dataset salvato in Excel: {excel_path}")
        
        # Salva anche una versione raw
        raw_path = final_dir / f'missioni_internazionali_raw_{timestamp}.csv'
        df.to_csv(raw_path, index=False, encoding='utf-8')
        logger.info(f"Dataset raw salvato: {raw_path}")
        
    except Exception as e:
        logger.error(f"Errore durante il salvataggio dei dati: {str(e)}")

def main():
    """Funzione principale per l'estrazione e l'elaborazione dei dati."""
    logger = logging.getLogger(__name__)
    logger.info("Avvio processo di estrazione dati")
    
    try:
        # Setup logging
        setup_logging()
        
        # Inizializza validator
        validator = DataValidator()
        
        # Dizionario degli scraper
        scrapers = {
            'camera': CameraScraper(),
            # 'senato': SenatoScraper(),  # Temporaneamente disabilitato
            'difesa': DifesaScraper(),
            'esteri': EsteriScraper(),
            'eeas': EEASScraper(),
            'nato': NATOScraper(),
            'un': UNScraper()
        }
        
        # Esegui scraping per ogni fonte
        dati_completi = []
        for nome, scraper in scrapers.items():
            df = estrai_dati_fonte(scraper, nome, validator)
            if not df.empty:
                dati_completi.append(df)
        
        # Unisci e salva i dati
        if dati_completi:
            df_finale = unisci_dati(dati_completi)
            salva_dati_finali(df_finale)
        else:
            logger.warning("Nessun dato valido estratto")
            
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione del programma: {str(e)}")
        raise

if __name__ == "__main__":
    main() 