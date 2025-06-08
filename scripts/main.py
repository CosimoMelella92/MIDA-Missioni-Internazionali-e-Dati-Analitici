import logging
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
from scripts.camera_scraper import CameraScraper
from scripts.senato_scraper import SenatoScraper
from scripts.difesa_scraper import DifesaScraper
from scripts.esteri_scraper import EsteriScraper
from scripts.eeas_scraper import EEASScraper
from scripts.nato_scraper import NATOScraper
from scripts.un_scraper import UNScraper
from scripts.data_validator import DataValidator

def setup_logging():
    """Configura il sistema di logging."""
    with open('config/config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
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

def estrai_dati_camera():
    """Estrae i dati dalla Camera dei Deputati"""
    logger = logging.getLogger(__name__)
    logger.info("Avvio estrazione dati Camera")
    
    try:
        scraper = CameraScraper()
        df = scraper.estrai_dati()
        logger.info(f"Estrazione dati Camera completata: {len(df)} missioni trovate")
        return df
    except Exception as e:
        logger.error(f"Errore durante l'estrazione dati Camera: {str(e)}")
        return pd.DataFrame()

def estrai_dati_senato():
    """Estrae i dati dal Senato"""
    logger = logging.getLogger(__name__)
    logger.info("Avvio estrazione dati Senato")
    
    try:
        scraper = SenatoScraper()
        df = scraper.estrai_dati()
        logger.info(f"Estrazione dati Senato completata: {len(df)} missioni trovate")
        return df
    except Exception as e:
        logger.error(f"Errore durante l'estrazione dati Senato: {str(e)}")
        return pd.DataFrame()

def estrai_dati_difesa():
    """Estrae i dati dal Ministero della Difesa"""
    logger = logging.getLogger(__name__)
    logger.info("Avvio estrazione dati Difesa")
    
    try:
        scraper = DifesaScraper()
        df = scraper.estrai_dati()
        logger.info(f"Estrazione dati Difesa completata: {len(df)} missioni trovate")
        return df
    except Exception as e:
        logger.error(f"Errore durante l'estrazione dati Difesa: {str(e)}")
        return pd.DataFrame()

def estrai_dati_esteri():
    """Estrae i dati dal Ministero degli Esteri"""
    logger = logging.getLogger(__name__)
    logger.info("Avvio estrazione dati Esteri")
    
    try:
        scraper = EsteriScraper()
        df = scraper.estrai_dati()
        logger.info(f"Estrazione dati Esteri completata: {len(df)} missioni trovate")
        return df
    except Exception as e:
        logger.error(f"Errore durante l'estrazione dati Esteri: {str(e)}")
        return pd.DataFrame()

def estrai_dati_eeas():
    """Estrae i dati dall'EEAS"""
    logger = logging.getLogger(__name__)
    logger.info("Avvio estrazione dati EEAS")
    
    try:
        scraper = EEASScraper()
        df = scraper.estrai_dati()
        logger.info(f"Estrazione dati EEAS completata: {len(df)} missioni trovate")
        return df
    except Exception as e:
        logger.error(f"Errore durante l'estrazione dati EEAS: {str(e)}")
        return pd.DataFrame()

def estrai_dati_nato():
    """Estrae i dati dalla NATO"""
    logger = logging.getLogger(__name__)
    logger.info("Avvio estrazione dati NATO")
    
    try:
        scraper = NATOScraper()
        df = scraper.estrai_dati()
        logger.info(f"Estrazione dati NATO completata: {len(df)} missioni trovate")
        return df
    except Exception as e:
        logger.error(f"Errore durante l'estrazione dati NATO: {str(e)}")
        return pd.DataFrame()

def estrai_dati_un():
    """Estrae i dati dall'ONU"""
    logger = logging.getLogger(__name__)
    logger.info("Avvio estrazione dati ONU")
    
    try:
        scraper = UNScraper()
        df = scraper.estrai_dati()
        logger.info(f"Estrazione dati ONU completata: {len(df)} missioni trovate")
        return df
    except Exception as e:
        logger.error(f"Errore durante l'estrazione dati ONU: {str(e)}")
        return pd.DataFrame()

def unisci_dati(lista_df: list) -> pd.DataFrame:
    """Unisce i dati da tutte le fonti"""
    logger = logging.getLogger(__name__)
    logger.info("Unione dei dati da tutte le fonti")
    
    try:
        # Unisci tutti i DataFrame
        df_unito = pd.concat(lista_df, ignore_index=True)
        
        # Rimuovi duplicati basati su nome_missione e paese
        df_unito = df_unito.drop_duplicates(subset=['nome_missione', 'paese'], keep='first')
        
        logger.info(f"Unione completata: {len(df_unito)} missioni totali")
        return df_unito
    except Exception as e:
        logger.error(f"Errore durante l'unione dei dati: {str(e)}")
        return pd.DataFrame()

def classifica_missione(row):
    fonte = str(row.get('fonte', '')).lower()
    tipo = str(row.get('tipo_missione', '')).lower()
    nome = str(row.get('nome_missione', '')).lower()
    note = str(row.get('note', '')).lower()
    mandato = str(row.get('mandato', '')).lower()
    # UE
    if 'eeas' in fonte or 'csdp' in tipo or 'pesd' in tipo or 'eu' in nome:
        if 'milit' in tipo or 'eutm' in nome or 'navfor' in nome or 'eunavfor' in nome:
            return 'UE - Militare'
        if 'civ' in tipo or 'eupol' in nome or 'eubam' in nome or 'eulex' in nome:
            return 'UE - Civile'
        return 'UE - Altro'
    # NATO
    if 'nato' in fonte or 'kfor' in nome or 'isaf' in nome or 'resolute support' in nome:
        return 'NATO - Difesa'
    # ONU
    if 'onu' in fonte or 'un' in nome or 'peacekeeping' in tipo or 'unifil' in nome or 'minurso' in nome or 'unsmis' in nome:
        return 'ONU - Peacekeeping'
    # ITA Bilaterale
    if 'camera' in fonte or 'senato' in fonte or 'difesa' in fonte or 'esteri' in fonte:
        if 'bilateral' in note or 'bilaterale' in note or 'misin' in nome or 'libia' in nome or 'niger' in nome:
            return 'ITA - Bilaterale'
        if 'umanit' in tipo or 'sanitar' in tipo or 'ospedale' in nome or 'mozambico' in nome:
            return 'ITA - Umanitaria'
        if 'antiterrorismo' in tipo or 'marittima' in tipo or 'golfo' in nome:
            return 'ITA - Sicurezza'
        return 'ITA - Altro'
    # Multilaterale/Ibrida
    if ('bosnia' in nome and ('nato' in fonte or 'ue' in fonte)) or ('althea' in nome and 'nato' in fonte):
        return 'Multilaterale - Ibrida'
    if 'unifil' in nome and 'onu' in fonte and ('ita' in note or 'italia' in note):
        return 'Multilaterale - Ibrida'
    return 'Altro'

def classifica_dataset(df):
    df['Tipologia'] = df.apply(classifica_missione, axis=1)
    return df

def salva_dati_finali(df: pd.DataFrame):
    """Salva il dataset finale"""
    logger = logging.getLogger(__name__)
    
    if df.empty:
        logger.warning("Nessun dato da salvare")
        return
    
    try:
        # Classifica prima di salvare
        df = classifica_dataset(df)
        final_dir = Path('data/final')
        final_dir.mkdir(parents=True, exist_ok=True)
        
        # Salva in CSV
        csv_path = final_dir / f'missioni_internazionali_{datetime.now().strftime("%Y%m%d")}.csv'
        df.to_csv(csv_path, index=False)
        logger.info(f"Dataset salvato in CSV: {csv_path}")
        
        # Salva in Excel
        excel_path = final_dir / f'missioni_internazionali_{datetime.now().strftime("%Y%m%d")}.xlsx'
        df.to_excel(excel_path, index=False)
        logger.info(f"Dataset salvato in Excel: {excel_path}")
        
    except Exception as e:
        logger.error(f"Errore durante il salvataggio dei dati: {str(e)}")

def main():
    """Funzione principale per l'estrazione e l'elaborazione dei dati."""
    logger = logging.getLogger(__name__)
    logger.info("Avvio processo di estrazione dati")
    
    # Inizializza validatore
    validator = DataValidator()
    
    # Crea directory necessarie
    for dir_path in ['data/raw', 'data/processed', 'data/final', 'logs']:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    # Inizializza scrapers
    scrapers = {
        'camera': CameraScraper(),
        'senato': SenatoScraper(),
        'difesa': DifesaScraper(),
        'esteri': EsteriScraper(),
        'eeas': EEASScraper(),
        'nato': NATOScraper(),
        'un': UNScraper()
    }
    
    # Esegui scraping per ogni fonte
    dati_completi = []
    for nome, scraper in scrapers.items():
        try:
            logger.info(f"Estrazione dati da {nome}")
            df = scraper.estrai_dati()
            
            # Valida dati
            is_valid, errori = validator.valida_dataframe(df, nome)
            if not is_valid:
                logger.warning(f"Errori di validazione per {nome}: {errori}")
                continue
                
            dati_completi.append(df)
            logger.info(f"Estrazione completata per {nome}")
            
        except Exception as e:
            logger.error(f"Errore durante l'estrazione da {nome}: {str(e)}")
    
    if not dati_completi:
        logger.error("Nessun dato estratto con successo")
        return
    
    # Unisci dati
    df_finale = pd.concat(dati_completi, ignore_index=True)
    
    # Rimuovi duplicati
    df_finale = df_finale.drop_duplicates(subset=['nome_missione', 'paese'])
    
    # Salva risultati
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path('data/final')
    
    # CSV
    csv_path = output_dir / f"missioni_internazionali_{timestamp}.csv"
    df_finale.to_csv(csv_path, index=False, encoding='utf-8', sep=';')
    logger.info(f"Salvato CSV: {csv_path}")
    
    # Excel
    excel_path = output_dir / f"missioni_internazionali_{timestamp}.xlsx"
    df_finale.to_excel(excel_path, index=False)
    logger.info(f"Salvato Excel: {excel_path}")
    
    logger.info("Processo completato con successo")

if __name__ == "__main__":
    setup_logging()
    main() 