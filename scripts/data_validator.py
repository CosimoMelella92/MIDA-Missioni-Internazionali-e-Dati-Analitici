import pandas as pd
import logging
import yaml
from datetime import datetime
from typing import Dict, List, Tuple

class DataValidator:
    """Classe per la validazione dei dati estratti."""
    
    def __init__(self):
        """Inizializza il validatore."""
        with open('config/config.yaml', 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        self.logger = logging.getLogger(__name__)
        
    def valida_dataframe(self, df: pd.DataFrame, fonte: str) -> Tuple[bool, List[str]]:
        """
        Valida un DataFrame secondo le regole configurate.
        
        Args:
            df: DataFrame da validare
            fonte: Nome della fonte dei dati
            
        Returns:
            Tuple[bool, List[str]]: (validità, lista errori)
        """
        errori = []
        
        # Verifica campi obbligatori
        for campo in self.config['validazione']['campi_obbligatori']:
            if campo not in df.columns:
                errori.append(f"Campo obbligatorio mancante: {campo}")
            elif df[campo].isnull().any():
                errori.append(f"Valori nulli nel campo: {campo}")
        
        # Verifica formato date
        if 'data_inizio' in df.columns:
            for formato in self.config['validazione']['formati_data']:
                try:
                    pd.to_datetime(df['data_inizio'], format=formato)
                except ValueError:
                    errori.append(f"Formato data non valido in data_inizio")
        
        # Verifica formato costi
        if 'costo_totale' in df.columns:
            for formato in self.config['validazione']['formati_costo']:
                try:
                    df['costo_totale'].str.replace('€', '').str.replace('EUR', '').astype(float)
                except ValueError:
                    errori.append(f"Formato costo non valido in costo_totale")
        
        # Verifica tipi missione
        if 'tipo_missione' in df.columns:
            tipi_validi = self.config['classificazione']['tipi_missione']
            tipi_invalidi = df[~df['tipo_missione'].isin(tipi_validi)]['tipo_missione'].unique()
            if len(tipi_invalidi) > 0:
                errori.append(f"Tipi missione non validi: {tipi_invalidi}")
        
        # Log risultati
        if errori:
            self.logger.warning(f"Validazione fallita per {fonte}: {errori}")
            return False, errori
        else:
            self.logger.info(f"Validazione completata con successo per {fonte}")
            return True, []
    
    def valida_missione(self, missione: Dict) -> Tuple[bool, List[str]]:
        """
        Valida una singola missione.
        
        Args:
            missione: Dizionario con i dati della missione
            
        Returns:
            Tuple[bool, List[str]]: (validità, lista errori)
        """
        errori = []
        
        # Verifica campi obbligatori
        for campo in self.config['validazione']['campi_obbligatori']:
            if campo not in missione or not missione[campo]:
                errori.append(f"Campo obbligatorio mancante: {campo}")
        
        # Verifica formato date
        if 'data_inizio' in missione:
            try:
                datetime.strptime(missione['data_inizio'], self.config['validazione']['formati_data'][0])
            except ValueError:
                errori.append(f"Formato data non valido: {missione['data_inizio']}")
        
        # Verifica formato costi
        if 'costo_totale' in missione:
            try:
                float(missione['costo_totale'].replace('€', '').replace('EUR', ''))
            except ValueError:
                errori.append(f"Formato costo non valido: {missione['costo_totale']}")
        
        # Verifica tipo missione
        if 'tipo_missione' in missione:
            if missione['tipo_missione'] not in self.config['classificazione']['tipi_missione']:
                errori.append(f"Tipo missione non valido: {missione['tipo_missione']}")
        
        return len(errori) == 0, errori 