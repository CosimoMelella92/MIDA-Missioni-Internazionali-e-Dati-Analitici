import pandas as pd
import logging
import yaml
from datetime import datetime
from typing import Dict, List, Tuple, Any
import re

class DataValidator:
    """Classe per la validazione dei dati estratti."""
    
    def __init__(self):
        """Inizializza il validatore."""
        with open('config/config.yaml', 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        self.logger = logging.getLogger(__name__)
        
    def _valida_formato_data(self, data_str: str) -> bool:
        """Valida il formato di una data."""
        for formato in self.config['validazione']['formati_data']:
            try:
                datetime.strptime(data_str, formato)
                return True
            except ValueError:
                continue
        return False
    
    def _valida_formato_costo(self, costo_str: str) -> bool:
        """Valida il formato di un costo."""
        if not isinstance(costo_str, str):
            return False
            
        # Rimuovi simboli e spazi
        costo_clean = re.sub(r'[€\s]', '', costo_str)
        try:
            float(costo_clean)
            return True
        except ValueError:
            return False
    
    def _valida_paese(self, paese: str) -> bool:
        """Valida il formato del paese."""
        if not isinstance(paese, str):
            return False
        return len(paese.strip()) > 0 and paese.strip().isalpha()
    
    def _valida_personale(self, personale: Any) -> bool:
        """Valida il formato del personale totale."""
        try:
            num = int(personale)
            return num >= 0
        except (ValueError, TypeError):
            return False
    
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
            date_invalide = df[~df['data_inizio'].apply(self._valida_formato_data)]['data_inizio'].unique()
            if len(date_invalide) > 0:
                errori.append(f"Date non valide trovate: {date_invalide}")
        
        if 'data_fine' in df.columns:
            date_invalide = df[~df['data_fine'].apply(self._valida_formato_data)]['data_fine'].unique()
            if len(date_invalide) > 0:
                errori.append(f"Date di fine non valide trovate: {date_invalide}")
        
        # Verifica formato costi
        if 'costo_totale' in df.columns:
            costi_invalidi = df[~df['costo_totale'].apply(self._valida_formato_costo)]['costo_totale'].unique()
            if len(costi_invalidi) > 0:
                errori.append(f"Costi non validi trovati: {costi_invalidi}")
        
        # Verifica paesi
        if 'paese' in df.columns:
            paesi_invalidi = df[~df['paese'].apply(self._valida_paese)]['paese'].unique()
            if len(paesi_invalidi) > 0:
                errori.append(f"Paesi non validi trovati: {paesi_invalidi}")
        
        # Verifica personale
        if 'personale_totale' in df.columns:
            personale_invalido = df[~df['personale_totale'].apply(self._valida_personale)]['personale_totale'].unique()
            if len(personale_invalido) > 0:
                errori.append(f"Valori personale non validi trovati: {personale_invalido}")
        
        # Verifica tipi missione
        if 'tipo_missione' in df.columns:
            tipi_validi = self.config['classificazione']['tipi_missione']
            tipi_invalidi = df[~df['tipo_missione'].isin(tipi_validi)]['tipo_missione'].unique()
            if len(tipi_invalidi) > 0:
                errori.append(f"Tipi missione non validi: {tipi_invalidi}")
        
        # Verifica coerenza date
        if 'data_inizio' in df.columns and 'data_fine' in df.columns:
            df['data_inizio_dt'] = pd.to_datetime(df['data_inizio'])
            df['data_fine_dt'] = pd.to_datetime(df['data_fine'])
            date_incoerenti = df[df['data_inizio_dt'] > df['data_fine_dt']]
            if not date_incoerenti.empty:
                errori.append(f"Date incoerenti trovate: {len(date_incoerenti)} righe")
        
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
        if 'data_inizio' in missione and not self._valida_formato_data(missione['data_inizio']):
            errori.append(f"Formato data non valido: {missione['data_inizio']}")
            
        if 'data_fine' in missione and not self._valida_formato_data(missione['data_fine']):
            errori.append(f"Formato data fine non valido: {missione['data_fine']}")
        
        # Verifica formato costi
        if 'costo_totale' in missione and not self._valida_formato_costo(missione['costo_totale']):
            errori.append(f"Formato costo non valido: {missione['costo_totale']}")
        
        # Verifica paese
        if 'paese' in missione and not self._valida_paese(missione['paese']):
            errori.append(f"Paese non valido: {missione['paese']}")
        
        # Verifica personale
        if 'personale_totale' in missione and not self._valida_personale(missione['personale_totale']):
            errori.append(f"Personale totale non valido: {missione['personale_totale']}")
        
        # Verifica tipo missione
        if 'tipo_missione' in missione:
            if missione['tipo_missione'] not in self.config['classificazione']['tipi_missione']:
                errori.append(f"Tipo missione non valido: {missione['tipo_missione']}")
        
        # Verifica coerenza date
        if 'data_inizio' in missione and 'data_fine' in missione:
            try:
                data_inizio = datetime.strptime(missione['data_inizio'], self.config['validazione']['formati_data'][0])
                data_fine = datetime.strptime(missione['data_fine'], self.config['validazione']['formati_data'][0])
                if data_inizio > data_fine:
                    errori.append("Data inizio successiva alla data fine")
            except ValueError:
                pass
        
        return len(errori) == 0, errori 