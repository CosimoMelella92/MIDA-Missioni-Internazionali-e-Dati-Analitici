import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from pathlib import Path
import logging
from datetime import datetime

class DataProcessor:
    def __init__(self, excel_path: str):
        self.excel_path = Path(excel_path)
        self.df = None
        self.logger = logging.getLogger(__name__)
        
    def load_data(self) -> None:
        """Carica il file Excel e pulisce la struttura iniziale."""
        try:
            # Leggi il file Excel saltando la prima riga (codici)
            self.df = pd.read_excel(self.excel_path, skiprows=1)
            
            # Pulisci i nomi delle colonne
            self.df.columns = self.df.columns.str.strip()
            
            # Converti i tipi di dati
            self._convert_data_types()
            
            self.logger.info(f"File Excel caricato con successo: {len(self.df)} righe")
        except Exception as e:
            self.logger.error(f"Errore nel caricamento del file Excel: {str(e)}")
            raise
    
    def _convert_data_types(self) -> None:
        """Converte i tipi di dati delle colonne."""
        try:
            # Converti date
            date_columns = ['Starting Year', 'End/Extension Year']
            for col in date_columns:
                if col in self.df.columns:
                    # Prima prova a convertire direttamente
                    try:
                        self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                    except:
                        # Se fallisce, prova a estrarre l'anno
                        self.df[col] = pd.to_datetime(self.df[col].astype(str).str.extract('(\d{4})')[0], format='%Y', errors='coerce')
            
            # Converti booleani
            bool_columns = ['Ended mission', 'Coop_UN', 'Coop_NATO', 'Coop_ASEAN', 'Coop_AU']
            for col in bool_columns:
                if col in self.df.columns:
                    self.df[col] = self.df[col].map({'Yes': True, 'No': False, 'yes': True, 'no': False, 1: True, 0: False})
                    # Converti i valori mancanti in False
                    self.df[col] = self.df[col].fillna(False)
            
            # Converti numerici
            numeric_columns = ['Length (months)', 'Number of EU States at Time of Launch',
                             'Peak Number of EU States', 'Proportional Number of States',
                             'Absolute Recorded Maximum Personnel']
            for col in numeric_columns:
                if col in self.df.columns:
                    # Rimuovi eventuali caratteri non numerici
                    self.df[col] = self.df[col].astype(str).str.replace(r'[^\d.]', '', regex=True)
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
            
            # Converti testo
            text_columns = ['Mission Name', 'Type', 'Notes']
            for col in text_columns:
                if col in self.df.columns:
                    self.df[col] = self.df[col].astype(str).str.strip()
                    # Sostituisci 'nan' con stringa vuota
                    self.df[col] = self.df[col].replace('nan', '')
            
        except Exception as e:
            self.logger.error(f"Errore nella conversione dei tipi di dati: {str(e)}")
            raise
    
    def add_mission_data(self, mission_name: str, new_data: Dict) -> None:
        """Aggiunge o aggiorna i dati di una missione specifica."""
        if self.df is None:
            raise ValueError("Devi prima caricare i dati con load_data()")
        
        # Cerca la missione (case insensitive)
        mask = self.df['Mission Name'].str.lower() == mission_name.lower()
        if not any(mask):
            self.logger.warning(f"Missione non trovata: {mission_name}")
            return
        
        for key, value in new_data.items():
            if key in self.df.columns:
                # Converti il valore nel tipo corretto
                if pd.api.types.is_datetime64_any_dtype(self.df[key]):
                    try:
                        value = pd.to_datetime(value)
                    except:
                        self.logger.warning(f"Impossibile convertire {value} in data per la colonna {key}")
                        continue
                elif pd.api.types.is_bool_dtype(self.df[key]):
                    value = bool(value)
                elif pd.api.types.is_numeric_dtype(self.df[key]):
                    try:
                        value = float(value)
                    except:
                        self.logger.warning(f"Impossibile convertire {value} in numero per la colonna {key}")
                        continue
                
                self.df.loc[mask, key] = value
    
    def save_data(self, output_path: Optional[str] = None) -> None:
        """Salva i dati in un nuovo file Excel."""
        if self.df is None:
            raise ValueError("Nessun dato da salvare")
        
        output_path = output_path or self.excel_path.parent / f"{self.excel_path.stem}_enriched.xlsx"
        
        # Crea una copia del DataFrame per il salvataggio
        df_to_save = self.df.copy()
        
        # Formatta le date
        date_columns = ['Starting Year', 'End/Extension Year']
        for col in date_columns:
            if col in df_to_save.columns and pd.api.types.is_datetime64_any_dtype(df_to_save[col]):
                df_to_save[col] = df_to_save[col].dt.strftime('%Y-%m-%d')
        
        # Salva il file
        df_to_save.to_excel(output_path, index=False)
        self.logger.info(f"Dati salvati in: {output_path}")
    
    def get_mission_data(self, mission_name: str) -> Dict:
        """Recupera i dati di una missione specifica."""
        if self.df is None:
            raise ValueError("Devi prima caricare i dati con load_data()")
        
        mission_data = self.df[self.df['Mission Name'].str.lower() == mission_name.lower()]
        if mission_data.empty:
            return {}
        
        return mission_data.iloc[0].to_dict()
    
    def get_all_missions(self) -> List[str]:
        """Recupera la lista di tutte le missioni."""
        if self.df is None:
            raise ValueError("Devi prima caricare i dati con load_data()")
        
        return self.df['Mission Name'].unique().tolist() 