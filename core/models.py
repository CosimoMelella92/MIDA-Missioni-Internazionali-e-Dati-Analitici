"""
Modelli dati Pydantic per MIDA - Missioni Internazionali e Dati Analitici.
Definisce lo schema canonico per le missioni e le fonti dati.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, Literal
from datetime import date, datetime
from enum import Enum
import re


class Organizzazione(str, Enum):
    ONU = "ONU"
    NATO = "NATO"
    UE = "UE"
    ITA = "ITA"
    MULTINATIONAL = "Multinational"
    BILATERAL = "Bilateral"
    COALIZIONE = "Coalizione"
    ALTRO = "Altro"


class TipoPartecipazione(str, Enum):
    MILITARE = "mil"
    CIVILE = "civ"
    CIVMIL = "civmil"


class CommitmentType(str, Enum):
    HEAD_OF_MISSION = "Head of Mission"
    TROOPS_GROUND = "Troops (ground forces)"
    TROOPS_NAVAL = "Troops (naval)"
    TROOPS_AIR = "Troops (air)"
    TROOPS_LOGISTICAL = "Troops (logistical support)"
    ADVISORY_TRAINING = "Advisory/Training"
    LOGISTICAL_SUPPORT = "Logistical Support & Advisory"
    TROOPS = "Troops"


class Regione(str, Enum):
    AFRICA = "Africa"
    EUROPA = "Europa"
    MEDIO_ORIENTE = "Medio Oriente"
    ASIA = "Asia"
    AMERICA = "America"
    NON_SPECIFICATA = "Non specificata"


class Mission(BaseModel):
    """Modello canonico per una missione internazionale."""

    nome: str = Field(..., min_length=1, description="Nome della missione")
    paese: str = Field(..., min_length=1, description="Paese di svolgimento")
    regione: str = Field(default="Non specificata", description="Macro-regione geografica")
    sub_regione: str = Field(default="Non specificata", description="Sub-regione geografica")
    tipo_partecipazione: str = Field(default="civmil", description="Tipo di partecipazione (mil/civ/civmil)")
    data_inizio: Optional[date] = Field(default=None, description="Data di inizio missione")
    data_fine: Optional[date] = Field(default=None, description="Data di fine missione")
    personale_militare: float = Field(default=0.0, ge=0, description="Personale militare impiegato")
    personale_civile: float = Field(default=0.0, ge=0, description="Personale civile impiegato")
    personale_totale: float = Field(default=0.0, ge=0, description="Personale totale impiegato")
    costo_totale: float = Field(default=0.0, ge=0, description="Costo totale in euro")
    tipo_missione: str = Field(default="Altro", description="Organizzazione/framework (ONU, NATO, UE, ...)")
    commitment: str = Field(default="Troops", description="Tipo di commitment")
    is_active: bool = Field(default=False, description="Se la missione è attualmente attiva")
    fonte_dati: str = Field(default="", description="Fonte originale del dato (file/scraper)")
    dati_stimati: bool = Field(default=False, description="True se i dati numerici sono stime/default")

    @field_validator("nome", "paese", mode="before")
    @classmethod
    def strip_strings(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("data_inizio", "data_fine", mode="before")
    @classmethod
    def parse_date(cls, v):
        import pandas as pd
        # Handle None, NaN, NaT
        if v is None:
            return None
        if isinstance(v, float) and (str(v) == "nan" or v != v):
            return None
        try:
            if pd.isna(v):
                return None
        except (ValueError, TypeError):
            pass
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            v = v.strip()
            if not v or v.lower() in ("nan", "nat", "none", ""):
                return None
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d", "%d.%m.%Y"):
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue
            # Prova a estrarre solo l'anno
            match = re.search(r"(\d{4})", str(v))
            if match:
                return date(int(match.group(1)), 1, 1)
        # Prova conversione tramite pandas Timestamp
        try:
            ts = pd.Timestamp(v)
            if pd.isna(ts):
                return None
            return ts.date()
        except Exception:
            return None

    @model_validator(mode="after")
    def compute_totale(self):
        if self.personale_totale == 0 and (self.personale_militare > 0 or self.personale_civile > 0):
            self.personale_totale = self.personale_militare + self.personale_civile
        return self

    @model_validator(mode="after")
    def check_date_coherence(self):
        if self.data_inizio and self.data_fine and self.data_inizio > self.data_fine:
            # Scambia le date se invertite
            self.data_inizio, self.data_fine = self.data_fine, self.data_inizio
        return self


class SourceConfig(BaseModel):
    """Configurazione di una fonte dati Excel."""

    name: str = Field(..., description="Nome identificativo della fonte")
    path: str = Field(..., description="Percorso relativo al file")
    type: Literal["excel", "csv"] = Field(default="excel")
    skip_rows: int = Field(default=0, description="Righe da saltare in testa")
    sheet_name: Optional[str] = Field(default=None, description="Nome del foglio Excel")
    column_mapping: dict[str, str] = Field(default_factory=dict, description="Mapping colonne sorgente → schema canonico")
    priority: int = Field(default=1, ge=1, description="Priorità in caso di conflitto (1=massima)")
    default_org: str = Field(default="Altro", description="Organizzazione di default per questa fonte")


class PipelineResult(BaseModel):
    """Risultato dell'esecuzione della pipeline di aggregazione."""

    total_missions: int = 0
    sources_loaded: int = 0
    duplicates_removed: int = 0
    validation_errors: int = 0
    new_missions: int = 0
    updated_missions: int = 0
    warnings: list[str] = Field(default_factory=list)
