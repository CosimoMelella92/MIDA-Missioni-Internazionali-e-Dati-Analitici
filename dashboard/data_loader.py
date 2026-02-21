"""
Modulo di caricamento dati per la dashboard MIDA.
Gestisce il caricamento, la cache e la preparazione dei dati per Streamlit.
Sostituisce load_data(), integrate_excel_data(), normalize_regions(),
normalize_excel_columns() dal monolite missioni_dashboard.py.
"""

import streamlit as st
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime
import sys
import os

# Aggiungi la root del progetto al path
_project_root = Path(__file__).parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from core.normalizer import (
    normalize_region,
    normalize_organization,
    normalize_commitment,
)

logger = logging.getLogger(__name__)

# Percorsi dati
DATA_DIR = _project_root / "data"
PROCESSED_DIR = DATA_DIR / "processed"
PRIMARY_CSV = PROCESSED_DIR / "missioni_complete.csv"
FALLBACK_CSV = PROCESSED_DIR / "missioni.csv"


@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame | None:
    """
    Carica e prepara i dati delle missioni.
    Cache di 1 ora per dati raw; le normalizzazioni sono deterministiche.
    """
    df = _load_raw_data()
    if df is None:
        return None

    df = _normalize_all(df)
    df = _ensure_schema(df)
    df = _compute_derived_fields(df)
    return df


def _load_raw_data() -> pd.DataFrame | None:
    """Carica il CSV principale con fallback."""
    for path in [PRIMARY_CSV, FALLBACK_CSV]:
        if path.exists():
            try:
                df = pd.read_csv(path)
                logger.info(f"Dati caricati da {path}: {len(df)} righe")
                return df
            except Exception as e:
                logger.error(f"Errore nel caricamento di {path}: {e}")
    logger.error("Nessun file dati trovato")
    return None


def _normalize_all(df: pd.DataFrame) -> pd.DataFrame:
    """Applica tutte le normalizzazioni in modo vettorizzato."""
    # Regioni
    if "regione" in df.columns:
        df["regione"] = df["regione"].apply(normalize_region)

    # Organizzazioni
    if "tipo_missione" in df.columns and "nome" in df.columns:
        df["tipo_missione"] = df.apply(
            lambda r: normalize_organization(str(r.get("nome", "")), str(r.get("tipo_missione", ""))),
            axis=1,
        )

    # Commitment
    if "commitment" in df.columns:
        df["commitment"] = df.apply(
            lambda r: normalize_commitment(str(r.get("commitment", "")), str(r.get("nome", ""))),
            axis=1,
        )
        df["commitment"] = df["commitment"].str.strip()

    # tipo_partecipazione: fill NaN con "civmil"
    if "tipo_partecipazione" in df.columns:
        df["tipo_partecipazione"] = df["tipo_partecipazione"].fillna("civmil")

    # sub_regione: fill NaN
    if "sub_regione" in df.columns:
        df["sub_regione"] = df["sub_regione"].fillna("Non specificata")

    # paese: fill NaN
    if "paese" in df.columns:
        df["paese"] = df["paese"].fillna("Non specificato")

    return df


def _ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Assicura che tutte le colonne essenziali esistano con valori di default."""
    schema_defaults = {
        "nome": "",
        "paese": "Non specificato",
        "regione": "Non specificata",
        "sub_regione": "Non specificata",
        "tipo_partecipazione": "civmil",
        "personale_militare": 0.0,
        "personale_civile": 0.0,
        "personale_totale": 0.0,
        "costo_totale": 0.0,
        "tipo_missione": "Altro",
        "commitment": "Troops",
        "is_active": False,
    }

    for col, default in schema_defaults.items():
        if col not in df.columns:
            if col == "personale_militare" and "personale_totale" in df.columns:
                df[col] = df["personale_totale"] * 0.7
            elif col == "personale_civile" and "personale_totale" in df.columns:
                df[col] = df["personale_totale"] * 0.3
            elif col == "personale_totale" and "personale" in df.columns:
                df[col] = df["personale"]
            else:
                df[col] = default

    # Rimuovi colonne duplicate
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def _compute_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Calcola campi derivati: date, is_active, personale_totale."""
    # Converti date
    df["data_inizio"] = pd.to_datetime(df["data_inizio"], errors="coerce")
    df["data_fine"] = pd.to_datetime(df["data_fine"], errors="coerce")

    current_date = pd.Timestamp.now()

    # Calcola is_active se non presente o se serve aggiornamento
    if "is_active" not in df.columns:
        df["is_active"] = False

    # Converti is_active in booleano robusto
    df["is_active"] = df["is_active"].map(
        {True: True, False: False, "True": True, "False": False, 1: True, 0: False}
    ).fillna(False).astype(bool)

    # Estendi data_fine per missioni attive recenti senza data_fine
    mask_extend = (
        df["data_inizio"].notna()
        & (df["data_fine"].isna() | (df["data_fine"] <= current_date))
        & ((current_date - df["data_inizio"]).dt.days < 1825)
    )
    df.loc[mask_extend, "data_fine"] = pd.Timestamp("2025-12-31")

    # Calcola personale_totale se mancante
    mask_no_total = (df["personale_totale"] == 0) & (
        (df["personale_militare"] > 0) | (df["personale_civile"] > 0)
    )
    df.loc[mask_no_total, "personale_totale"] = (
        df.loc[mask_no_total, "personale_militare"] + df.loc[mask_no_total, "personale_civile"]
    )

    # Converti numerici
    for col in ["personale_militare", "personale_civile", "personale_totale", "costo_totale"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def get_data_stats(df: pd.DataFrame) -> dict:
    """Restituisce statistiche di riepilogo sui dati caricati."""
    if df is None or df.empty:
        return {}
    return {
        "total_missions": len(df),
        "active_missions": int(df["is_active"].sum()),
        "invalid_dates": int(df["data_inizio"].isna().sum()),
        "missing_fields": int(df[["nome", "paese", "tipo_missione"]].isna().any(axis=1).sum()),
        "organizations": sorted(df["tipo_missione"].unique().tolist()),
        "regions": sorted(df["regione"].unique().tolist()),
        "date_range": (
            df["data_inizio"].min().strftime("%Y-%m-%d") if df["data_inizio"].notna().any() else "N/A",
            df["data_fine"].max().strftime("%Y-%m-%d") if df["data_fine"].notna().any() else "N/A",
        ),
    }
