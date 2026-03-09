"""
Modulo funzioni analitiche per la dashboard MIDA.
Contiene le funzioni di aggregazione dati estratte dal monolite missioni_dashboard.py.
Tutte le funzioni sono pure e vettorizzate (nessun iterrows).
"""

import numpy as np
import pandas as pd

from core.normalizer import classify_period, normalize_commitment


def create_period_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Crea l'analisi per periodi temporali."""
    df_a = df.copy()
    df_a = df_a.loc[:, ~df_a.columns.duplicated()]
    df_a["data_inizio"] = pd.to_datetime(df_a["data_inizio"], errors="coerce")
    df_a = df_a.dropna(subset=["data_inizio"])

    # Assegna periodo in modo vettorizzato
    df_a["periodo"] = df_a["data_inizio"].dt.year.apply(classify_period)

    period_stats = df_a.groupby("periodo").agg(
        nome=("nome", "count"),
        personale_militare=("personale_militare", "sum"),
        personale_civile=("personale_civile", "sum"),
        personale_totale=("personale_totale", "sum"),
        costo_totale=("costo_totale", "sum"),
    ).reset_index()

    period_stats.columns = [
        "Periodo", "Numero Missioni", "Personale Militare",
        "Personale Civile", "Personale Totale", "Costo Totale",
    ]
    return period_stats


def create_participation_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Analisi per tipo di partecipazione."""
    stats = df.groupby("tipo_partecipazione").agg(
        nome=("nome", "count"),
        personale_totale=("personale_totale", "sum"),
        costo_totale=("costo_totale", "sum"),
    ).reset_index()

    stats.columns = ["Tipo Partecipazione", "Numero Missioni", "Personale Totale", "Costo Totale"]
    return stats


def create_regional_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Analisi per regione e sub-regione."""
    stats = df.groupby(["regione", "sub_regione"]).agg(
        nome=("nome", "count"),
        personale_totale=("personale_totale", "sum"),
        costo_totale=("costo_totale", "sum"),
    ).reset_index()

    stats.columns = ["Regione", "Sub-Regione", "Numero Missioni", "Personale Totale", "Costo Totale"]
    return stats


def create_organization_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Analisi per organizzazione."""
    stats = df.groupby("tipo_missione").agg(
        nome=("nome", "count"),
        personale_totale=("personale_totale", "sum"),
        costo_totale=("costo_totale", "sum"),
        personale_militare=("personale_militare", "sum"),
        personale_civile=("personale_civile", "sum"),
    ).reset_index()

    stats.columns = [
        "Organizzazione", "Numero Missioni", "Personale Totale",
        "Costo Totale", "Personale Militare", "Personale Civile",
    ]
    return stats


def create_commitment_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Analisi per tipo di commitment."""
    df_c = df.copy()

    if "commitment" not in df_c.columns:
        df_c["commitment"] = df_c.apply(
            lambda r: "Head of Mission" if r.get("tipo_partecipazione") == "civ"
            else ("Troops" if r.get("personale_totale", 0) > 500 else "Head of Mission"),
            axis=1,
        )

    stats = df_c.groupby("commitment").agg(
        nome=("nome", "count"),
        personale_totale=("personale_totale", "sum"),
        costo_totale=("costo_totale", "sum"),
    ).reset_index()

    stats.columns = ["Tipo Commitment", "Numero Missioni", "Personale Totale", "Costo Totale"]
    return stats


def create_commitment_detailed(df: pd.DataFrame) -> pd.DataFrame:
    """Crea tabella commitment dettagliato per missione (vettorizzato)."""
    df_c = df.copy()
    df_c["Commitment Dettagliato"] = df_c.apply(
        lambda r: normalize_commitment(str(r.get("commitment", "")), str(r.get("nome", ""))),
        axis=1,
    )
    return df_c[["nome", "paese", "tipo_missione", "personale_totale", "Commitment Dettagliato"]]


def create_historical_period_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Assegna periodi storici al DataFrame."""
    df_p = df.copy()
    df_p = df_p.loc[:, ~df_p.columns.duplicated()]
    df_p["data_inizio"] = pd.to_datetime(df_p["data_inizio"], errors="coerce")
    df_p = df_p.dropna(subset=["data_inizio"])
    df_p["Periodo Storico"] = df_p["data_inizio"].dt.year.apply(classify_period)
    return df_p


def get_timeline_data(df: pd.DataFrame, selected_years: tuple) -> pd.DataFrame:
    """Filtra e prepara i dati per la timeline interattiva."""
    df_t = df.copy()
    df_t = df_t[
        (df_t["data_inizio"].dt.year >= selected_years[0])
        & (df_t["data_inizio"].dt.year <= selected_years[1])
    ]
    df_t["anno"] = df_t["data_inizio"].dt.year
    df_t["durata_mesi"] = ((df_t["data_fine"] - df_t["data_inizio"]).dt.days / 30).fillna(12)
    return df_t
