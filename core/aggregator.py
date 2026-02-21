"""
Pipeline centralizzata di aggregazione dati per MIDA.
Sostituisce la logica frammentata in:
- core/mergers/merge_excel.py
- core/processors/data_merger.py
- clean_duplicates_and_verify.py
- fix_mission_count.py
- dashboard/missioni_dashboard.py (integrate_excel_data)

Flusso: load_sources → normalize → deduplicate → enrich → validate → save
"""

import pandas as pd
import numpy as np
import yaml
import logging
from pathlib import Path
from datetime import date, datetime
from typing import Optional

from core.normalizer import (
    normalize_mission_name,
    normalize_mission_name_strict,
    extract_mission_acronym,
    normalize_organization,
    normalize_region,
    normalize_commitment,
    normalize_column_name,
)
from core.models import Mission, SourceConfig, PipelineResult


logger = logging.getLogger(__name__)


class ExcelAggregator:
    """
    Pipeline dichiarativa per aggregare dati da fonti Excel/CSV multiple.
    Usa una singola strategia di deduplicazione e normalizzazione centralizzata.
    """

    def __init__(self, sources_config_path: str = "config/sources.yaml", base_dir: Optional[str] = None):
        self.base_dir = Path(base_dir) if base_dir else Path(".")
        self.sources = self._load_sources_config(sources_config_path)
        self.result = PipelineResult()

    def _load_sources_config(self, config_path: str) -> list[SourceConfig]:
        """Carica la configurazione delle fonti dal file YAML."""
        full_path = self.base_dir / config_path
        if not full_path.exists():
            logger.warning(f"File configurazione fonti non trovato: {full_path}")
            return []

        with open(full_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        sources = []
        for src in config.get("sources", []):
            try:
                sources.append(SourceConfig(**src))
            except Exception as e:
                logger.warning(f"Errore nel parsing della fonte '{src.get('name', '?')}': {e}")
        
        # Ordina per priorità (1 = massima)
        sources.sort(key=lambda s: s.priority)
        logger.info(f"Caricate {len(sources)} configurazioni fonti")
        return sources

    # =========================================================================
    # STEP 1: CARICAMENTO
    # =========================================================================

    def load_sources(self) -> pd.DataFrame:
        """Carica e concatena i dati da tutte le fonti configurate."""
        all_frames = []

        for source in self.sources:
            df = self._load_single_source(source)
            if df is not None and not df.empty:
                all_frames.append(df)
                self.result.sources_loaded += 1
                logger.info(f"Fonte '{source.name}': {len(df)} righe caricate")

        if not all_frames:
            logger.warning("Nessuna fonte dati caricata")
            return pd.DataFrame()

        df_combined = pd.concat(all_frames, ignore_index=True)
        logger.info(f"Totale righe dopo concatenazione: {len(df_combined)}")
        return df_combined

    def _load_single_source(self, source: SourceConfig) -> Optional[pd.DataFrame]:
        """Carica un singolo file sorgente e applica il mapping colonne."""
        file_path = self.base_dir / source.path
        if not file_path.exists():
            logger.warning(f"File non trovato: {file_path}")
            return None

        try:
            if source.type == "csv":
                df = pd.read_csv(file_path)
            else:
                kwargs = {}
                if source.skip_rows > 0:
                    kwargs["skiprows"] = source.skip_rows
                if source.sheet_name:
                    kwargs["sheet_name"] = source.sheet_name
                df = pd.read_excel(file_path, **kwargs)

            # Pulisci nomi colonne
            df.columns = df.columns.str.strip()

            # Applica mapping colonne dalla configurazione
            if source.column_mapping:
                rename_map = {}
                for src_col, dst_col in source.column_mapping.items():
                    # Cerca la colonna nel DataFrame (case-insensitive)
                    for actual_col in df.columns:
                        if actual_col.strip().lower() == src_col.strip().lower():
                            rename_map[actual_col] = dst_col
                            break
                if rename_map:
                    df = df.rename(columns=rename_map)

            # Fallback: prova mapping automatico per colonne non ancora mappate
            auto_rename = {}
            for col in df.columns:
                canonical = normalize_column_name(col)
                if canonical != col and col not in auto_rename.values():
                    auto_rename[col] = canonical
            if auto_rename:
                df = df.rename(columns=auto_rename)

            # Aggiungi metadati fonte
            df["fonte_dati"] = source.name
            df["_source_priority"] = source.priority

            # Se manca tipo_missione, usa il default della fonte
            if "tipo_missione" not in df.columns:
                df["tipo_missione"] = source.default_org

            return df

        except Exception as e:
            logger.error(f"Errore nel caricamento di '{source.name}' ({file_path}): {e}")
            return None

    # =========================================================================
    # STEP 2: NORMALIZZAZIONE
    # =========================================================================

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applica tutte le normalizzazioni in modo vettorizzato."""
        if df.empty:
            return df

        df = df.copy()

        # Assicura colonne essenziali
        self._ensure_columns(df)

        # Filtra righe spazzatura (codici come v001, v002, nomi troppo corti)
        garbage_mask = df["nome"].astype(str).str.match(r'^v\d+$', case=False, na=False)
        if garbage_mask.any():
            logger.info(f"Rimosse {garbage_mask.sum()} righe spazzatura (codici vXXX)")
            df = df[~garbage_mask]

        # Normalizza nomi (per dedup)
        df["_nome_norm"] = df["nome"].apply(normalize_mission_name_strict)
        # Acronimo: estrae da parentesi per catturare duplicati come
        # "EU Training Mission Mali (EUTM Mali)" -> "eutmmali" == "EUTM Mali"
        df["_acronym_norm"] = df["nome"].apply(extract_mission_acronym)
        df["_paese_norm"] = df["paese"].astype(str).str.lower().str.strip()

        # Normalizza organizzazioni (vettorizzato)
        df["tipo_missione"] = df.apply(
            lambda row: normalize_organization(
                str(row.get("nome", "")),
                str(row.get("tipo_missione", ""))
            ),
            axis=1,
        )

        # Normalizza regioni (vettorizzato)
        df["regione"] = df["regione"].apply(normalize_region)

        # Normalizza commitment (vettorizzato)
        if "commitment" in df.columns:
            df["commitment"] = df.apply(
                lambda row: normalize_commitment(
                    str(row.get("commitment", "")),
                    str(row.get("nome", ""))
                ),
                axis=1,
            )

        # Converti date
        for col in ["data_inizio", "data_fine"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Converti numerici
        for col in ["personale_militare", "personale_civile", "personale_totale", "costo_totale"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # Flag dati stimati: se personale_totale e costo_totale sono entrambi 0 o NaN
        if "dati_stimati" not in df.columns:
            df["dati_stimati"] = (df["personale_totale"] == 0) & (df["costo_totale"] == 0)

        logger.info(f"Normalizzazione completata: {len(df)} righe")
        return df

    def _ensure_columns(self, df: pd.DataFrame) -> None:
        """Assicura che tutte le colonne essenziali esistano con valori di default."""
        defaults = {
            "nome": "",
            "paese": "Non specificato",
            "regione": "Non specificata",
            "sub_regione": "Non specificata",
            "tipo_partecipazione": "civmil",
            "data_inizio": pd.NaT,
            "data_fine": pd.NaT,
            "personale_militare": 0.0,
            "personale_civile": 0.0,
            "personale_totale": 0.0,
            "costo_totale": 0.0,
            "tipo_missione": "Altro",
            "commitment": "Troops",
            "is_active": False,
            "fonte_dati": "",
            "dati_stimati": False,
        }
        for col, default_val in defaults.items():
            if col not in df.columns:
                df[col] = default_val

        # Rimuovi righe senza nome
        df.dropna(subset=["nome"], inplace=True)
        mask_empty = df["nome"].astype(str).str.strip() == ""
        df.drop(df[mask_empty].index, inplace=True)

    # =========================================================================
    # STEP 3: DEDUPLICAZIONE (strategia unica)
    # =========================================================================

    def deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Deduplicazione a due passaggi:
        1. Chiave esatta: _nome_norm (es. "eutmmali" == "eutmmali")
        2. Chiave acronimo: se _acronym_norm di una riga matcha _nome_norm di un'altra,
           sono la stessa missione (es. "EU Training Mission Mali (EUTM Mali)" → "eutmmali"
           matcha "EUTM Mali" → "eutmmali")
        In caso di duplicati: vince la fonte con priorità più alta + più dati non-null.
        """
        if df.empty:
            return df

        initial_count = len(df)

        # Calcola completezza di ogni riga (numero di campi non-null/non-zero)
        data_cols = ["personale_totale", "costo_totale", "personale_militare",
                     "personale_civile", "data_inizio", "data_fine", "regione", "commitment"]
        existing_cols = [c for c in data_cols if c in df.columns]
        df["_completeness"] = df[existing_cols].notna().sum(axis=1)

        # Per le colonne numeriche, aggiungi bonus se > 0
        for col in ["personale_totale", "costo_totale"]:
            if col in df.columns:
                df["_completeness"] += (df[col] > 0).astype(int)

        # Ordina: priorità fonte (asc), completezza (desc)
        df = df.sort_values(
            ["_source_priority", "_completeness"],
            ascending=[True, False],
        )

        # Pass 1: dedup esatto su _nome_norm
        df_deduped = df.drop_duplicates(subset=["_nome_norm"], keep="first")
        removed_pass1 = initial_count - len(df_deduped)

        # Pass 2: dedup acronimo — cattura duplicati con nomi lunghi EU
        # Solo match SICURI: l'acronimo estratto da parentesi deve corrispondere
        # esattamente a un _nome_norm esistente.
        # Es: "EU Training Mission Mali (EUTM Mali)" → acronimo "eutmmali" == "EUTM Mali"
        # NON usa substring matching (troppi falsi positivi: "eufor" != "euforalthea")
        if "_acronym_norm" in df_deduped.columns:
            df_deduped = df_deduped.copy()
            norms = list(df_deduped["_nome_norm"])
            acronyms = list(df_deduped["_acronym_norm"])
            canonical_keys = list(norms)

            for i in range(len(norms)):
                acr_i = acronyms[i]
                nm_i = norms[i]
                if not acr_i or acr_i == nm_i:
                    continue  # nessun acronimo diverso dal nome
                for j in range(i):
                    nm_j = norms[j]
                    acr_j = acronyms[j]
                    if not nm_j or len(nm_j) < 4:
                        continue
                    # 1. Acronimo di i == nome di j (match esatto)
                    if acr_i == nm_j:
                        canonical_keys[i] = canonical_keys[j]
                        break
                    # 2. Acronimo di j == nome di i (match esatto)
                    if acr_j and acr_j == nm_i:
                        canonical_keys[i] = canonical_keys[j]
                        break
                    # 3. Acronimo di i == acronimo di j (entrambi hanno parentesi)
                    if acr_i and acr_j and acr_i == acr_j:
                        canonical_keys[i] = canonical_keys[j]
                        break
                    # 4. Acronimo di i inizia con nome di j (min 6 chars)
                    #    Es: "euforaltheabih" startswith "euforalthea"
                    if len(nm_j) >= 6 and acr_i.startswith(nm_j):
                        canonical_keys[i] = canonical_keys[j]
                        break
                    # 5. Nome di j inizia con acronimo di i (min 6 chars)
                    if len(acr_i) >= 6 and nm_j.startswith(acr_i):
                        canonical_keys[i] = canonical_keys[j]
                        break

            df_deduped["_dedup_key"] = canonical_keys
            df_deduped = df_deduped.sort_values(
                ["_source_priority", "_completeness"],
                ascending=[True, False],
            )
            df_deduped = df_deduped.drop_duplicates(subset=["_dedup_key"], keep="first")
            df_deduped.drop(columns=["_dedup_key"], inplace=True)

        removed = initial_count - len(df_deduped)
        self.result.duplicates_removed = removed
        if removed > 0:
            logger.info(
                f"Deduplicazione: rimossi {removed} duplicati "
                f"({initial_count} → {len(df_deduped)}, "
                f"pass1={removed_pass1}, pass2={removed - removed_pass1})"
            )

        return df_deduped

    # =========================================================================
    # STEP 4: ARRICCHIMENTO
    # =========================================================================

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Arricchisce i dati con campi calcolati."""
        if df.empty:
            return df

        df = df.copy()
        current_date = pd.Timestamp.now()

        # ── Correzioni dati vs fonti ufficiali difesa.it ──
        df = self._apply_official_corrections(df)

        # Riempi commitment NaN residui (fonti senza colonna commitment)
        if "commitment" in df.columns:
            df["commitment"] = df["commitment"].fillna("Troops")
            df.loc[df["commitment"].isin(["nan", "none", "nat", ""]), "commitment"] = "Troops"

        # Calcola personale_totale se mancante
        mask_no_total = (df["personale_totale"] == 0) & (
            (df["personale_militare"] > 0) | (df["personale_civile"] > 0)
        )
        df.loc[mask_no_total, "personale_totale"] = (
            df.loc[mask_no_total, "personale_militare"] + df.loc[mask_no_total, "personale_civile"]
        )

        # Calcola is_active:
        # - Attiva se data_fine è esplicitamente nel futuro (>= oggi)
        # - Attiva se data_fine è NaT (sconosciuta) E la fonte dice is_active=True
        # - Attiva se data_fine è NaT E il nome è nella lista ufficiale difesa.it 2026
        # - Inattiva se data_fine è nel passato (anche se fonte dice attiva)
        date_future = df["data_fine"].notna() & (df["data_fine"] >= current_date)
        date_expired = df["data_fine"].notna() & (df["data_fine"] < current_date)
        date_unknown = df["data_fine"].isna()

        # Match con lista ufficiale missioni attive (Ministero Difesa 2026)
        official_active = self._match_official_active(df)

        if "is_active" in df.columns:
            already_active = (
                df["is_active"]
                .astype(str).str.strip().str.lower()
                .isin(["true", "1", "1.0"])
            )
            # Lista ufficiale Difesa 2026 ha priorità massima (sovrascrive date scadute)
            # Attiva se: lista ufficiale, OPPURE (fonte dice attiva E non scaduta), OPPURE data futura
            df["is_active"] = (
                official_active | (already_active & ~date_expired) | date_future
            )
        else:
            # Senza flag fonte: attiva se lista ufficiale, data futura, o (data sconosciuta)
            df["is_active"] = official_active | date_future

        # Forza inattive le missioni storiche concluse (falsi positivi da NaT + keyword)
        concluded = df["nome"].str.lower().str.strip().isin([
            "provide comfort i", "provide comfort ii",
            "eunavfor med - sophia", "eunavfor med",
            "nato standing naval forces (storico)",
            "eumm georgia",  # missione civile UE, non in lista difesa.it operazioni militari
        ])
        df.loc[concluded, "is_active"] = False

        # Inietta missioni mancanti dalla lista ufficiale Difesa 2026
        df = self._inject_missing_official(df)

        # Dedup finale post-correzioni (le correzioni possono creare nomi identici)
        before = len(df)
        df = df.drop_duplicates(subset=["nome"], keep="first").reset_index(drop=True)
        if len(df) < before:
            logger.info(f"Dedup post-correzioni: rimossi {before - len(df)} duplicati")

        # Aggiungi timestamp
        df["ultimo_aggiornamento"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"Arricchimento completato: {df['is_active'].sum()} missioni attive su {len(df)}")
        return df

    # =========================================================================
    # CORREZIONI DATI vs FONTI UFFICIALI (difesa.it 2026)
    # =========================================================================

    # Correzioni puntuali: {nome_lower: {campo: valore_corretto}}
    # Fonte: https://www.difesa.it/operazionimilitari/op-intern-corso/
    OFFICIAL_CORRECTIONS = {
        # ── Paesi errati ──
        "mare sicuro": {"paese": "Mediterraneo", "regione": "Europa", "sub_regione": "Mediterraneo",
                        "tipo_missione": "ITA", "personale_totale": 700, "personale_militare": 700},
        "operazione levante": {"paese": "Palestina", "regione": "Medio Oriente",
                               "personale_totale": 200, "personale_militare": 200,
                               "tipo_missione": "ITA"},
        # ── Nomi paese EN→IT ──
        "enhanced forward presence - baltic guardian lettonia": {
            "paese": "Lettonia", "personale_totale": 250, "personale_militare": 250},
        "nato hq sarajevo": {"paese": "Bosnia ed Erzegovina", "personale_totale": 10, "personale_militare": 10},
        "miadit palestine": {"paese": "Palestina", "nome": "MIADIT Palestina"},
        "miadit somalia": {"paese": "Somalia"},
        "baltic eagle": {"paese": "Estonia", "nome": "Baltic Eagle III",
                         "personale_totale": 200, "personale_militare": 200},
        "eunavfor med - irini": {"paese": "Mediterraneo", "nome": "EUNAVFOR MED Irini",
                                 "personale_totale": 350, "personale_militare": 350},
        "nato standing naval forces med": {"paese": "Mediterraneo",
                                           "personale_totale": 300, "personale_militare": 300},
        "prima parthica": {"paese": "Iraq", "personale_totale": 900, "personale_militare": 900},
        "eutm mozambico": {"nome": "EUMAM Mozambico", "personale_totale": 15, "personale_militare": 15,
                          "paese": "Mozambico", "regione": "Africa"},
        "eumam mozambico": {"paese": "Mozambico", "regione": "Africa"},
        # ── Dati personale aggiornati (Delibera CdM 2024/2025) ──
        "unifil": {"personale_totale": 1200, "personale_militare": 1200, "paese": "Libano"},
        "kfor": {"personale_totale": 700, "personale_militare": 700, "paese": "Kosovo"},
        "nato mission iraq": {"personale_totale": 50, "personale_militare": 50, "paese": "Iraq"},
        "sea guardian": {"personale_totale": 280, "personale_militare": 280, "paese": "Mediterraneo"},
        "eufor althea": {"personale_totale": 100, "personale_militare": 100, "paese": "Bosnia ed Erzegovina"},
        "eulex kosovo": {"personale_totale": 30, "personale_civile": 30, "personale_militare": 0,
                         "tipo_partecipazione": "civ", "paese": "Kosovo"},
        "eunavfor atalanta": {"personale_totale": 300, "personale_militare": 300, "paese": "Oceano Indiano"},
        "atalanta": {"personale_totale": 300, "personale_militare": 300, "paese": "Oceano Indiano",
                     "nome": "EUNAVFOR Atalanta", "tipo_missione": "UE"},
        "eucap somalia": {"personale_totale": 15, "personale_civile": 15, "personale_militare": 0,
                          "tipo_partecipazione": "civ"},
        "eupol copps": {"personale_totale": 10, "personale_civile": 10, "personale_militare": 0,
                        "tipo_partecipazione": "civ", "paese": "Palestina"},
        "eubam rafah": {"personale_totale": 10, "personale_civile": 10, "personale_militare": 0,
                        "tipo_partecipazione": "civ", "paese": "Palestina"},
        "eunavfor aspides": {"personale_totale": 350, "personale_militare": 350, "paese": "Mar Rosso"},
        "emasoh": {"personale_totale": 150, "personale_militare": 150, "paese": "Stretto di Hormuz",
                   "tipo_missione": "Multinational"},
        "misin": {"personale_totale": 350, "personale_militare": 350, "paese": "Niger",
                  "tipo_missione": "Bilateral"},
        "miasit": {"personale_totale": 233, "personale_militare": 233, "paese": "Libia",
                   "tipo_missione": "Bilateral"},  # Delibera CdM scheda 6/2025: max 233
        "mibil": {"personale_totale": 200, "personale_militare": 200, "paese": "Libano",
                  "tipo_missione": "Bilateral"},
        "miccd": {"personale_totale": 30, "personale_militare": 30, "paese": "Malta",
                  "tipo_missione": "Bilateral"},
        "miadit somalia": {"personale_totale": 100, "personale_militare": 100, "paese": "Somalia",
                          "tipo_missione": "Bilateral"},
        "base gibuti": {"personale_totale": 300, "personale_militare": 300, "paese": "Gibuti",
                        "tipo_missione": "ITA", "nome": "BMIS Gibuti"},
        "minurso": {"personale_totale": 5, "personale_militare": 5, "paese": "Sahara Occidentale"},
        "unficyp": {"personale_totale": 4, "personale_militare": 4, "paese": "Cipro"},
        "unmogip": {"personale_totale": 7, "personale_militare": 7, "paese": "India/Pakistan"},
        "mfo": {"personale_totale": 75, "personale_militare": 75, "paese": "Egitto",
                "tipo_missione": "Multinational"},
        "untso": {"paese": "Israele", "personale_totale": 7, "personale_militare": 7},
        "euma armenia": {"personale_totale": 40, "personale_militare": 40, "paese": "Armenia",
                         "tipo_missione": "UE"},  # Delibera CdM scheda 14/2025: max 52 per 12 missioni civili
        "eutm somalia": {"personale_totale": 250, "personale_militare": 250, "paese": "Somalia"},
        "irini": {"nome": "EUNAVFOR MED Irini", "paese": "Mediterraneo",
                  "personale_totale": 350, "personale_militare": 350, "tipo_missione": "UE"},
        # ── Costi sproporzionati: dati coalizione, non quota italiana ──
        "operation inherent resolve": {"costo_totale": 150_000_000},  # quota ITA ~150M (non 12B coalizione)
        "nato ground forces europe": {"costo_totale": 80_000_000},    # quota ITA ~80M (non 1B NATO totale)
        "ctf153 mar rosso": {"paese": "Mar Rosso"},
    }

    # ── Date mancanti: fonti difesa.it, analisidifesa.it, EU CSDP factsheets ──
    # Formato: "nome_lower": {"data_inizio": "YYYY-MM-DD", "data_fine": "YYYY-MM-DD", "paese": "..."}
    DATE_CORRECTIONS = {
        # NATO
        "nato mission iraq": {"data_inizio": "2018-10-09"},
        "resolute support": {"data_inizio": "2015-01-01", "data_fine": "2021-09-07"},
        "unified protector": {"data_inizio": "2011-03-31", "data_fine": "2011-10-31"},
        "cieli ghiacciati": {"data_inizio": "2013-05-01", "data_fine": "2013-09-30"},
        "frontiera baltica": {"data_inizio": "2014-09-01", "data_fine": "2015-03-31"},
        "enhanced forward presence - baltic guardian lettonia": {"data_inizio": "2017-06-19"},
        "enhanced air policing bulgaria": {"data_inizio": "2014-09-01", "data_fine": "2022-12-31"},
        "northern ice": {"data_inizio": "2014-10-01", "data_fine": "2014-12-31"},
        "northern stork": {"data_inizio": "2015-10-01", "data_fine": "2015-12-31"},
        "northern lightning i": {"data_inizio": "2016-04-01", "data_fine": "2016-06-30"},
        "northern lightning ii": {"data_inizio": "2017-04-01", "data_fine": "2017-06-30"},
        "active fence": {"data_inizio": "2013-01-25", "data_fine": "2015-12-31"},
        "nato standing naval forces med": {"data_inizio": "1968-01-01"},
        "nato military liaison office belgrade": {"data_inizio": "2006-12-14"},
        "nato multinational battle group bulgaria": {"data_inizio": "2022-06-29"},
        "nato multinational battle group ungheria": {"data_inizio": "2022-06-29"},
        "air policing": {"data_inizio": "2004-03-29"},
        # ONU
        "white crane": {"data_inizio": "2010-01-19", "data_fine": "2010-03-31"},
        "caravella": {"data_inizio": "2010-01-19", "data_fine": "2010-02-28"},
        "unsmis": {"data_inizio": "2012-04-21", "data_fine": "2012-08-19"},
        # Bilateral / Nazionali
        "cyrene": {"data_inizio": "2011-04-18", "data_fine": "2011-10-31"},
        "miccd": {"data_inizio": "2020-01-01"},
        "ippocrate": {"data_inizio": "2016-09-01", "data_fine": "2020-12-31"},
        "emochm": {"data_inizio": "2022-07-01", "data_fine": "2023-12-31"},
        "task force cedri": {"data_inizio": "2020-08-04", "data_fine": "2021-06-30"},
        "operazione levante": {"data_inizio": "2023-10-15"},
        "mediterraneo sicuro": {"data_inizio": "2015-03-12"},
        # Multinazionali / Coalizione
        "prima parthica": {"data_inizio": "2014-10-01"},
        "task force takuba": {"data_inizio": "2020-07-15", "data_fine": "2022-06-30"},
        "mtc4l libano": {"data_inizio": "2023-01-01"},
        "ctf153 mar rosso": {"data_inizio": "2022-04-17"},
        # EU CSDP (fonti: EU factsheets, eeas.europa.eu)
        "aceh mission- amm": {"data_inizio": "2005-09-15", "data_fine": "2006-12-15", "paese": "Indonesia"},
        "eu advisory mission in iraq": {"data_inizio": "2017-10-16", "data_fine": "2024-06-30", "paese": "Iraq"},  # EUAM Iraq, conclusa
        "eu aviation security south sudan (euavsec south sudan)": {"data_inizio": "2012-06-18", "data_fine": "2014-01-17", "paese": "Sud Sudan"},
        "eu capacity building sahel niger (eucap niger)": {"data_inizio": "2012-08-16", "data_fine": "2024-06-30", "paese": "Niger"},
        "eu integrated rule of law mission iraq (eujust lex-iraq)": {"data_inizio": "2005-07-01", "data_fine": "2013-12-31", "paese": "Iraq"},
        "eu military advisory mission, central african republic, eumam rca": {"data_inizio": "2015-03-16", "data_fine": "2023-09-19", "paese": "Repubblica Centrafricana"},
        "eu military bridging mission (eufor tchad/rca)": {"data_inizio": "2008-01-28", "data_fine": "2009-03-15", "paese": "Ciad"},
        "eu military mission artemis, democratic republic of congo (drc)": {"data_inizio": "2003-06-12", "data_fine": "2003-09-01", "paese": "Repubblica Democratica del Congo"},
        "eu military mission concordia/ fyrom, former yugoslav republic of macedonia": {"data_inizio": "2003-03-31", "data_fine": "2003-12-15", "paese": "Macedonia del Nord"},
        "eu naval operation mediterranean sophia": {"data_inizio": "2015-06-22", "data_fine": "2020-03-31", "paese": "Mediterraneo"},
        "eu police mission afghanistan (eupol)": {"data_inizio": "2007-06-15", "data_fine": "2016-12-31", "paese": "Afghanistan"},
        "eu police mission bosnia and herzegovina (eupm bih)": {"data_inizio": "2003-01-01", "data_fine": "2012-06-30", "paese": "Bosnia Erzegovina"},
        "eu police mission former republic of yugoslavia proxima  (proxima/ fyrom) 1 and 2": {"data_inizio": "2003-12-15", "data_fine": "2005-12-14", "paese": "Macedonia del Nord"},
        "eu rule of law mission georgia (eujust themis)": {"data_inizio": "2004-07-16", "data_fine": "2005-07-14", "paese": "Georgia"},
        "eu security sector reform mission in guinea-bissau (eu-ssr)": {"data_inizio": "2008-06-12", "data_fine": "2010-09-30", "paese": "Guinea-Bissau"},
        "eu support to amis (darfur)": {"data_inizio": "2005-07-18", "data_fine": "2007-12-31", "paese": "Sudan"},
        "eu military training mission in mozambique (eutm monzambique)": {"data_inizio": "2021-11-15", "data_fine": "2024-09-30", "paese": "Mozambico"},
        "eudel": {"data_inizio": "2011-05-22", "data_fine": "2012-01-31", "paese": "Libia"},
        "eudel libya": {"data_inizio": "2013-05-22", "data_fine": "2015-02-28", "paese": "Libia"},
        "eufor rca": {"data_inizio": "2014-04-01", "data_fine": "2015-03-15", "paese": "Repubblica Centrafricana"},
        "joint operation themis": {"data_inizio": "2018-02-01", "data_fine": "2023-12-31", "paese": "Mediterraneo"},  # Frontex/UE, conclusa
        "eunavfor med - sophia": {"data_inizio": "2015-06-22", "data_fine": "2020-03-31"},
        "eumam ucraina": {"data_inizio": "2022-11-15"},
        # ── Missioni storiche (pre-2010) — fonti: difesa.it archivio, Wikipedia, EU CSDP ──
        # Balcani anni '90
        "upfm": {"data_inizio": "1998-01-15", "data_fine": "2000-06-30"},  # UN Police Force Macedonia
        "deliberate force": {"data_inizio": "1995-08-30", "data_fine": "1995-09-20"},
        "alba": {"data_inizio": "1997-04-15", "data_fine": "1997-08-12"},  # Op Alba Albania
        "28esimo gruppo navale": {"data_inizio": "1997-03-14", "data_fine": "1997-08-20"},
        "mape": {"data_inizio": "1997-05-15", "data_fine": "2001-05-31"},  # Multinational Advisory Police Element
        "unmibh (iptf)": {"data_inizio": "1995-12-21", "data_fine": "2002-12-31"},
        "die": {"data_inizio": "1997-11-01", "data_fine": "2001-12-31"},  # Delegazione Italiana Esperti Albania
        "eagle eye": {"data_inizio": "1998-10-15", "data_fine": "1999-03-24"},  # Kosovo verification
        "joint guarantor": {"data_inizio": "1998-10-15", "data_fine": "1999-03-24"},
        "allied force": {"data_inizio": "1999-03-24", "data_fine": "1999-06-10"},  # Kosovo bombing
        "allied harbour": {"data_inizio": "1999-04-04", "data_fine": "1999-09-01"},  # Albania refugees
        "allied harvest": {"data_inizio": "1999-06-12", "data_fine": "1999-10-31"},  # Adriatic mine clearing
        "albit": {"data_inizio": "1999-09-01", "data_fine": "2002-12-31"},  # Albania bilateral
        "essential harvest": {"data_inizio": "2001-08-22", "data_fine": "2001-09-26"},  # Macedonia
        "amber fox": {"data_inizio": "2001-09-27", "data_fine": "2003-03-31"},  # Macedonia
        "allied harmony": {"data_inizio": "2002-12-16", "data_fine": "2003-03-31"},  # Macedonia
        "eupm": {"data_inizio": "2003-01-01", "data_fine": "2012-06-30"},  # EU Police Mission BiH
        "eu concordia": {"data_inizio": "2003-03-31", "data_fine": "2003-12-15"},  # Macedonia
        "eupol proxima": {"data_inizio": "2003-12-15", "data_fine": "2005-12-14"},  # Macedonia
        "eupat": {"data_inizio": "2005-12-15", "data_fine": "2006-06-14"},  # EU Police Advisory Team Macedonia
        "eupt": {"data_inizio": "2006-04-10", "data_fine": "2008-12-04"},  # EU Planning Team Kosovo
        "nato hq tirana": {"data_inizio": "2002-07-01", "data_fine": "2012-06-30"},
        "nato hq skopje": {"data_inizio": "2002-04-03", "data_fine": "2012-06-30"},
        "nato hq sarajevo": {"data_inizio": "2004-12-02"},  # still active
        "nato hq belgrado": {"data_inizio": "2006-12-14"},  # MLO Belgrade
        "tiph ii": {"data_inizio": "1997-01-21", "data_fine": "2019-01-31"},  # Temporary International Presence Hebron
        # ONU storiche
        "minugua": {"data_inizio": "1994-11-21", "data_fine": "2004-11-15"},  # Guatemala
        "unavem iii": {"data_inizio": "1995-02-08", "data_fine": "1997-06-30"},  # Angola
        "processo pace sudan": {"data_inizio": "2005-03-24", "data_fine": "2011-07-09"},  # UNMIS
        "unowa": {"data_inizio": "2002-03-01", "data_fine": "2016-03-31"},  # UN Office West Africa
        "amisom": {"data_inizio": "2007-03-06", "data_fine": "2022-03-31"},  # AU Mission Somalia
        # NATO storiche
        "active endeavour": {"data_inizio": "2001-10-26", "data_fine": "2016-11-09"},  # Mediterranean
        "ntm i": {"data_inizio": "2004-08-14", "data_fine": "2011-12-31"},  # NATO Training Mission Iraq
        "indus": {"data_inizio": "2005-10-08", "data_fine": "2006-02-01"},  # Pakistan earthquake
        "ocean shield": {"data_inizio": "2009-08-17", "data_fine": "2016-12-15"},  # Anti-piracy
        "baltic air policing": {"data_inizio": "2004-03-29", "data_fine": "2023-12-31"},  # NATO, rotazione conclusa
        # Multinazionali storiche
        "enduring freedom - nibbio": {"data_inizio": "2001-11-18", "data_fine": "2014-12-28"},  # Afghanistan
        "antica babilonia": {"data_inizio": "2003-07-15", "data_fine": "2006-12-01"},  # Iraq
        "interfet": {"data_inizio": "1999-09-20", "data_fine": "2000-02-28"},  # East Timor
        # UE storiche
        "althea": {"data_inizio": "2004-12-02"},  # EUFOR Althea, ongoing
        "eupol kinshasa": {"data_inizio": "2005-04-30", "data_fine": "2007-06-30"},
        "eufor rdc": {"data_inizio": "2006-07-12", "data_fine": "2006-11-30"},  # DRC elections
        "eusec rdc": {"data_inizio": "2005-06-08", "data_fine": "2016-06-30"},
        "eupol afghanistan": {"data_inizio": "2007-06-15", "data_fine": "2016-12-31"},
        "eupol rdc": {"data_inizio": "2007-07-01", "data_fine": "2014-09-30"},
        "eufor ciad": {"data_inizio": "2008-01-28", "data_fine": "2009-03-15"},  # Chad/RCA
        "eumm georgia": {"data_inizio": "2008-10-01", "data_fine": "2025-12-31"},  # civile UE, non in difesa.it ops militari
        "eu amis ii sudan": {"data_inizio": "2005-07-18", "data_fine": "2007-12-31"},
        "artemis": {"data_inizio": "2003-06-12", "data_fine": "2003-09-01"},  # DRC
        "mare sicuro": {"data_inizio": "2015-03-12"},  # ongoing national op
        # ── Missioni Guerra Fredda e anni '90 — fonti: difesa.it archivio storico ──
        "mandato onu in somalia": {"data_inizio": "1950-01-01", "data_fine": "1960-12-31"},  # AFIS trust territory
        "unef": {"data_inizio": "1956-11-15", "data_fine": "1967-06-17"},  # Suez crisis
        "unogil": {"data_inizio": "1958-06-11", "data_fine": "1958-12-09"},  # Lebanon observation
        "intervento in laos": {"data_inizio": "1961-05-01", "data_fine": "1962-12-31"},
        "unoc": {"data_inizio": "1960-07-14", "data_fine": "1964-06-30"},  # Congo
        "unyom": {"data_inizio": "1963-07-04", "data_fine": "1964-09-04"},  # Yemen
        "mictm": {"data_inizio": "1967-01-01", "data_fine": "1979-12-31"},  # Malta bilateral
        "diatm": {"data_inizio": "1973-01-01", "data_fine": "1979-12-31"},  # Malta bilateral
        "libano i": {"data_inizio": "1982-08-21", "data_fine": "1982-09-13"},  # Beirut MNF I
        "libano ii": {"data_inizio": "1982-09-29", "data_fine": "1984-02-26"},  # Beirut MNF II
        "mine mar rosso": {"data_inizio": "1984-08-09", "data_fine": "1984-10-15"},  # Red Sea minesweeping
        "miatm": {"data_inizio": "1980-01-01", "data_fine": "1988-12-31"},  # Malta bilateral
        "uniimog": {"data_inizio": "1988-08-20", "data_fine": "1991-02-28"},  # Iran-Iraq
        "untag": {"data_inizio": "1989-04-01", "data_fine": "1990-03-21"},  # Namibia
        "unoca": {"data_inizio": "1988-05-15", "data_fine": "1990-03-15"},  # Afghanistan
        "desert shield - golfo 2": {"data_inizio": "1990-08-07", "data_fine": "1991-01-16"},
        "desert storm - locusta": {"data_inizio": "1991-01-17", "data_fine": "1991-02-28"},
        "unosgi": {"data_inizio": "1991-04-09", "data_fine": "1991-10-31"},  # Iraq-Kuwait
        "provide comfort i": {"data_inizio": "1991-04-07", "data_fine": "1991-07-15"},  # Kurdistan
        "provide comfort ii": {"data_inizio": "1991-07-15", "data_fine": "1996-12-31"},
        "eumm balcani": {"data_inizio": "1991-07-07", "data_fine": "2007-12-31"},  # EC/EU Monitor Mission
        "onusal": {"data_inizio": "1991-07-26", "data_fine": "1995-04-30"},  # El Salvador
        "pellicano": {"data_inizio": "1991-09-14", "data_fine": "1993-12-03"},  # Albania humanitarian
        "sharp fence - maritime guard": {"data_inizio": "1992-07-16", "data_fine": "1993-06-15"},  # Adriatic
        "untac": {"data_inizio": "1992-03-15", "data_fine": "1993-09-26"},  # Cambodia
        "unitaf - restore hope - ibis i": {"data_inizio": "1992-12-09", "data_fine": "1993-05-04"},  # Somalia
        "unomoz": {"data_inizio": "1992-12-16", "data_fine": "1994-12-09"},  # Mozambique
        "unosom ii - ibis ii": {"data_inizio": "1993-05-04", "data_fine": "1995-03-28"},  # Somalia
        "operazione danubio": {"data_inizio": "1993-04-17", "data_fine": "1996-06-18"},  # Danube patrol
        "sharp guard": {"data_inizio": "1993-06-15", "data_fine": "1996-10-02"},  # Adriatic
        "deny flight": {"data_inizio": "1993-04-12", "data_fine": "1995-12-20"},  # Bosnia no-fly
        "ippocampo": {"data_inizio": "1994-07-01", "data_fine": "1994-12-31"},  # Rwanda humanitarian
        "tiph i": {"data_inizio": "1994-05-12", "data_fine": "1997-01-17"},  # Hebron
        "united shield": {"data_inizio": "1995-01-01", "data_fine": "1995-03-03"},  # Somalia withdrawal
    }

    # Record da rimuovere (duplicati o dati storici erroneamente attivi)
    # NOTA: usare il nome ORIGINALE (prima delle correzioni) in lowercase
    RECORDS_TO_REMOVE = [
        # Duplicati: tenere il record con dati migliori
        "kfor - joint enterprise",          # duplicato di KFOR
        "misin niger",                      # duplicato di MISIN
        "eu naval force somalia atalanta (eu- navfor somalia)",  # duplicato di EUNAVFOR Atalanta
        "eu naval operation mediterranean irini",               # duplicato di IRINI/EUNAVFOR MED Irini
        "eu regional maritime capacity building for the horn of africa and the western indian ocean (eucap nestor) eucap somalia",  # duplicato di EUCAP Somalia
        "eunavfor med - irini",             # duplicato di IRINI (entrambi diventano EUNAVFOR MED Irini)
        "atalanta",                         # duplicato di EUNAVFOR ATALANTA
        "miadit",                           # duplicato di MIADIT Palestine (entrambi diventano MIADIT Palestina)
        "miadit palestine",                 # duplicato di MIADIT (69) che ha dati migliori
        "base gibuti",                      # duplicato: viene rinominato BMIS Gibuti ma c'è anche l'iniettato
        "althea",                              # duplicato di EUFOR ALTHEA
        "nato hq belgrado",                    # duplicato di NATO HQ Sarajevo (rinominato nel 2010)
        "combined task force 153",             # duplicato di CTF153 Mar Rosso (iniettato)
    ]

    def _apply_official_corrections(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applica correzioni puntuali ai dati basate sulle fonti ufficiali."""
        nomi = df["nome"].str.lower().str.strip()
        corrections_applied = 0

        # 1. Rimuovi record duplicati/obsoleti
        mask_remove = nomi.isin(self.RECORDS_TO_REMOVE)
        if mask_remove.any():
            removed = mask_remove.sum()
            df = df[~mask_remove].reset_index(drop=True)
            nomi = df["nome"].str.lower().str.strip()
            logger.info(f"Rimossi {removed} record duplicati/obsoleti")

        # 2. Correggi record "NATO Standing Naval Forces" storico (data_fine=1991)
        #    Questo è un record storico diverso dalla missione attuale (NATO SNF Med)
        mask_snf_old = (nomi == "nato standing naval forces") & (
            df["data_fine"].notna() & (df["data_fine"] < pd.Timestamp("2000-01-01"))
        )
        if mask_snf_old.any():
            df.loc[mask_snf_old, "is_active"] = False
            df.loc[mask_snf_old, "nome"] = "NATO Standing Naval Forces (storico)"
            df.loc[mask_snf_old, "personale_totale"] = 0
            df.loc[mask_snf_old, "personale_militare"] = 0
            logger.info("NATO Standing Naval Forces storico (1991) marcato inattivo e rinominato")

        # 3. Applica correzioni puntuali
        for nome_key, corrections in self.OFFICIAL_CORRECTIONS.items():
            mask = nomi == nome_key
            if not mask.any() and len(nome_key) >= 6:
                # Prova match parziale solo per keyword sufficientemente lunghe
                mask = nomi.str.contains(nome_key, na=False, regex=False)
            if mask.any():
                for field, value in corrections.items():
                    df.loc[mask, field] = value
                corrections_applied += mask.sum()

        # 3a-bis. Correzioni date forzate (sovrascrivono anche valori esistenti)
        # Fonte: Wikipedia, Delibera CdM — date errate nei dati originali
        FORCED_DATE_FIXES = {
            "miadit palestina": {"data_inizio": "2010-01-01"},  # Wikipedia: MIADIT dal 2010, non 2015
        }
        nomi_post = df["nome"].str.lower().str.strip()
        for nome_key, date_fix in FORCED_DATE_FIXES.items():
            mask = nomi_post == nome_key
            if not mask.any() and len(nome_key) >= 6:
                mask = nomi_post.str.contains(nome_key, na=False, regex=False)
            if mask.any():
                for field, value in date_fix.items():
                    df.loc[mask, field] = pd.to_datetime(value)
                    corrections_applied += mask.sum()

        if corrections_applied:
            logger.info(f"Applicate correzioni ufficiali a {corrections_applied} record")

        # 3b. Applica correzioni date (solo se data_inizio è NaT)
        nomi = df["nome"].str.lower().str.strip()  # refresh after corrections
        dates_applied = 0
        for nome_key, date_corr in self.DATE_CORRECTIONS.items():
            mask = nomi == nome_key
            if not mask.any() and len(nome_key) >= 6:
                mask = nomi.str.contains(nome_key, na=False, regex=False)
            if mask.any():
                for field, value in date_corr.items():
                    if field in ("data_inizio", "data_fine"):
                        # Solo se il campo è NaT (non sovrascrivere date esistenti)
                        nat_mask = mask & df[field].isna()
                        if nat_mask.any():
                            df.loc[nat_mask, field] = pd.to_datetime(value)
                            dates_applied += nat_mask.sum()
                    else:
                        # paese e altri campi: sovrascrivi solo se vuoto/nan
                        empty_mask = mask & (df[field].isna() | (df[field].astype(str).str.strip() == "") | (df[field].astype(str).str.lower() == "nan"))
                        if empty_mask.any():
                            df.loc[empty_mask, field] = value
                            dates_applied += empty_mask.sum()
        if dates_applied:
            logger.info(f"Applicate {dates_applied} correzioni date da fonti ufficiali")

        # 4. Normalizza TUTTI i nomi paese EN→IT
        COUNTRY_NORMALIZE = {
            "Bosnia and Herzegovina": "Bosnia ed Erzegovina",
            "Bosnia and Herzegovina, Croatia, Serbia, Montenegro, Albania, Macedonia": "Balcani",
            "Egypt": "Egitto",
            "Lebanon": "Libano",
            "Libya": "Libia",
            "Mozambique": "Mozambico",
            "Palestine": "Palestina",
            "Turkey": "Turchia",
            "Ukraine": "Ucraina",
            "Hungary": "Ungheria",
            "Indian Ocean": "Oceano Indiano",
            "Mediterranean Sea": "Mediterraneo",
            "Iraq-Kuwait": "Iraq",
            "Iraq, Iran": "Iraq",
            "DR Congo": "Repubblica Democratica del Congo",
            "Central African Republic": "Repubblica Centrafricana",
            "East Timor": "Timor Est",
            "Syria": "Siria",
            "Morocco": "Marocco",
            "Adriatic Sea": "Mare Adriatico",
            "Iceland": "Islanda",
            "Cambodia": "Cambogia",
            "Rwanda": "Ruanda",
            "Chad": "Ciad",
            "Darfur": "Sudan (Darfur)",
            "Abyei": "Sudan (Abyei)",
            "Sudan del Sud": "Sud Sudan",
            "Lithuania": "Lituania",
            "Estonia, Latvia, Lithuania": "Paesi Baltici",
            "Moldova/Ucraina": "Moldova",
            "Etiopia-Eritrea": "Etiopia/Eritrea",
            "Yugoslavia": "Ex-Jugoslavia",
            "Macedonia": "Macedonia del Nord",
            "Varie": "Non specificato",
        }
        before_countries = df["paese"].nunique()
        df["paese"] = df["paese"].replace(COUNTRY_NORMALIZE)
        after_countries = df["paese"].nunique()
        if before_countries != after_countries:
            logger.info(f"Normalizzati paesi: {before_countries} → {after_countries} unici")

        # 5. Riclassifica missioni "Altro" → organizzazione corretta
        ORG_RECLASSIFY = {
            "Aceh Mission- AMM": "UE",
            "EU Advisory Mission in Iraq": "UE",
            "EU Military Mission ARTEMIS, Democratic Republic of Congo (DRC)": "UE",
            "EU Military Mission CONCORDIA/ FYROM, Former Yugoslav Republic of Macedonia": "UE",
            "EU Naval Operation Mediterranean SOPHIA": "UE",
            "EU Police Mission Former Republic of Yugoslavia PROXIMA  (Proxima/ FYROM) 1 AND 2": "UE",
            "EU Security Sector Reform Mission in Guinea-Bissau (EU-SSR)": "UE",
            "EU Support to AMIS (Darfur)": "UE",
        }
        for nome_missione, org in ORG_RECLASSIFY.items():
            mask = df["nome"] == nome_missione
            if mask.any():
                df.loc[mask, "tipo_missione"] = org
        reclassified = sum(1 for n in ORG_RECLASSIFY if (df["nome"] == n).any())
        if reclassified:
            logger.info(f"Riclassificate {reclassified} missioni da 'Altro' a organizzazione corretta")

        return df

    # =========================================================================
    # LISTA UFFICIALE MISSIONI ATTIVE (Ministero Difesa 2026)
    # =========================================================================

    # Keyword di matching per le 38 operazioni internazionali in corso
    # Fonte: https://www.difesa.it/operazionimilitari/op-intern-corso/operazioni-int/26752.html
    OFFICIAL_ACTIVE_2026 = [
        "nato hq sarajevo",
        "sea guardian",
        "nato mission iraq",
        "kfor",
        "joint enterprise",
        "nato mlo belgrade",
        "baltic guardian",
        "nato standing naval",
        "battle group bulgaria",
        "battle group ungheria",
        "baltic eagle",
        "baltic eagle iii",
        "minurso",
        "unifil",
        "unficyp",
        "unmogip",
        "eunavfor atalanta",
        "atalanta",
        "emasoh",
        "eutm somalia",
        "eucap somalia",
        "eunavfor med irini",
        "eunavfor med - irini",
        "irini",
        "eufor althea",
        "eulex",
        "eunavfor aspides",
        "aspides",
        "eumam mozambico",
        "eutm mozambico",
        "eubam rafah",
        "eupol copps",
        "mtc4l",
        "levante",
        "ctf153",
        "ctf 153",
        "prima parthica",
        "mibil",
        "miccd",
        "miasit",
        "misin",
        "bmis",
        "base gibuti",
        "mediterraneo sicuro",
        "mare sicuro",
        "miadit somalia",
        "miadit palestina",
        "miadit",
        "mfo",
        "euma armenia",
        "untso",
    ]

    # Keyword che richiedono match come parola intera (evita 'mfo' in 'comfort')
    _WHOLE_WORD_KEYWORDS = {"mfo"}

    def _match_official_active(self, df: pd.DataFrame) -> pd.Series:
        """Restituisce una maschera booleana: True se il nome missione matcha la lista ufficiale."""
        import re as _re
        nomi = df["nome"].str.lower().str.strip()
        mask = pd.Series(False, index=df.index)
        for keyword in self.OFFICIAL_ACTIVE_2026:
            if keyword in self._WHOLE_WORD_KEYWORDS:
                pattern = r"\b" + _re.escape(keyword) + r"\b"
                mask = mask | nomi.str.contains(pattern, na=False, regex=True)
            else:
                mask = mask | nomi.str.contains(keyword, na=False, regex=False)
        return mask

    # Missioni ufficiali Difesa 2026 non presenti nelle fonti Excel
    MISSING_OFFICIAL = [
        {"nome": "NATO Military Liaison Office Belgrade", "paese": "Serbia",
         "regione": "Europa", "sub_regione": "Balcani", "tipo_partecipazione": "mil",
         "data_inizio": "2006-01-01", "personale_militare": 5, "personale_totale": 5,
         "tipo_missione": "NATO", "commitment": "Head of Mission",
         "link_documento": "https://www.difesa.it/operazionimilitari/op-intern-corso/serbia-nato-mlo/default/27735.html"},
        {"nome": "NATO Multinational Battle Group Bulgaria", "paese": "Bulgaria",
         "regione": "Europa", "sub_regione": "Europa Orientale", "tipo_partecipazione": "mil",
         "data_inizio": "2022-06-01", "personale_militare": 250, "personale_totale": 250,
         "tipo_missione": "NATO", "commitment": "Troops",
         "link_documento": "https://www.difesa.it/operazionimilitari/op-intern-corso/operazione-eva-bulgaria/default/28035.html"},
        {"nome": "NATO Multinational Battle Group Ungheria", "paese": "Ungheria",
         "regione": "Europa", "sub_regione": "Europa Orientale", "tipo_partecipazione": "mil",
         "data_inizio": "2023-01-01", "personale_militare": 200, "personale_totale": 200,
         "tipo_missione": "NATO", "commitment": "Troops",
         "link_documento": "https://www.difesa.it/operazionimilitari/op-intern-corso/operazione-eva-ungheria/default/28061.html"},
        {"nome": "MTC4L Libano", "paese": "Libano",
         "regione": "Medio Oriente", "sub_regione": "Medio Oriente", "tipo_partecipazione": "mil",
         "data_inizio": "2024-01-01", "personale_militare": 100, "personale_totale": 100,
         "tipo_missione": "Multinational", "commitment": "Troops",
         "link_documento": "https://www.difesa.it/operazionimilitari/op-intern-corso/libano-mtc4l/default/63929.html"},
        {"nome": "CTF153 Mar Rosso", "paese": "Mar Rosso",
         "regione": "Medio Oriente", "sub_regione": "Medio Oriente", "tipo_partecipazione": "mil",
         "data_inizio": "2022-04-01", "personale_militare": 150, "personale_totale": 150,
         "tipo_missione": "Coalizione", "commitment": "Troops",
         "link_documento": "https://www.difesa.it/operazionimilitari/op-intern-corso/ctf153/index/53056.html"},
    ]

    def _inject_missing_official(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggiunge missioni ufficiali Difesa 2026 non presenti nel dataset."""
        # Ricalcola nomi dopo le correzioni
        nomi_lower = df["nome"].str.lower().str.strip().tolist()
        added = 0
        for mission in self.MISSING_OFFICIAL:
            # Verifica se già presente (match esatto o parziale)
            check = mission["nome"].lower()
            already = any(
                check == n or check in n or n in check
                for n in nomi_lower
            )
            if already:
                continue
            row = {col: "" for col in df.columns}
            row.update(mission)
            row["is_active"] = True
            row["fonte_dati"] = "difesa.it_2026"
            row["dati_stimati"] = True
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            added += 1
            logger.info(f"Aggiunta missione mancante: {mission['nome']}")
        if added:
            logger.info(f"Iniettate {added} missioni dalla lista ufficiale Difesa 2026")
        return df

    # =========================================================================
    # STEP 5: VALIDAZIONE
    # =========================================================================

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida i record usando il modello Pydantic Mission."""
        if df.empty:
            return df

        valid_rows = []
        errors = 0

        for idx, row in df.iterrows():
            try:
                mission_data = {
                    "nome": self._safe_str(row.get("nome"), ""),
                    "paese": self._safe_str(row.get("paese"), "Non specificato"),
                    "regione": self._safe_str(row.get("regione"), "Non specificata"),
                    "sub_regione": self._safe_str(row.get("sub_regione"), "Non specificata"),
                    "tipo_partecipazione": self._safe_str(row.get("tipo_partecipazione"), "civmil"),
                    "data_inizio": row.get("data_inizio"),
                    "data_fine": row.get("data_fine"),
                    "personale_militare": self._safe_float(row.get("personale_militare")),
                    "personale_civile": self._safe_float(row.get("personale_civile")),
                    "personale_totale": self._safe_float(row.get("personale_totale")),
                    "costo_totale": self._safe_float(row.get("costo_totale")),
                    "tipo_missione": self._safe_str(row.get("tipo_missione"), "Altro"),
                    "commitment": self._safe_str(row.get("commitment"), "Troops"),
                    "is_active": bool(row.get("is_active", False)),
                    "fonte_dati": self._safe_str(row.get("fonte_dati"), ""),
                    "dati_stimati": bool(row.get("dati_stimati", False)),
                }
                Mission(**mission_data)
                valid_rows.append(idx)
            except Exception as e:
                errors += 1
                if errors <= 10:
                    logger.warning(f"Validazione fallita per '{row.get('nome', '?')}': {e}")

        self.result.validation_errors = errors
        if errors > 0:
            logger.warning(f"Validazione: {errors} errori su {len(df)} record")

        return df.loc[valid_rows]

    @staticmethod
    def _safe_str(value, default: str = "") -> str:
        """Converte un valore in stringa, gestendo NaN/None."""
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        s = str(value).strip()
        return s if s and s.lower() not in ("nan", "none", "nat") else default

    @staticmethod
    def _safe_float(value, default: float = 0.0) -> float:
        """Converte un valore in float, gestendo NaN/None."""
        if value is None:
            return default
        try:
            f = float(value)
            return default if np.isnan(f) else f
        except (ValueError, TypeError):
            return default

    # =========================================================================
    # STEP 6: SALVATAGGIO
    # =========================================================================

    # Colonne canoniche da includere nel dataset finale
    CANONICAL_COLUMNS = [
        "nome", "paese", "regione", "sub_regione", "tipo_partecipazione",
        "data_inizio", "data_fine", "personale_militare", "personale_civile",
        "personale_totale", "costo_totale", "tipo_missione", "commitment",
        "is_active", "fonte_dati", "dati_stimati", "ultimo_aggiornamento",
        "mandato", "note", "link_documento",
    ]

    def save(self, df: pd.DataFrame, output_path: str = "data/processed/missioni_complete.csv") -> str:
        """Salva il dataset finale, mantenendo solo le colonne canoniche."""
        if df.empty:
            logger.warning("Nessun dato da salvare")
            return ""

        df_out = df.copy()

        # Mantieni solo le colonne canoniche presenti nel DataFrame
        keep_cols = [c for c in self.CANONICAL_COLUMNS if c in df_out.columns]
        df_out = df_out[keep_cols]

        # Rimuovi colonne duplicate
        df_out = df_out.loc[:, ~df_out.columns.duplicated()]

        # Salva
        full_path = self.base_dir / output_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(full_path, index=False, encoding="utf-8")

        self.result.total_missions = len(df_out)
        logger.info(f"Dataset salvato: {full_path} ({len(df_out)} missioni, {len(keep_cols)} colonne)")
        return str(full_path)

    # =========================================================================
    # PIPELINE COMPLETA
    # =========================================================================

    def run(self, output_path: str = "data/processed/missioni_complete.csv") -> pd.DataFrame:
        """
        Esegue la pipeline completa:
        load_sources → normalize → deduplicate → enrich → validate → save
        """
        logger.info("=" * 60)
        logger.info("MIDA Pipeline - Avvio aggregazione dati")
        logger.info("=" * 60)

        # Step 1
        logger.info("Step 1/6: Caricamento fonti...")
        df = self.load_sources()
        if df.empty:
            logger.error("Nessun dato caricato. Pipeline terminata.")
            return df

        # Step 2
        logger.info("Step 2/6: Normalizzazione...")
        df = self.normalize(df)

        # Step 3
        logger.info("Step 3/6: Deduplicazione...")
        df = self.deduplicate(df)

        # Step 4
        logger.info("Step 4/6: Arricchimento...")
        df = self.enrich(df)

        # Step 5
        logger.info("Step 5/6: Validazione...")
        df = self.validate(df)

        # Step 6
        logger.info("Step 6/6: Salvataggio...")
        self.save(df, output_path)

        # Report finale
        logger.info("=" * 60)
        logger.info(f"Pipeline completata:")
        logger.info(f"  Fonti caricate: {self.result.sources_loaded}")
        logger.info(f"  Missioni totali: {self.result.total_missions}")
        logger.info(f"  Duplicati rimossi: {self.result.duplicates_removed}")
        logger.info(f"  Errori validazione: {self.result.validation_errors}")
        logger.info("=" * 60)

        return df

    def get_result(self) -> PipelineResult:
        """Restituisce il risultato dell'ultima esecuzione."""
        return self.result


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/aggregation.log", encoding="utf-8"),
        ],
    )

    aggregator = ExcelAggregator()
    df = aggregator.run()

    result = aggregator.get_result()
    print(f"\nRisultato: {result.total_missions} missioni, {result.duplicates_removed} duplicati rimossi")
