"""
Test unitari per core/aggregator.py
Copre: caricamento fonti, normalizzazione, deduplicazione, arricchimento, pipeline.
"""

import pytest
import sys
import tempfile
import os
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.aggregator import ExcelAggregator


@pytest.fixture
def tmp_project(tmp_path):
    """Crea una struttura di progetto temporanea con dati di test."""
    # Crea directory
    (tmp_path / "data" / "processed").mkdir(parents=True)
    (tmp_path / "data" / "raw" / "Excel").mkdir(parents=True)
    (tmp_path / "config").mkdir(parents=True)
    (tmp_path / "logs").mkdir(parents=True)

    # Crea CSV principale
    df_main = pd.DataFrame([
        {"nome": "UNIFIL", "paese": "Libano", "regione": "Medio Oriente",
         "sub_regione": "Levant", "tipo_partecipazione": "mil",
         "data_inizio": "1978-03-19", "data_fine": "2027-12-31",
         "personale_militare": 1100, "personale_civile": 50,
         "personale_totale": 1150, "costo_totale": 150000000,
         "tipo_missione": "ONU", "commitment": "Head of Mission", "is_active": True},
        {"nome": "KFOR", "paese": "Kosovo", "regione": "Europa",
         "sub_regione": "Balcani", "tipo_partecipazione": "mil",
         "data_inizio": "1999-06-12", "data_fine": "2027-12-31",
         "personale_militare": 600, "personale_civile": 30,
         "personale_totale": 630, "costo_totale": 80000000,
         "tipo_missione": "NATO", "commitment": "Troops (ground forces)", "is_active": True},
        {"nome": "EUTM Mali", "paese": "Mali", "regione": "Africa",
         "sub_regione": "Africa Occidentale", "tipo_partecipazione": "mil",
         "data_inizio": "2013-02-18", "data_fine": "2024-05-18",
         "personale_militare": 200, "personale_civile": 20,
         "personale_totale": 220, "costo_totale": 30000000,
         "tipo_missione": "UE", "commitment": "Advisory/Training", "is_active": False},
    ])
    df_main.to_csv(tmp_path / "data" / "processed" / "missioni_complete.csv", index=False)

    # Crea Excel con missioni (alcune duplicate, alcune nuove)
    df_excel = pd.DataFrame([
        {"mission": "UNIFIL", "country": "Lebanon", "region": "Middle East",
         "framework": "UN", "date_start": "1978-03-19", "date_end": "2027-12-31"},
        {"mission": "UNMISS", "country": "South Sudan", "region": "Africa",
         "framework": "UN", "date_start": "2011-07-09", "date_end": "2027-12-31"},
        {"mission": "EUNAVFOR Atalanta", "country": "Somalia", "region": "Africa",
         "framework": "EU", "date_start": "2008-12-08", "date_end": "2024-12-31"},
    ])
    df_excel.to_excel(tmp_path / "data" / "raw" / "Excel" / "missions.xlsx", index=False)

    # Crea sources.yaml
    sources_yaml = """
sources:
  - name: missioni_complete
    path: data/processed/missioni_complete.csv
    type: csv
    priority: 1
    default_org: Altro
    column_mapping:
      nome: nome
      paese: paese
      regione: regione
      tipo_missione: tipo_missione

  - name: missions_excel
    path: data/raw/Excel/missions.xlsx
    type: excel
    priority: 2
    default_org: ONU
    column_mapping:
      mission: nome
      country: paese
      region: regione
      framework: tipo_missione
      date_start: data_inizio
      date_end: data_fine
"""
    (tmp_path / "config" / "sources.yaml").write_text(sources_yaml, encoding="utf-8")

    return tmp_path


class TestExcelAggregatorLoadSources:
    def test_loads_csv_and_excel(self, tmp_project):
        agg = ExcelAggregator(base_dir=str(tmp_project))
        df = agg.load_sources()
        assert not df.empty
        assert agg.result.sources_loaded == 2

    def test_missing_file_skipped(self, tmp_project):
        # Rimuovi il file Excel
        os.remove(tmp_project / "data" / "raw" / "Excel" / "missions.xlsx")
        agg = ExcelAggregator(base_dir=str(tmp_project))
        df = agg.load_sources()
        assert agg.result.sources_loaded == 1

    def test_fonte_dati_column_added(self, tmp_project):
        agg = ExcelAggregator(base_dir=str(tmp_project))
        df = agg.load_sources()
        assert "fonte_dati" in df.columns
        assert set(df["fonte_dati"].unique()) == {"missioni_complete", "missions_excel"}


class TestExcelAggregatorNormalize:
    def test_normalizes_organizations(self, tmp_project):
        agg = ExcelAggregator(base_dir=str(tmp_project))
        df = agg.load_sources()
        df = agg.normalize(df)
        # UNIFIL dovrebbe essere ONU
        unifil_rows = df[df["nome"].str.contains("UNIFIL", case=False, na=False)]
        assert all(unifil_rows["tipo_missione"] == "ONU")

    def test_normalizes_regions(self, tmp_project):
        agg = ExcelAggregator(base_dir=str(tmp_project))
        df = agg.load_sources()
        df = agg.normalize(df)
        # "Middle East" dovrebbe diventare "Medio Oriente"
        assert "Middle East" not in df["regione"].values

    def test_ensures_all_columns(self, tmp_project):
        agg = ExcelAggregator(base_dir=str(tmp_project))
        df = agg.load_sources()
        df = agg.normalize(df)
        required = ["nome", "paese", "regione", "tipo_missione", "commitment", "is_active"]
        for col in required:
            assert col in df.columns


class TestExcelAggregatorDeduplicate:
    def test_removes_duplicates(self, tmp_project):
        agg = ExcelAggregator(base_dir=str(tmp_project))
        df = agg.load_sources()
        df = agg.normalize(df)
        count_before = len(df)
        df = agg.deduplicate(df)
        # UNIFIL appare in entrambe le fonti, dovrebbe essere deduplicato
        assert len(df) < count_before
        assert agg.result.duplicates_removed > 0

    def test_priority_wins(self, tmp_project):
        agg = ExcelAggregator(base_dir=str(tmp_project))
        df = agg.load_sources()
        df = agg.normalize(df)
        df = agg.deduplicate(df)
        # UNIFIL dal CSV (priorità 1) dovrebbe vincere
        unifil = df[df["nome"].str.contains("UNIFIL", case=False, na=False)]
        assert len(unifil) == 1
        assert unifil.iloc[0]["fonte_dati"] == "missioni_complete"

    def test_new_missions_kept(self, tmp_project):
        agg = ExcelAggregator(base_dir=str(tmp_project))
        df = agg.load_sources()
        df = agg.normalize(df)
        df = agg.deduplicate(df)
        # UNMISS è solo nell'Excel, deve rimanere
        unmiss = df[df["nome"].str.contains("UNMISS", case=False, na=False)]
        assert len(unmiss) == 1


class TestExcelAggregatorEnrich:
    def test_computes_is_active(self, tmp_project):
        agg = ExcelAggregator(base_dir=str(tmp_project))
        df = agg.load_sources()
        df = agg.normalize(df)
        df = agg.deduplicate(df)
        df = agg.enrich(df)
        # UNIFIL con data_fine 2027-12-31 dovrebbe essere attiva
        unifil = df[df["nome"].str.contains("UNIFIL", case=False, na=False)]
        assert unifil.iloc[0]["is_active"] is True or unifil.iloc[0]["is_active"] == True


class TestExcelAggregatorPipeline:
    def test_full_pipeline(self, tmp_project):
        agg = ExcelAggregator(base_dir=str(tmp_project))
        df = agg.run(output_path="data/processed/missioni_complete.csv")
        assert not df.empty
        result = agg.get_result()
        assert result.total_missions > 0
        assert result.sources_loaded == 2
        # Verifica che il file sia stato salvato
        assert (tmp_project / "data" / "processed" / "missioni_complete.csv").exists()

    def test_no_internal_columns_in_output(self, tmp_project):
        agg = ExcelAggregator(base_dir=str(tmp_project))
        agg.run(output_path="data/processed/missioni_complete.csv")
        df_saved = pd.read_csv(tmp_project / "data" / "processed" / "missioni_complete.csv")
        internal_cols = [c for c in df_saved.columns if c.startswith("_")]
        assert len(internal_cols) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
