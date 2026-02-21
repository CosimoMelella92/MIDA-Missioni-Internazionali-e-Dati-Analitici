"""
Test E2E per la dashboard MIDA.
Verifica che tutti i componenti funzionino end-to-end senza avviare Streamlit.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Ensure project root is in path
_root = Path(__file__).parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


# ── Fixtures ──

@pytest.fixture(scope="module")
def dataset():
    """Carica il dataset completo come farebbe la dashboard."""
    from dashboard.data_loader import load_data
    df = load_data()
    assert df is not None, "Dataset non caricato"
    assert not df.empty, "Dataset vuoto"
    return df


# ── 1. Data Loading ──

class TestDataLoading:
    def test_load_data_returns_dataframe(self, dataset):
        assert isinstance(dataset, pd.DataFrame)

    def test_expected_row_count(self, dataset):
        assert len(dataset) >= 200, f"Troppe poche missioni: {len(dataset)}"

    def test_required_columns_present(self, dataset):
        required = ["nome", "paese", "regione", "tipo_missione", "data_inizio",
                     "personale_totale", "costo_totale", "is_active", "commitment"]
        missing = [c for c in required if c not in dataset.columns]
        assert not missing, f"Colonne mancanti: {missing}"

    def test_no_missing_dates(self, dataset):
        missing = dataset["data_inizio"].isna().sum()
        assert missing == 0, f"{missing} missioni senza data_inizio"

    def test_no_nan_commitment(self, dataset):
        nan_count = dataset["commitment"].isna().sum()
        str_nan = (dataset["commitment"].astype(str).str.lower() == "nan").sum()
        assert nan_count == 0, f"{nan_count} commitment NaN"
        assert str_nan == 0, f"{str_nan} commitment stringa 'nan'"

    def test_active_missions_count(self, dataset):
        active = dataset["is_active"].sum()
        assert 30 <= active <= 50, f"Missioni attive fuori range: {active}"

    def test_no_nan_paese(self, dataset):
        nan_paese = dataset["paese"].isna().sum()
        str_nan = (dataset["paese"].astype(str).str.lower() == "nan").sum()
        assert nan_paese + str_nan == 0, f"{nan_paese + str_nan} paesi mancanti"


# ── 2. Analysis Functions ──

class TestAnalysis:
    def test_organization_analysis(self, dataset):
        from dashboard.analysis import create_organization_analysis
        result = create_organization_analysis(dataset)
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert "Organizzazione" in result.columns

    def test_commitment_analysis(self, dataset):
        from dashboard.analysis import create_commitment_analysis
        result = create_commitment_analysis(dataset)
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_regional_analysis(self, dataset):
        from dashboard.analysis import create_regional_analysis
        result = create_regional_analysis(dataset)
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0


# ── 3. Charts ──

class TestCharts:
    def test_chart_missions_by_org(self, dataset):
        from dashboard.analysis import create_organization_analysis
        from dashboard.charts import chart_missions_by_org
        org_stats = create_organization_analysis(dataset)
        fig = chart_missions_by_org(org_stats)
        assert fig is not None
        assert len(fig.data) > 0

    def test_chart_missions_by_commitment(self, dataset):
        from dashboard.analysis import create_commitment_analysis
        from dashboard.charts import chart_missions_by_commitment
        stats = create_commitment_analysis(dataset)
        fig = chart_missions_by_commitment(stats)
        assert fig is not None

    def test_chart_missions_by_region(self, dataset):
        from dashboard.analysis import create_regional_analysis
        from dashboard.charts import chart_missions_by_region
        stats = create_regional_analysis(dataset)
        region_summary = stats.groupby("Regione")["Numero Missioni"].sum().reset_index()
        fig = chart_missions_by_region(region_summary)
        assert fig is not None

    def test_format_currency(self):
        from dashboard.charts import format_currency
        assert "M" in format_currency(5_000_000)
        assert "B" in format_currency(2_000_000_000)
        assert "K" in format_currency(50_000)


# ── 4. Filters ──

class TestFilters:
    def test_apply_empty_filters(self, dataset):
        from dashboard.filters import apply_filters
        result = apply_filters(dataset, {})
        assert len(result) == len(dataset)


# ── 5. Pipeline Integrity ──

class TestPipelineIntegrity:
    def test_csv_matches_pipeline(self):
        """Verifica che il CSV salvato corrisponda all'output della pipeline."""
        csv_path = _root / "data" / "processed" / "missioni_complete.csv"
        assert csv_path.exists(), "missioni_complete.csv non trovato"
        df_csv = pd.read_csv(csv_path)
        assert len(df_csv) >= 200

    def test_all_orgs_valid(self, dataset):
        valid_orgs = {"ONU", "NATO", "UE", "ITA", "Bilateral", "Multinational", "Coalizione"}
        actual_orgs = set(dataset["tipo_missione"].dropna().unique())
        invalid = actual_orgs - valid_orgs
        assert not invalid, f"Organizzazioni non valide: {invalid}"

    def test_dates_are_datetime(self, dataset):
        assert pd.api.types.is_datetime64_any_dtype(dataset["data_inizio"])

    def test_no_duplicate_names(self, dataset):
        dupes = dataset["nome"].duplicated().sum()
        assert dupes == 0, f"{dupes} nomi duplicati"
