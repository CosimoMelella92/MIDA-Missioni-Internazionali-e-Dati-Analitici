"""
Test unitari per core/models.py
Copre: validazione Mission, parsing date, coerenza date, calcolo personale.
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.models import Mission, PipelineResult, SourceConfig

# =============================================================================
# TEST MISSION MODEL
# =============================================================================

class TestMission:
    def test_minimal_valid(self):
        m = Mission(nome="UNIFIL", paese="Libano")
        assert m.nome == "UNIFIL"
        assert m.paese == "Libano"
        assert m.is_active is False

    def test_strips_whitespace(self):
        m = Mission(nome="  KFOR  ", paese="  Kosovo  ")
        assert m.nome == "KFOR"
        assert m.paese == "Kosovo"

    def test_date_parsing_iso(self):
        m = Mission(nome="Test", paese="Test", data_inizio="2020-01-15")
        assert m.data_inizio == date(2020, 1, 15)

    def test_date_parsing_european(self):
        m = Mission(nome="Test", paese="Test", data_inizio="15/01/2020")
        assert m.data_inizio == date(2020, 1, 15)

    def test_date_parsing_year_only(self):
        m = Mission(nome="Test", paese="Test", data_inizio="2020")
        assert m.data_inizio == date(2020, 1, 1)

    def test_date_none_for_invalid(self):
        m = Mission(nome="Test", paese="Test", data_inizio="invalid")
        assert m.data_inizio is None

    def test_date_none_for_nan(self):
        m = Mission(nome="Test", paese="Test", data_inizio="nan")
        assert m.data_inizio is None

    def test_date_none_for_nat(self):
        m = Mission(nome="Test", paese="Test", data_inizio="NaT")
        assert m.data_inizio is None

    def test_date_coherence_swap(self):
        m = Mission(
            nome="Test", paese="Test",
            data_inizio="2025-01-01", data_fine="2020-01-01",
        )
        # Le date invertite vengono scambiate
        assert m.data_inizio == date(2020, 1, 1)
        assert m.data_fine == date(2025, 1, 1)

    def test_compute_totale(self):
        m = Mission(
            nome="Test", paese="Test",
            personale_militare=100, personale_civile=50,
        )
        assert m.personale_totale == 150

    def test_totale_not_overwritten_if_set(self):
        m = Mission(
            nome="Test", paese="Test",
            personale_militare=100, personale_civile=50,
            personale_totale=200,
        )
        assert m.personale_totale == 200

    def test_defaults(self):
        m = Mission(nome="Test", paese="Test")
        assert m.regione == "Non specificata"
        assert m.sub_regione == "Non specificata"
        assert m.tipo_partecipazione == "civmil"
        assert m.costo_totale == 0.0
        assert m.tipo_missione == "Altro"
        assert m.commitment == "Troops"
        assert m.dati_stimati is False

    def test_negative_personnel_rejected(self):
        with pytest.raises(Exception):
            Mission(nome="Test", paese="Test", personale_totale=-10)

    def test_negative_cost_rejected(self):
        with pytest.raises(Exception):
            Mission(nome="Test", paese="Test", costo_totale=-100)

    def test_empty_nome_rejected(self):
        with pytest.raises(Exception):
            Mission(nome="", paese="Test")


# =============================================================================
# TEST SOURCE CONFIG
# =============================================================================

class TestSourceConfig:
    def test_minimal(self):
        s = SourceConfig(name="test", path="data/test.xlsx")
        assert s.type == "excel"
        assert s.priority == 1
        assert s.skip_rows == 0

    def test_csv_type(self):
        s = SourceConfig(name="test", path="data/test.csv", type="csv")
        assert s.type == "csv"

    def test_column_mapping(self):
        s = SourceConfig(
            name="test", path="data/test.xlsx",
            column_mapping={"mission": "nome", "country": "paese"},
        )
        assert s.column_mapping["mission"] == "nome"


# =============================================================================
# TEST PIPELINE RESULT
# =============================================================================

class TestPipelineResult:
    def test_defaults(self):
        r = PipelineResult()
        assert r.total_missions == 0
        assert r.duplicates_removed == 0
        assert r.warnings == []

    def test_update(self):
        r = PipelineResult(total_missions=100, duplicates_removed=15)
        assert r.total_missions == 100
        assert r.duplicates_removed == 15


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
