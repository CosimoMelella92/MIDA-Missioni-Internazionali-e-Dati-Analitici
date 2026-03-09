"""
Test unitari per core/normalizer.py
Copre: normalizzazione nomi, organizzazioni, regioni, commitment, colonne.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.normalizer import (
    classify_period,
    normalize_column_name,
    normalize_columns,
    normalize_commitment,
    normalize_mission_name,
    normalize_mission_name_strict,
    normalize_organization,
    normalize_region,
)

# =============================================================================
# TEST NORMALIZZAZIONE NOMI MISSIONE
# =============================================================================

class TestNormalizeMissionName:
    def test_basic(self):
        assert normalize_mission_name("UNIFIL") == "unifil"

    def test_strips_whitespace(self):
        assert normalize_mission_name("  KFOR  ") == "kfor"

    def test_removes_special_chars(self):
        assert normalize_mission_name("EUNAVFOR (Atalanta)") == "eunavfor atalanta"

    def test_collapses_spaces(self):
        assert normalize_mission_name("Operation   Desert   Shield") == "operation desert shield"

    def test_empty_string(self):
        assert normalize_mission_name("") == ""

    def test_none(self):
        assert normalize_mission_name(None) == ""

    def test_replaces_hyphens_with_spaces(self):
        assert normalize_mission_name("EU-BAM Rafah") == "eu bam rafah"

    def test_strict_removes_all_separators(self):
        assert normalize_mission_name_strict("EU-BAM Rafah") == "eubamrafah"
        assert normalize_mission_name_strict("UNIFIL II") == "unifilii"
        assert normalize_mission_name_strict("KFOR") == "kfor"


# =============================================================================
# TEST NORMALIZZAZIONE ORGANIZZAZIONI
# =============================================================================

class TestNormalizeOrganization:
    # ONU patterns
    def test_unifil_is_onu(self):
        assert normalize_organization("UNIFIL", "") == "ONU"

    def test_unmiss_is_onu(self):
        assert normalize_organization("UNMISS", "") == "ONU"

    def test_minurso_is_onu(self):
        assert normalize_organization("MINURSO", "") == "ONU"

    def test_unama_is_onu(self):
        assert normalize_organization("UNAMA", "") == "ONU"

    def test_unficyp_is_onu(self):
        assert normalize_organization("UNFICYP", "") == "ONU"

    def test_untso_is_onu(self):
        assert normalize_organization("UNTSO", "") == "ONU"

    def test_framework_un(self):
        assert normalize_organization("Some Mission", "UN") == "ONU"

    def test_framework_onu(self):
        assert normalize_organization("Some Mission", "ONU") == "ONU"

    def test_framework_united_nations(self):
        assert normalize_organization("Some Mission", "United Nations") == "ONU"

    # NATO patterns
    def test_kfor_is_nato(self):
        assert normalize_organization("KFOR", "") == "NATO"

    def test_isaf_is_nato(self):
        assert normalize_organization("ISAF", "") == "NATO"

    def test_resolute_support_is_nato(self):
        assert normalize_organization("Resolute Support", "") == "NATO"

    def test_framework_nato(self):
        assert normalize_organization("Some Mission", "NATO") == "NATO"

    # UE patterns
    def test_eutm_is_ue(self):
        assert normalize_organization("EUTM Mali", "") == "UE"

    def test_eucap_is_ue(self):
        assert normalize_organization("EUCAP Sahel", "") == "UE"

    def test_eunavfor_is_ue(self):
        assert normalize_organization("EUNAVFOR Atalanta", "") == "UE"

    def test_eulex_is_ue(self):
        assert normalize_organization("EULEX Kosovo", "") == "UE"

    def test_framework_eu(self):
        assert normalize_organization("Some Mission", "EU") == "UE"

    # ITA
    def test_framework_ita(self):
        assert normalize_organization("MISIN", "ITA") == "ITA"

    # Default
    def test_unknown_returns_altro(self):
        assert normalize_organization("Operazione Alba", "") == "Altro"

    def test_nan_framework_returns_altro(self):
        assert normalize_organization("Operazione Alba", "nan") == "Altro"


# =============================================================================
# TEST NORMALIZZAZIONE REGIONI
# =============================================================================

class TestNormalizeRegion:
    def test_americas_to_america(self):
        assert normalize_region("Americas") == "America"

    def test_america_stays(self):
        assert normalize_region("America") == "America"

    def test_middle_east(self):
        assert normalize_region("Middle East") == "Medio Oriente"

    def test_medio_oriente(self):
        assert normalize_region("Medio Oriente") == "Medio Oriente"

    def test_europe(self):
        assert normalize_region("Europe") == "Europa"

    def test_europa(self):
        assert normalize_region("Europa") == "Europa"

    def test_sub_saharan_africa(self):
        assert normalize_region("Sub-Saharan Africa") == "Africa"

    def test_asia(self):
        assert normalize_region("Asia") == "Asia"

    def test_far_east(self):
        assert normalize_region("Far East") == "Asia"

    def test_horn_of_africa(self):
        assert normalize_region("Horn of Africa") == "Africa"

    def test_empty(self):
        assert normalize_region("") == "Non specificata"

    def test_none(self):
        assert normalize_region(None) == "Non specificata"

    def test_oceania_maps_to_asia(self):
        assert normalize_region("Oceania") == "Asia"

    def test_unknown_passthrough(self):
        assert normalize_region("Antartide") == "Antartide"


# =============================================================================
# TEST NORMALIZZAZIONE COMMITMENT
# =============================================================================

class TestNormalizeCommitment:
    def test_head_of_mission(self):
        assert normalize_commitment("Head of Mission", "") == "Head of Mission"

    def test_troops_ground(self):
        assert normalize_commitment("Troops (ground forces)", "") == "Troops (ground forces)"

    def test_advisory_training(self):
        assert normalize_commitment("Advisory/Training", "") == "Advisory/Training"

    def test_naval(self):
        assert normalize_commitment("naval forces", "") == "Troops (naval)"

    def test_unifil_override(self):
        assert normalize_commitment("Troops", "UNIFIL") == "Head of Mission"

    def test_eunavfor_is_naval(self):
        assert normalize_commitment("something", "EUNAVFOR Atalanta") == "Troops (naval)"

    def test_kfor_is_ground(self):
        assert normalize_commitment("something", "KFOR") == "Troops (ground forces)"

    def test_empty_returns_troops(self):
        assert normalize_commitment("", "") == "Troops"

    def test_none_returns_troops(self):
        assert normalize_commitment(None, "") == "Troops"


# =============================================================================
# TEST NORMALIZZAZIONE COLONNE
# =============================================================================

class TestNormalizeColumns:
    def test_mission_to_nome(self):
        assert normalize_column_name("mission") == "nome"

    def test_country_to_paese(self):
        assert normalize_column_name("country") == "paese"

    def test_framework_to_tipo_missione(self):
        assert normalize_column_name("framework") == "tipo_missione"

    def test_date_start_to_data_inizio(self):
        assert normalize_column_name("date_start") == "data_inizio"

    def test_unknown_passthrough(self):
        assert normalize_column_name("unknown_col") == "unknown_col"

    def test_normalize_columns_dict(self):
        cols = ["mission", "country", "framework", "unknown"]
        mapping = normalize_columns(cols)
        assert mapping == {"mission": "nome", "country": "paese", "framework": "tipo_missione"}


# =============================================================================
# TEST CLASSIFICAZIONE PERIODO
# =============================================================================

class TestClassifyPeriod:
    def test_cold_war(self):
        assert classify_period(1960) == "1948-1990"

    def test_post_cold_war(self):
        assert classify_period(1995) == "1991-2001"

    def test_war_on_terror(self):
        assert classify_period(2005) == "2002-2015"

    def test_modern(self):
        assert classify_period(2020) == "2016-oggi"

    def test_pre_1948(self):
        assert classify_period(1940) == "Pre-1948"

    def test_none(self):
        assert classify_period(None) == "Non specificato"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
