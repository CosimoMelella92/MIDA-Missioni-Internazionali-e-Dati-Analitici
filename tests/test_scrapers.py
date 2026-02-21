"""
Test suite per gli scrapers MIDA.
Testa struttura, interfaccia, e logica di parsing senza fare richieste HTTP reali.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pathlib import Path

from core.scrapers.base_scraper import BaseScraper, SCRAPER_COLUMNS
from core.scrapers import (
    ALL_SCRAPERS,
    DifesaScraper,
    CameraScraper,
    SenatoScraper,
    EsteriScraper,
    EEASScraper,
    NATOScraper,
    UNScraper,
)


# =========================================================================
# Test BaseScraper
# =========================================================================

class TestBaseScraper:
    """Test per la classe base astratta."""

    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            BaseScraper()

    def test_scraper_columns_defined(self):
        assert "nome" in SCRAPER_COLUMNS
        assert "paese" in SCRAPER_COLUMNS
        assert "fonte" in SCRAPER_COLUMNS
        assert len(SCRAPER_COLUMNS) == 10

    def test_to_dataframe_empty(self):
        scraper = DifesaScraper()
        df = scraper.to_dataframe([])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == SCRAPER_COLUMNS

    def test_to_dataframe_with_records(self):
        scraper = DifesaScraper()
        records = [
            {"nome": "Test Mission", "paese": "Italia"},
            {"nome": "Another", "paese": "Francia", "personale_totale": 100},
        ]
        df = scraper.to_dataframe(records)
        assert len(df) == 2
        assert df.iloc[0]["fonte"] == "difesa"
        assert list(df.columns) == SCRAPER_COLUMNS

    def test_to_dataframe_fills_missing_columns(self):
        scraper = NATOScraper()
        records = [{"nome": "KFOR"}]
        df = scraper.to_dataframe(records)
        assert len(df) == 1
        assert df.iloc[0]["fonte"] == "nato"
        assert df.iloc[0]["paese"] == ""


# =========================================================================
# Test struttura di tutti gli scrapers
# =========================================================================

class TestAllScrapersStructure:
    """Verifica che tutti gli scrapers rispettino l'interfaccia."""

    def test_all_scrapers_count(self):
        assert len(ALL_SCRAPERS) == 7

    @pytest.mark.parametrize("scraper_cls", ALL_SCRAPERS)
    def test_inherits_from_base(self, scraper_cls):
        assert issubclass(scraper_cls, BaseScraper)

    @pytest.mark.parametrize("scraper_cls", ALL_SCRAPERS)
    def test_has_fonte(self, scraper_cls):
        assert hasattr(scraper_cls, "fonte")
        assert isinstance(scraper_cls.fonte, str)
        assert len(scraper_cls.fonte) > 0

    @pytest.mark.parametrize("scraper_cls", ALL_SCRAPERS)
    def test_has_urls(self, scraper_cls):
        assert hasattr(scraper_cls, "URLS")
        assert isinstance(scraper_cls.URLS, list)
        assert len(scraper_cls.URLS) > 0

    @pytest.mark.parametrize("scraper_cls", ALL_SCRAPERS)
    def test_has_scrape_method(self, scraper_cls):
        assert hasattr(scraper_cls, "scrape")
        assert callable(getattr(scraper_cls, "scrape"))

    @pytest.mark.parametrize("scraper_cls", ALL_SCRAPERS)
    def test_has_run_method(self, scraper_cls):
        assert hasattr(scraper_cls, "run")
        assert callable(getattr(scraper_cls, "run"))

    @pytest.mark.parametrize("scraper_cls", ALL_SCRAPERS)
    def test_instantiation(self, scraper_cls):
        scraper = scraper_cls()
        assert scraper.fonte == scraper_cls.fonte
        assert scraper.session is not None
        assert scraper.timeout == 30
        assert scraper.max_retries == 3

    def test_unique_fonti(self):
        fonti = [cls.fonte for cls in ALL_SCRAPERS]
        assert len(fonti) == len(set(fonti)), f"Duplicate fonti: {fonti}"


# =========================================================================
# Test DifesaScraper parsing
# =========================================================================

class TestDifesaScraper:

    def test_extract(self):
        assert DifesaScraper._extract(r"(\d+) soldati", "Ci sono 500 soldati") == "500"
        assert DifesaScraper._extract(r"(\d+) soldati", "nessun dato") is None

    def test_parse_int(self):
        assert DifesaScraper._parse_int("1.500") == 1500
        assert DifesaScraper._parse_int("200") == 200
        assert DifesaScraper._parse_int(None) == 0
        assert DifesaScraper._parse_int("") == 0

    def test_parse_float(self):
        assert DifesaScraper._parse_float("1.500,50") == 1500.50
        assert DifesaScraper._parse_float(None) == 0.0

    def test_extract_date_start(self):
        assert DifesaScraper._extract_date("operativa dal 15/06/2015", start=True) == "15/06/2015"
        assert DifesaScraper._extract_date("inizio 2018", start=True) == "2018"
        assert DifesaScraper._extract_date("nessuna data", start=True) is None

    def test_extract_date_end(self):
        assert DifesaScraper._extract_date("conclusa 31/12/2020", start=False) == "31/12/2020"
        assert DifesaScraper._extract_date("nessuna data", start=False) is None

    def test_guess_org(self):
        scraper = DifesaScraper()
        assert scraper._guess_org("KFOR", "Kosovo - KFOR") == "NATO"
        assert scraper._guess_org("EUNAVFOR Atalanta", "Oceano Indiano") == "UE"
        assert scraper._guess_org("UNIFIL", "Libano - UNIFIL") == "ONU"
        assert scraper._guess_org("MIASIT", "Libia") == "Bilateral"
        assert scraper._guess_org("Sconosciuta", "link generico") == ""

    @patch.object(DifesaScraper, "get")
    def test_scrape_empty_response(self, mock_get):
        mock_get.return_value = None
        scraper = DifesaScraper()
        records = scraper.scrape()
        assert records == []

    @patch.object(DifesaScraper, "get")
    def test_scrape_with_mock_index(self, mock_get):
        index_html = """
        <html><body>
        <a href="/operazionimilitari/op-intern-corso/unifil/default/27993.html">Libano - UNIFIL</a>
        <a href="/operazionimilitari/op-intern-corso/kfor/default/27717.html">Kosovo - KFOR - Joint Enterprise</a>
        </body></html>
        """
        detail_html = """
        <html><body>
        <h1>UNIFIL</h1>
        <article><p>La missione UNIFIL è operativa dal 1978 in Libano con circa 1.200 militari italiani.</p></article>
        </body></html>
        """
        mock_resp_index = MagicMock()
        mock_resp_index.text = index_html
        mock_resp_detail = MagicMock()
        mock_resp_detail.text = detail_html
        mock_get.side_effect = [mock_resp_index, mock_resp_detail, mock_resp_detail]

        scraper = DifesaScraper()
        records = scraper.scrape()
        assert len(records) == 2
        assert any("UNIFIL" in r["nome"] for r in records)


# =========================================================================
# Test NATOScraper parsing
# =========================================================================

class TestNATOScraper:

    def test_get_sibling_text(self):
        from bs4 import BeautifulSoup
        html = "<div><h3>Title</h3><p>Para 1</p><p>Para 2</p><h3>Next</h3></div>"
        soup = BeautifulSoup(html, "html.parser")
        h3 = soup.find("h3")
        text = NATOScraper._get_sibling_text(h3, max_siblings=5)
        assert "Para 1" in text
        assert "Para 2" in text

    @patch.object(NATOScraper, "get")
    def test_scrape_with_mock_html(self, mock_get):
        html = """
        <html><body>
        <h3>Operation Sea Guardian</h3>
        <p>Deployed in Mediterranean since 2016. About 1,200 personnel.</p>
        <h3>KFOR</h3>
        <p>NATO mission in Kosovo since 1999. 3,500 troops deployed.</p>
        </body></html>
        """
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_get.return_value = mock_resp

        scraper = NATOScraper()
        records = scraper.scrape()
        assert len(records) >= 2
        names = [r["nome"] for r in records]
        assert "Operation Sea Guardian" in names
        assert "KFOR" in names
        # Check tipo_missione is set
        for r in records:
            assert r["tipo_missione"] == "NATO"


# =========================================================================
# Test UNScraper parsing
# =========================================================================

class TestUNScraper:

    def test_extract_country(self):
        assert UNScraper._extract_country("deployed in Lebanon since 1978") == "Lebanon"
        assert UNScraper._extract_country("no country here") == ""

    @patch.object(UNScraper, "get")
    def test_scrape_with_mock_table(self, mock_get):
        html = """
        <html><body>
        <table>
        <tr><th>Mission</th><th>Country</th><th>Year</th></tr>
        <tr><td>UNIFIL</td><td>Lebanon</td><td>1978</td></tr>
        <tr><td>UNMISS</td><td>South Sudan</td><td>2011</td></tr>
        </table>
        </body></html>
        """
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_get.return_value = mock_resp

        scraper = UNScraper()
        records = scraper.scrape()
        assert len(records) >= 2
        names = [r["nome"] for r in records]
        assert "UNIFIL" in names
        assert "UNMISS" in names
        for r in records:
            assert r["tipo_missione"] == "ONU"


# =========================================================================
# Test EEASScraper parsing
# =========================================================================

class TestEEASScraper:

    def test_classify_mission(self):
        assert EEASScraper._classify_mission("EUNAVFOR Atalanta", "military operation") == "UE"
        assert EEASScraper._classify_mission("EUPOL COPPS", "civilian mission") == "UE"

    @patch.object(EEASScraper, "get")
    def test_scrape_fallback_acronyms(self, mock_get):
        html = """
        <html><body>
        <p>The EU currently has several CSDP missions including EUNAVFOR Atalanta
        and EUTM Somalia as well as EUPOL COPPS in Palestine.</p>
        </body></html>
        """
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_get.return_value = mock_resp

        scraper = EEASScraper()
        records = scraper.scrape()
        assert len(records) >= 2
        for r in records:
            assert r["tipo_missione"] == "UE"


# =========================================================================
# Test run() con mock
# =========================================================================

class TestRunMethod:

    @patch.object(DifesaScraper, "scrape")
    def test_run_returns_dataframe(self, mock_scrape, tmp_path):
        mock_scrape.return_value = [
            {"nome": "Test", "paese": "Italia", "personale_totale": 100},
        ]
        scraper = DifesaScraper(data_dir=str(tmp_path))
        df = scraper.run()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]["fonte"] == "difesa"

    @patch.object(NATOScraper, "scrape")
    def test_run_handles_exception(self, mock_scrape, tmp_path):
        mock_scrape.side_effect = RuntimeError("Network error")
        scraper = NATOScraper(data_dir=str(tmp_path))
        df = scraper.run()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @patch.object(UNScraper, "scrape")
    def test_run_saves_files(self, mock_scrape, tmp_path):
        mock_scrape.return_value = [
            {"nome": "UNIFIL", "paese": "Lebanon"},
        ]
        scraper = UNScraper(data_dir=str(tmp_path))
        df = scraper.run()
        assert len(df) == 1
        # Check raw JSON was saved
        raw_files = list((tmp_path / "raw").glob("*.json"))
        assert len(raw_files) == 1
        # Check CSV was saved
        csv_files = list((tmp_path / "processed").glob("*.csv"))
        assert len(csv_files) == 1
