"""Scraper per il Ministero della Difesa italiano — difesa.it"""

import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper


class DifesaScraper(BaseScraper):
    """Estrae dati sulle missioni dal sito del Ministero della Difesa."""

    fonte = "difesa"

    URLS = [
        "https://www.difesa.it/OperazioniMilitari/op_intern_corso/Pagine/default.aspx",
        "https://www.difesa.it/OperazioniMilitari/op_intern_concluse/Pagine/default.aspx",
    ]

    def scrape(self) -> List[Dict]:
        records: List[Dict] = []
        for url in self.URLS:
            is_concluded = "concluse" in url
            page_records = self._scrape_listing(url, is_concluded)
            records.extend(page_records)
        self.logger.info(f"Difesa: {len(records)} missioni estratte")
        return records

    def _scrape_listing(self, url: str, concluded: bool) -> List[Dict]:
        """Scarica la pagina elenco e estrae i link alle singole missioni."""
        resp = self.get(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        records = []

        # Cerca link alle pagine delle singole missioni
        links = soup.select("a[href*='OperazioniMilitari']")
        seen = set()
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if not text or len(text) < 3 or href in seen:
                continue
            seen.add(href)

            full_url = href if href.startswith("http") else f"https://www.difesa.it{href}"
            detail = self._scrape_detail(full_url, text, concluded)
            if detail:
                records.append(detail)

        return records

    def _scrape_detail(self, url: str, fallback_name: str, concluded: bool) -> Optional[Dict]:
        """Scarica la pagina di dettaglio di una missione."""
        resp = self.get(url)
        if not resp:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        nome = self._extract(r"(?:Missione|Operazione)\s+[\"']?([A-Za-z\s\-]+)", text) or fallback_name
        paese = self._extract(r"(?:in|presso)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", text)
        data_inizio = self._extract(r"(?:dal|inizio[:\s]+)(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})", text)
        data_fine = self._extract(r"(?:al|fine[:\s]+)(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})", text) if concluded else ""
        personale = self._extract(r"(?:personale|effettivi|militari)[:\s]*(\d[\d.]*)", text)
        costo = self._extract(r"(?:costo|spesa|onere)[:\s]*€?\s*([\d.,]+)", text)

        return {
            "nome": nome.strip(),
            "paese": (paese or "").strip(),
            "data_inizio": (data_inizio or "").strip(),
            "data_fine": (data_fine or "").strip(),
            "personale_totale": self._parse_int(personale),
            "costo_totale": self._parse_float(costo),
            "tipo_missione": "",
            "mandato": "",
            "link_documento": url,
        }

    @staticmethod
    def _extract(pattern: str, text: str) -> Optional[str]:
        m = re.search(pattern, text, re.IGNORECASE)
        return m.group(1) if m else None

    @staticmethod
    def _parse_int(s: Optional[str]) -> int:
        if not s:
            return 0
        try:
            return int(re.sub(r"[^\d]", "", s))
        except ValueError:
            return 0

    @staticmethod
    def _parse_float(s: Optional[str]) -> float:
        if not s:
            return 0.0
        try:
            return float(s.replace(".", "").replace(",", "."))
        except ValueError:
            return 0.0
