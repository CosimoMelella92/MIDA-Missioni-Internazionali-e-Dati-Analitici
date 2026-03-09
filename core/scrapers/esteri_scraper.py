"""Scraper per il Ministero degli Affari Esteri — esteri.it"""

import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper


class EsteriScraper(BaseScraper):
    """Estrae dati sulle missioni dal sito del MAECI."""

    fonte = "esteri"

    URLS = [
        "https://www.esteri.it/it/politica-estera-e-cooperazione-allo-sviluppo/politica_estera/missioni-internazionali/",
    ]

    def scrape(self) -> List[Dict]:
        records: List[Dict] = []
        for url in self.URLS:
            page_records = self._scrape_page(url)
            records.extend(page_records)
        self.logger.info(f"Esteri: {len(records)} missioni estratte")
        return records

    def _scrape_page(self, url: str) -> List[Dict]:
        resp = self.get(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        records = []

        # Il MAECI elenca missioni in card/blocchi
        for card in soup.find_all(["article", "div", "section"], class_=re.compile(r"card|item|mission", re.I)):
            title_el = card.find(["h2", "h3", "h4", "a", "strong"])
            if not title_el:
                continue
            nome = title_el.get_text(strip=True)
            if len(nome) < 3:
                continue

            body = card.get_text(" ", strip=True)
            paese = self._extract(r"(?:in|presso|nel)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", body)
            data_inizio = self._extract(r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})", body)
            personale = self._extract(r"(\d[\d.]*)\s*(?:unit[àa]|militari|personale)", body)

            records.append({
                "nome": nome,
                "paese": (paese or "").strip(),
                "data_inizio": (data_inizio or "").strip(),
                "data_fine": "",
                "personale_totale": self._parse_int(personale),
                "costo_totale": 0.0,
                "tipo_missione": "",
                "mandato": "",
                "link_documento": url,
            })

        # Fallback: cerca link a missioni specifiche
        if not records:
            for link in soup.find_all("a", href=True):
                text = link.get_text(strip=True)
                if re.search(r"(missione|operazione|UNIFIL|KFOR)", text, re.IGNORECASE) and len(text) > 5:
                    records.append({
                        "nome": text,
                        "paese": "",
                        "data_inizio": "",
                        "data_fine": "",
                        "personale_totale": 0,
                        "costo_totale": 0.0,
                        "tipo_missione": "",
                        "mandato": "",
                        "link_documento": link["href"] if link["href"].startswith("http") else url,
                    })

        return records

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
