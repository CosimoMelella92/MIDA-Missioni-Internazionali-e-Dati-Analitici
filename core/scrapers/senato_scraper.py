"""Scraper per il Senato della Repubblica — senato.it"""

import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper


class SenatoScraper(BaseScraper):
    """Estrae dati sulle missioni dai dossier del Senato."""

    fonte = "senato"

    URLS = [
        "https://www.senato.it/leg/19/BGT/Schede/Dossier/Testi/IFR.htm",
        "https://www.senato.it/leg/19/BGT/Schede/docuocc_702.htm",
    ]

    def scrape(self) -> List[Dict]:
        records: List[Dict] = []
        for url in self.URLS:
            page_records = self._scrape_page(url)
            records.extend(page_records)
        self.logger.info(f"Senato: {len(records)} missioni estratte")
        return records

    def _scrape_page(self, url: str) -> List[Dict]:
        resp = self.get(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        records = []

        # Il Senato pubblica dossier con elenchi di missioni
        for item in soup.find_all(["li", "p", "tr"]):
            text = item.get_text(" ", strip=True)
            if len(text) < 10:
                continue

            # Cerca pattern tipici di missioni internazionali
            mission_match = re.search(
                r"(UNIFIL|KFOR|EUNAVFOR|EUTM|UNMISS|MINUSMA|EUPOL|"
                r"Resolute Support|Sea Guardian|Active Endeavour|"
                r"Missione\s+\w+|Operazione\s+\w+)",
                text, re.IGNORECASE,
            )
            if not mission_match:
                continue

            nome = mission_match.group(1).strip()
            paese = self._extract(r"(?:in|presso|nel)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", text)
            data_inizio = self._extract(r"(\d{4})", text)
            personale = self._extract(r"(\d[\d.]*)\s*(?:unit[àa]|militari|personale|effettivi)", text)

            records.append({
                "nome": nome,
                "paese": (paese or "").strip(),
                "data_inizio": f"01/01/{data_inizio}" if data_inizio else "",
                "data_fine": "",
                "personale_totale": self._parse_int(personale),
                "costo_totale": 0.0,
                "tipo_missione": "",
                "mandato": "",
                "link_documento": url,
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
