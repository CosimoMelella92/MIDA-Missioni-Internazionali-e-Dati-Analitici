"""Scraper per NATO — nato.int"""

import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper


class NATOScraper(BaseScraper):
    """Estrae dati sulle operazioni NATO."""

    fonte = "nato"

    URLS = [
        "https://www.nato.int/cps/en/natohq/topics_52060.htm",
        "https://www.nato.int/cps/en/natohq/topics_37750.htm",
    ]

    def scrape(self) -> List[Dict]:
        records: List[Dict] = []
        for url in self.URLS:
            is_past = "37750" in url
            page_records = self._scrape_page(url, concluded=is_past)
            records.extend(page_records)
        self.logger.info(f"NATO: {len(records)} operazioni estratte")
        return records

    def _scrape_page(self, url: str, concluded: bool = False) -> List[Dict]:
        resp = self.get(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        records = []

        # NATO usa blocchi con titoli per ogni operazione
        for heading in soup.find_all(["h2", "h3", "h4"]):
            text = heading.get_text(strip=True)
            if len(text) < 3:
                continue

            # Filtra solo heading che sembrano nomi di operazioni
            if not re.search(
                r"(operation|mission|KFOR|ISAF|Resolute|Sea Guardian|"
                r"Active Endeavour|Unified Protector|Allied|Eagle|Ocean Shield|"
                r"Essential Harvest|Amber Fox|Allied Harmony)",
                text, re.IGNORECASE,
            ):
                continue

            # Raccogli contesto dai siblings
            context = self._get_sibling_text(heading, max_siblings=5)
            full = text + " " + context

            paese = self._extract(r"(?:in|deployed to)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", full)
            year_start = self._extract(r"(?:since|from|launched|began)\s*:?\s*(\d{4})", full)
            year_end = self._extract(r"(?:ended|concluded|terminated)\s*:?\s*(\d{4})", full) if concluded else None
            personale = self._extract(r"([\d,]+)\s*(?:troops|personnel|soldiers)", full)

            href = ""
            link_el = heading.find("a", href=True)
            if link_el:
                h = link_el["href"]
                href = h if h.startswith("http") else f"https://www.nato.int{h}"

            records.append({
                "nome": text.strip(),
                "paese": (paese or "").strip(),
                "data_inizio": f"01/01/{year_start}" if year_start else "",
                "data_fine": f"31/12/{year_end}" if year_end else "",
                "personale_totale": self._parse_int(personale),
                "costo_totale": 0.0,
                "tipo_missione": "NATO",
                "mandato": "",
                "link_documento": href or url,
            })

        return records

    @staticmethod
    def _get_sibling_text(el, max_siblings: int = 5) -> str:
        parts = []
        sib = el.find_next_sibling()
        for _ in range(max_siblings):
            if sib is None or sib.name in ("h1", "h2", "h3"):
                break
            parts.append(sib.get_text(" ", strip=True))
            sib = sib.find_next_sibling()
        return " ".join(parts)

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
