"""Scraper per UN Peacekeeping — peacekeeping.un.org"""

import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper


class UNScraper(BaseScraper):
    """Estrae dati sulle operazioni di peacekeeping ONU."""

    fonte = "un"

    URLS = [
        "https://peacekeeping.un.org/en/where-we-operate",
        "https://peacekeeping.un.org/en/past-peacekeeping-operations",
    ]

    def scrape(self) -> List[Dict]:
        records: List[Dict] = []
        for url in self.URLS:
            is_past = "past" in url
            page_records = self._scrape_page(url, concluded=is_past)
            records.extend(page_records)
        self.logger.info(f"UN: {len(records)} operazioni estratte")
        return records

    def _scrape_page(self, url: str, concluded: bool = False) -> List[Dict]:
        resp = self.get(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        records = []

        # UN Peacekeeping lista missioni in card/blocchi o tabelle
        # Prova prima le card
        cards = soup.find_all(["article", "div", "li"], class_=re.compile(
            r"card|node|mission|operation|view-row|field-item", re.I
        ))

        for card in cards:
            title_el = card.find(["h2", "h3", "h4", "a", "strong", "span"])
            if not title_el:
                continue
            nome = title_el.get_text(strip=True)
            # Filtra nomi troppo corti o generici
            if len(nome) < 3 or nome.lower() in ("read more", "learn more", "where we operate"):
                continue
            # Filtra solo se sembra un acronimo UN o nome missione
            if not re.search(r"(UN[A-Z]{2,}|MINUSMA|MONUSCO|UNMISS|UNIFIL|UNDOF|UNFICYP|MINURSO|UNISFA|Mission|Operation)", nome, re.IGNORECASE):
                continue

            body = card.get_text(" ", strip=True)
            paese = self._extract_country(body)
            year_start = self._extract(r"(?:since|from|established)\s*:?\s*(\d{4})", body)
            year_end = self._extract(r"(?:ended|closed|terminated)\s*:?\s*(\d{4})", body) if concluded else None
            personale = self._extract(r"([\d,]+)\s*(?:personnel|troops|uniformed)", body)

            href = ""
            link_el = card.find("a", href=True)
            if link_el:
                h = link_el["href"]
                href = h if h.startswith("http") else f"https://peacekeeping.un.org{h}"

            records.append({
                "nome": nome,
                "paese": paese,
                "data_inizio": f"01/01/{year_start}" if year_start else "",
                "data_fine": f"31/12/{year_end}" if year_end else "",
                "personale_totale": self._parse_int(personale),
                "costo_totale": 0.0,
                "tipo_missione": "ONU",
                "mandato": "",
                "link_documento": href or url,
            })

        # Fallback: cerca tabelle
        if not records:
            for table in soup.find_all("table"):
                records.extend(self._parse_table(table, url, concluded))

        # Fallback: cerca acronimi nel testo
        if not records:
            text = soup.get_text(" ", strip=True)
            for m in re.finditer(
                r"(UN[A-Z]{3,}\w*|MINUSMA|MONUSCO|UNMISS|UNIFIL|UNDOF|UNFICYP|MINURSO|UNISFA)",
                text,
            ):
                records.append({
                    "nome": m.group(1).strip(),
                    "paese": "",
                    "data_inizio": "",
                    "data_fine": "",
                    "personale_totale": 0,
                    "costo_totale": 0.0,
                    "tipo_missione": "ONU",
                    "mandato": "",
                    "link_documento": url,
                })

        return records

    def _parse_table(self, table, url: str, concluded: bool) -> List[Dict]:
        """Estrae missioni da una tabella HTML."""
        records = []
        rows = table.find_all("tr")
        for row in rows[1:]:  # skip header
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            nome = cells[0].get_text(strip=True)
            if len(nome) < 3:
                continue
            paese = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            year = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            year_match = re.search(r"(\d{4})", year)

            records.append({
                "nome": nome,
                "paese": paese,
                "data_inizio": f"01/01/{year_match.group(1)}" if year_match else "",
                "data_fine": "",
                "personale_totale": 0,
                "costo_totale": 0.0,
                "tipo_missione": "ONU",
                "mandato": "",
                "link_documento": url,
            })
        return records

    @staticmethod
    def _extract(pattern: str, text: str) -> Optional[str]:
        m = re.search(pattern, text, re.IGNORECASE)
        return m.group(1) if m else None

    @staticmethod
    def _extract_country(text: str) -> str:
        m = re.search(r"(?:in|deployed to|location)\s*:?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", text)
        return m.group(1).strip() if m else ""

    @staticmethod
    def _parse_int(s: Optional[str]) -> int:
        if not s:
            return 0
        try:
            return int(re.sub(r"[^\d]", "", s))
        except ValueError:
            return 0
