"""Scraper per l'European External Action Service — eeas.europa.eu"""

import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper


class EEASScraper(BaseScraper):
    """Estrae dati sulle missioni CSDP dal sito EEAS (UE)."""

    fonte = "eeas"

    URLS = [
        "https://www.eeas.europa.eu/eeas/missions-and-operations_en",
    ]

    def scrape(self) -> List[Dict]:
        records: List[Dict] = []
        for url in self.URLS:
            page_records = self._scrape_page(url)
            records.extend(page_records)
        self.logger.info(f"EEAS: {len(records)} missioni estratte")
        return records

    def _scrape_page(self, url: str) -> List[Dict]:
        resp = self.get(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        records = []

        # EEAS lista missioni in card/blocchi con titolo e dettagli
        cards = soup.find_all(["article", "div", "li"], class_=re.compile(
            r"card|listing|node|mission|operation|view-row", re.I
        ))

        for card in cards:
            title_el = card.find(["h2", "h3", "h4", "a", "strong"])
            if not title_el:
                continue
            nome = title_el.get_text(strip=True)
            if len(nome) < 3 or nome.lower() in ("read more", "see all", "missions and operations"):
                continue

            body = card.get_text(" ", strip=True)
            paese = self._extract_country(body)
            launched = self._extract(r"(?:launched|started|since)\s*:?\s*(\d{4})", body)
            mission_type = self._classify_mission(nome, body)

            href = ""
            link_el = card.find("a", href=True)
            if link_el:
                h = link_el["href"]
                href = h if h.startswith("http") else f"https://www.eeas.europa.eu{h}"

            records.append({
                "nome": nome,
                "paese": paese,
                "data_inizio": f"01/01/{launched}" if launched else "",
                "data_fine": "",
                "personale_totale": 0,
                "costo_totale": 0.0,
                "tipo_missione": mission_type,
                "mandato": "",
                "link_documento": href or url,
            })

        # Fallback: parse testo libero per acronimi noti
        if not records:
            text = soup.get_text(" ", strip=True)
            for m in re.finditer(
                r"(EU[A-Z]{2,}\s+\w+|CSDP\s+\w+|EUNAVFOR\s+\w+|EUTM\s+\w+|EUPOL\s+\w+|EUBAM\s+\w+)",
                text,
            ):
                records.append({
                    "nome": m.group(1).strip(),
                    "paese": "",
                    "data_inizio": "",
                    "data_fine": "",
                    "personale_totale": 0,
                    "costo_totale": 0.0,
                    "tipo_missione": "UE",
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
    def _classify_mission(nome: str, body: str) -> str:
        combined = (nome + " " + body).upper()
        if any(k in combined for k in ["MILITARY", "NAVAL", "EUNAVFOR", "EUTM", "EUFOR"]):
            return "UE"
        if any(k in combined for k in ["CIVILIAN", "EUPOL", "EUBAM", "EULEX", "EUCAP"]):
            return "UE"
        return "UE"
