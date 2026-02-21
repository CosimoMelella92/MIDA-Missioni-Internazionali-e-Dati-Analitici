"""Scraper per la Camera dei Deputati — camera.it"""

import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper


class CameraScraper(BaseScraper):
    """Estrae dati sulle missioni dai documenti della Camera dei Deputati."""

    fonte = "camera"

    URLS = [
        "https://www.camera.it/leg19/1132",
        "https://temi.camera.it/leg19/temi/missioni-internazionali",
    ]

    PDF_URLS = [
        "https://www.camera.it/application/xmanager/projects/leg19/attachments/upload_file_doc_acquisiti/pdfs/000/001/missioni_internazionali.pdf",
    ]

    def scrape(self) -> List[Dict]:
        records: List[Dict] = []

        # 1. Scrape pagine HTML
        for url in self.URLS:
            page_records = self._scrape_page(url)
            records.extend(page_records)

        self.logger.info(f"Camera: {len(records)} missioni estratte")
        return records

    def _scrape_page(self, url: str) -> List[Dict]:
        resp = self.get(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        records = []

        # Cerca sezioni missioni — la Camera usa div/section con titoli
        sections = soup.find_all(["h2", "h3", "h4", "strong"])
        for section in sections:
            text = section.get_text(strip=True)
            if not text or len(text) < 5:
                continue

            # Cerca pattern di nomi missione
            if re.search(r"(missione|operazione|UNIFIL|KFOR|EUNAVFOR|EUTM|UNMISS)", text, re.IGNORECASE):
                record = self._parse_mission_block(section, text, url)
                if record and record.get("nome"):
                    records.append(record)

        return records

    def _parse_mission_block(self, element, title: str, url: str) -> Optional[Dict]:
        """Estrae dati da un blocco missione nella pagina."""
        # Prendi il testo del blocco successivo
        context = ""
        sibling = element.find_next_sibling()
        for _ in range(5):
            if sibling:
                context += " " + sibling.get_text(" ", strip=True)
                sibling = sibling.find_next_sibling()

        full_text = title + " " + context

        nome = title.strip()
        paese = self._extract(r"(?:in|presso|nel)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", full_text)
        data_inizio = self._extract(r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})", full_text)
        personale = self._extract(r"(\d[\d.]*)\s*(?:unit[àa]|militari|personale)", full_text)

        return {
            "nome": nome,
            "paese": (paese or "").strip(),
            "data_inizio": (data_inizio or "").strip(),
            "data_fine": "",
            "personale_totale": self._parse_int(personale),
            "costo_totale": 0.0,
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
