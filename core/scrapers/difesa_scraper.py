"""
Scraper per il Ministero della Difesa italiano — difesa.it
Aggiornato v3.5: URL ristrutturati 2024/2025, parsing pagine missioni in corso.
"""

import re
from typing import Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper


class DifesaScraper(BaseScraper):
    """Estrae dati sulle missioni dal sito del Ministero della Difesa."""

    fonte = "difesa"

    # URL indice missioni in corso (ristrutturato 2024)
    INDEX_URL = "https://www.difesa.it/operazionimilitari/op-intern-corso/operazioni-int/26752.html"
    URLS = [INDEX_URL]  # compatibilità con test_scrapers
    BASE = "https://www.difesa.it"

    # Mapping nome link → organizzazione
    _ORG_HINTS = {
        "nato": "NATO", "kfor": "NATO", "sea guardian": "NATO",
        "nmi": "NATO", "baltic": "NATO", "air policing": "NATO",
        "standing naval": "NATO", "battle group": "NATO",
        "eunavfor": "UE", "eutm": "UE", "eucap": "UE",
        "eufor": "UE", "eulex": "UE", "eubam": "UE",
        "eupol": "UE", "eumam": "UE", "eumm": "UE",
        "eunavformed": "UE", "irini": "UE", "aspides": "UE",
        "unifil": "ONU", "unficyp": "ONU", "unmogip": "ONU",
        "minurso": "ONU",
        "mfo": "Multinational", "ctf": "Coalizione",
        "mibil": "Bilateral", "miasit": "Bilateral",
        "misin": "Bilateral", "miccd": "Bilateral",
        "levante": "ITA", "mediterraneo sicuro": "ITA",
        "mare sicuro": "ITA", "strade sicure": "ITA",
    }

    def scrape(self) -> List[Dict]:
        """Scarica l'indice delle missioni in corso e le pagine di dettaglio."""
        records: List[Dict] = []

        resp = self.get(self.INDEX_URL)
        if not resp:
            self.logger.error("Impossibile raggiungere l'indice difesa.it")
            return records

        soup = BeautifulSoup(resp.text, "html.parser")

        # Estrai tutti i link a pagine di missioni
        links = soup.select("a[href*='/operazionimilitari/']")
        seen: set = set()

        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if not text or len(text) < 4 or href in seen:
                continue
            # Filtra link di navigazione/menu
            if href == self.INDEX_URL or "operazioni-int/26752" in href:
                continue
            seen.add(href)

            full_url = urljoin(self.BASE, href)
            detail = self._scrape_detail(full_url, text)
            if detail:
                records.append(detail)

        self.logger.info(f"Difesa: {len(records)} missioni estratte dall'indice")
        return records

    def _scrape_detail(self, url: str, link_text: str) -> Optional[Dict]:
        """Scarica e analizza la pagina di dettaglio di una missione."""
        resp = self.get(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Titolo: h1 o primo heading
        h1 = soup.find("h1")
        nome = h1.get_text(strip=True) if h1 else link_text

        # Pulisci nome da prefissi comuni
        nome = re.sub(r"^(Operazione|Missione)\s+", "", nome, flags=re.IGNORECASE).strip()
        if not nome:
            nome = link_text

        # Estrai testo completo per regex
        body = soup.find("article") or soup.find("main") or soup.find("body")
        text = body.get_text(" ", strip=True) if body else ""

        # Paese dal link text (pattern: "Paese - Nome Missione")
        paese = ""
        if " - " in link_text:
            parts = link_text.split(" - ", 1)
            paese = parts[0].strip()
            if not nome or nome == link_text:
                nome = parts[1].strip() if len(parts) > 1 else nome

        # Fallback paese da testo
        if not paese:
            paese = self._extract(
                r"(?:in|presso|nel)\s+([A-Z][a-zà-ú]+(?:\s+[A-Z][a-zà-ú]+){0,3})", text
            ) or ""

        # Date
        data_inizio = self._extract_date(text, start=True)
        data_fine = self._extract_date(text, start=False)

        # Personale
        personale = self._extract(
            r"(?:personale|effettivi|militari|unità)[:\s]*(?:circa\s+)?(\d[\d.]*)", text
        )

        # Costo
        costo = self._extract(
            r"(?:costo|spesa|onere|stanziamento)[:\s]*€?\s*([\d.,]+)\s*(?:milion|mln)", text
        )

        # Organizzazione
        tipo_missione = self._guess_org(nome, link_text)

        return {
            "nome": nome.strip(),
            "paese": paese.strip(),
            "data_inizio": (data_inizio or "").strip(),
            "data_fine": (data_fine or "").strip(),
            "personale_totale": self._parse_int(personale),
            "costo_totale": self._parse_float(costo) * 1_000_000 if costo else 0.0,
            "tipo_missione": tipo_missione,
            "mandato": "",
            "link_documento": url,
        }

    def _guess_org(self, nome: str, link_text: str) -> str:
        """Indovina l'organizzazione dal nome missione."""
        combined = f"{nome} {link_text}".lower()
        for keyword, org in self._ORG_HINTS.items():
            if keyword in combined:
                return org
        return ""

    @staticmethod
    def _extract(pattern: str, text: str) -> Optional[str]:
        m = re.search(pattern, text, re.IGNORECASE)
        return m.group(1) if m else None

    @staticmethod
    def _extract_date(text: str, start: bool = True) -> Optional[str]:
        """Estrai data inizio o fine dal testo."""
        if start:
            patterns = [
                r"(?:dal|inizio|avviata?|istituita?|operativa? dal)[:\s]*(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})",
                r"(?:dal|inizio)[:\s]*(\d{4})",
            ]
        else:
            patterns = [
                r"(?:al|fine|conclusa?|terminata?)[:\s]*(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})",
            ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                return m.group(1)
        return None

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
