"""
Base scraper per MIDA — classe astratta con HTTP session, retry, logging, salvataggio.
Tutti gli scrapers ereditano da qui.
"""

import json
import logging
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Colonne standard output di ogni scraper
SCRAPER_COLUMNS = [
    "nome", "paese", "data_inizio", "data_fine",
    "personale_totale", "costo_totale", "tipo_missione",
    "mandato", "fonte", "link_documento",
]


class BaseScraper(ABC):
    """Classe base per tutti gli scrapers MIDA."""

    fonte: str = "base"

    def __init__(
        self,
        user_agent: str = "MIDA-Bot/2.0 (+https://github.com/MIDA)",
        timeout: int = 30,
        max_retries: int = 3,
        delay_range: tuple[float, float] = (1.0, 3.0),
        data_dir: str = "data",
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.delay_range = delay_range
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.documents_dir = self.data_dir / "documents"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.documents_dir.mkdir(parents=True, exist_ok=True)

        self.session = self._build_session(user_agent)
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------
    def _build_session(self, user_agent: str) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.8,en-US;q=0.5,en;q=0.3",
        })
        return session

    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        """GET con delay, retry e logging."""
        time.sleep(random.uniform(*self.delay_range))
        try:
            resp = self.session.get(url, timeout=self.timeout, **kwargs)
            resp.raise_for_status()
            self.logger.info(f"OK {resp.status_code} {url}")
            return resp
        except requests.RequestException as exc:
            self.logger.error(f"FAIL {url}: {exc}")
            return None

    # ------------------------------------------------------------------
    # Salvataggio
    # ------------------------------------------------------------------
    def save_raw_json(self, data: Any, filename: str) -> Path:
        path = self.raw_dir / filename
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        self.logger.info(f"Saved raw JSON: {path}")
        return path

    def save_csv(self, df: pd.DataFrame, filename: str) -> Path:
        path = self.data_dir / "processed" / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8")
        self.logger.info(f"Saved CSV ({len(df)} rows): {path}")
        return path

    # ------------------------------------------------------------------
    # Normalizzazione output
    # ------------------------------------------------------------------
    def to_dataframe(self, records: List[Dict]) -> pd.DataFrame:
        """Converte una lista di dict in DataFrame con colonne standard."""
        if not records:
            return pd.DataFrame(columns=SCRAPER_COLUMNS)
        df = pd.DataFrame(records)
        for col in SCRAPER_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        df["fonte"] = self.fonte
        return df[SCRAPER_COLUMNS]

    # ------------------------------------------------------------------
    # Interfaccia pubblica
    # ------------------------------------------------------------------
    @abstractmethod
    def scrape(self) -> List[Dict]:
        """Esegue lo scraping e restituisce una lista di dict (una per missione)."""
        ...

    def run(self) -> pd.DataFrame:
        """Esegue scrape() → DataFrame → salva CSV. Entry point principale."""
        self.logger.info(f"=== START {self.__class__.__name__} ===")
        try:
            records = self.scrape()
            df = self.to_dataframe(records)
            ts = datetime.now().strftime("%Y%m%d")
            self.save_raw_json(records, f"{self.fonte}_raw_{ts}.json")
            if not df.empty:
                self.save_csv(df, f"{self.fonte}_{ts}.csv")
            self.logger.info(f"=== DONE {self.__class__.__name__}: {len(df)} records ===")
            return df
        except Exception as exc:
            self.logger.exception(f"=== FAIL {self.__class__.__name__}: {exc} ===")
            return pd.DataFrame(columns=SCRAPER_COLUMNS)
