"""
Scrapers per la raccolta dati da fonti istituzionali."""

from .base_scraper import BaseScraper
from .camera_scraper import CameraScraper
from .difesa_scraper import DifesaScraper
from .eeas_scraper import EEASScraper
from .esteri_scraper import EsteriScraper
from .nato_scraper import NATOScraper
from .senato_scraper import SenatoScraper
from .un_scraper import UNScraper

ALL_SCRAPERS = [
    DifesaScraper,
    CameraScraper,
    SenatoScraper,
    EsteriScraper,
    EEASScraper,
    NATOScraper,
    UNScraper,
]

__all__ = [
    "BaseScraper",
    "DifesaScraper",
    "CameraScraper",
    "SenatoScraper",
    "EsteriScraper",
    "EEASScraper",
    "NATOScraper",
    "UNScraper",
    "ALL_SCRAPERS",
]
