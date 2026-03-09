"""
Core module for MIDA - Missioni Internazionali e Dati Analitici
"""

__version__ = "2.0.0"

from core.aggregator import ExcelAggregator
from core.models import Mission, PipelineResult, SourceConfig
from core.normalizer import (
    normalize_commitment,
    normalize_mission_name,
    normalize_organization,
    normalize_region,
)
