"""
Core module for MIDA - Missioni Internazionali e Dati Analitici
"""

__version__ = "2.0.0"

from core.models import Mission, SourceConfig, PipelineResult
from core.normalizer import (
    normalize_mission_name,
    normalize_organization,
    normalize_region,
    normalize_commitment,
)
from core.aggregator import ExcelAggregator