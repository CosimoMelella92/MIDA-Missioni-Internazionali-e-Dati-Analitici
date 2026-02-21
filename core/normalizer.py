"""
Modulo unificato di normalizzazione per MIDA.
Contiene tutte le funzioni pure di normalizzazione per nomi, organizzazioni,
regioni, commitment e colonne. Sostituisce la logica duplicata in:
- core/processors/normalize_organizations.py
- core/processors/normalize_commitments.py
- dashboard/missioni_dashboard.py (normalize_organization, normalize_regions, normalize_excel_columns)
- clean_duplicates_and_verify.py (normalize_mission_name)
- fix_mission_count.py (normalize_mission_name)
"""

import re
from typing import Optional


# =============================================================================
# NORMALIZZAZIONE NOMI MISSIONE
# =============================================================================

def normalize_mission_name(name: str) -> str:
    """
    Normalizza il nome di una missione per confronti e deduplicazione.
    Converte in minuscolo, rimuove caratteri speciali, spazi multipli.
    """
    if not name or not isinstance(name, str):
        return ""
    name = str(name).strip().lower()
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'[\s_-]+', ' ', name)
    return name.strip()


def normalize_mission_name_strict(name: str) -> str:
    """
    Normalizzazione stretta: rimuove anche spazi e trattini.
    Usata per deduplicazione aggressiva.
    """
    normalized = normalize_mission_name(name)
    return re.sub(r'[\s\-_]', '', normalized)


def extract_mission_acronym(name: str) -> str:
    """
    Estrae l'acronimo da nomi lunghi come:
    'EU Training Mission Mali (EUTM Mali)' -> 'eutmmali'
    'EU Naval Force Somalia ATALANTA (EU- NAVFOR Somalia)' -> 'eunavforsomalia'
    Se non c'è parentesi, restituisce il nome normalizzato strict.
    """
    if not name or not isinstance(name, str):
        return ""
    # Cerca contenuto tra parentesi
    match = re.search(r'\(([^)]+)\)', name)
    if match:
        acronym = match.group(1).strip()
        # Normalizza "EU- NAVFOR" -> "EU NAVFOR" (rimuovi solo trattino dopo EU)
        acronym = re.sub(r'^EU-\s*', 'EU', acronym, flags=re.IGNORECASE)
        return normalize_mission_name_strict(acronym)
    return normalize_mission_name_strict(name)


# =============================================================================
# NORMALIZZAZIONE ORGANIZZAZIONI
# =============================================================================

_ONU_PATTERNS = [
    r'^unifil',        # UNIFIL
    r'^unmiss',        # UNMISS
    r'^unprofor',      # UNPROFOR
    r'^unmik',         # UNMIK
    r'^unmil',         # UNMIL
    r'^unoci',         # UNOCI
    r'^unficyp',       # UNFICYP
    r'^unisfa',        # UNISFA
    r'^unsmil',        # UNSMIL
    r'^unscom',        # UNSCOM
    r'^untaet',        # UNTAET
    r'^untmih',        # UNTMIH
    r'^unmogip',       # UNMOGIP
    r'^untso',         # UNTSO
    r'^unama',         # UNAMA
    r'^unamid',        # UNAMID
    r'^unami\b',      # UNAMI
    r'^minu',          # MINURSO, MINUSTAH, etc.
    r'\bpeacekeeping\b',
    r'\bpeace\s*keeping\b',
    r'united\s*nations',
    r'nazioni\s*unite',
]

_NATO_PATTERNS = [
    r'\bnato\b',
    r'\bkfor\b',
    r'\bisaf\b',
    r'resolute\s*support',
    r'sea\s*guardian',
    r'north\s*atlantic',
]

_UE_PATTERNS = [
    r'\beu[a-z]{2,}',  # EUTM, EUCAP, EUNAVFOR, EULEX, EUBAM, EUAM, EUMM
    r'\bcsdp\b',
    r'\bpesd\b',
    r'european\s*union',
    r'unione\s*europea',
]

_FRAMEWORK_MAP = {
    'onu': 'ONU', 'un': 'ONU', 'united nations': 'ONU', 'nazioni unite': 'ONU',
    'nato': 'NATO', 'north atlantic treaty organization': 'NATO',
    'ue': 'UE', 'eu': 'UE', 'european union': 'UE', 'unione europea': 'UE',
    'ita': 'ITA', 'italia': 'ITA', 'italian': 'ITA', 'italy': 'ITA',
    'coalizione': 'Coalizione', 'coalition': 'Coalizione',
    'multinational': 'Multinational', 'multinazionale': 'Multinational',
    'bilaterale': 'Bilateral', 'bilateral': 'Bilateral',
}


def normalize_organization(mission_name: str, framework: str = "") -> str:
    """
    Normalizza l'organizzazione di una missione basandosi su nome e framework.
    Restituisce una delle categorie canoniche: ONU, NATO, UE, ITA, Multinational, Bilateral, Coalizione, Altro.
    """
    name_lower = str(mission_name).lower().strip()
    fw_lower = str(framework).lower().strip()

    # 1. Controlla pattern ONU nel nome
    for pattern in _ONU_PATTERNS:
        if re.search(pattern, name_lower):
            return 'ONU'

    # 2. Controlla pattern NATO nel nome
    for pattern in _NATO_PATTERNS:
        if re.search(pattern, name_lower):
            return 'NATO'

    # 3. Controlla pattern UE nel nome
    for pattern in _UE_PATTERNS:
        if re.search(pattern, name_lower):
            return 'UE'

    # 4. Controlla framework diretto
    if fw_lower in _FRAMEWORK_MAP:
        return _FRAMEWORK_MAP[fw_lower]

    # 5. Controlla pattern nel framework
    for pattern in _ONU_PATTERNS:
        if re.search(pattern, fw_lower):
            return 'ONU'
    for pattern in _NATO_PATTERNS:
        if re.search(pattern, fw_lower):
            return 'NATO'
    for pattern in _UE_PATTERNS:
        if re.search(pattern, fw_lower):
            return 'UE'

    # 6. Ignora valori che non sono organizzazioni (es. CIVILIAN, MILITARY, codici vXXX)
    _NON_ORG_VALUES = {
        'civilian', 'military', 'civilian-military', 'civmil', 'mil', 'civ',
    }
    if fw_lower in _NON_ORG_VALUES or re.match(r'^v\d+$', fw_lower):
        return 'Altro'

    # 7. Framework non vuoto e non 'nan' → restituiscilo capitalizzato
    if fw_lower and fw_lower != 'nan' and fw_lower != 'none':
        return _FRAMEWORK_MAP.get(fw_lower, framework.strip())

    return 'Altro'


# =============================================================================
# NORMALIZZAZIONE REGIONI
# =============================================================================

_REGION_MAP = {
    # America
    'americas': 'America', 'america': 'America',
    'north america': 'America', 'south america': 'America',
    'central america': 'America', 'latin america': 'America',
    'caribbean': 'America', 'caraibi': 'America',
    'america centrale': 'America', 'america meridionale': 'America',
    'america settentrionale': 'America', 'america latina': 'America',

    # Medio Oriente
    'middle east': 'Medio Oriente', 'medio oriente': 'Medio Oriente',
    'near east': 'Medio Oriente', 'vicino oriente': 'Medio Oriente',
    'levant': 'Medio Oriente', 'golfo persico': 'Medio Oriente',

    # Europa
    'europe': 'Europa', 'europa': 'Europa',
    'european union': 'Europa', 'unione europea': 'Europa',
    'europa occidentale': 'Europa', 'europa orientale': 'Europa',
    'europa centrale': 'Europa', 'balcani': 'Europa',
    'balkans': 'Europa', 'rest of europe': 'Europa',
    'atlantico': 'Europa', 'eurasia': 'Europa',
    'caucaso': 'Europa', 'caucasus': 'Europa',

    # Medio Oriente / Mediterraneo
    'mediterraneo': 'Medio Oriente', 'mediterranean': 'Medio Oriente',
    'northern africa and meditterranean': 'Medio Oriente',
    'northern africa and mediterranean': 'Medio Oriente',
    'nord africa e mediterraneo': 'Medio Oriente',

    # Asia
    'asia': 'Asia', 'asia centrale': 'Asia',
    'asia sudorientale': 'Asia', 'asia meridionale': 'Asia',
    'asia orientale': 'Asia', 'far east': 'Asia',
    'estremo oriente': 'Asia', 'oceania': 'Asia',

    # Africa
    'africa': 'Africa', 'sub-saharan africa': 'Africa',
    'africa sub-sahariana': 'Africa', 'northern africa': 'Africa',
    'nord africa': 'Africa', 'western africa': 'Africa',
    'africa occidentale': 'Africa', 'eastern africa': 'Africa',
    'africa orientale': 'Africa', 'central africa': 'Africa',
    'africa centrale': 'Africa', 'southern africa': 'Africa',
    'africa meridionale': 'Africa', 'horn of africa': 'Africa',
    "corno d'africa": 'Africa', 'africa australe': 'Africa',
    'nord africa e mediterraneo': 'Africa',
}


def normalize_region(region: str) -> str:
    """Normalizza una regione alla macro-regione canonica."""
    if not region or not isinstance(region, str):
        return "Non specificata"
    key = region.strip().lower()
    if key in ("nan", "none", "nat", ""):
        return "Non specificata"
    # Lookup diretto
    if key in _REGION_MAP:
        return _REGION_MAP[key]
    # Pattern matching per regioni composte (es. "Africa/Asia")
    for canonical in ["Africa", "Europa", "Asia", "Medio Oriente", "America"]:
        if canonical.lower() in key:
            return canonical
    # Codici sconosciuti (es. "v064") → Non specificata
    if re.match(r'^v\d+$', key):
        return "Non specificata"
    return region.strip()


# =============================================================================
# NORMALIZZAZIONE COMMITMENT
# =============================================================================

def normalize_commitment(commitment: str, mission_name: str = "") -> str:
    """
    Normalizza il tipo di commitment di una missione.
    Tiene conto anche del nome missione per override specifici.
    """
    if not commitment or not isinstance(commitment, str):
        return "Troops"

    c = str(commitment).strip().lower()
    if c in ("nan", "none", "nat", ""):
        return "Troops"
    n = str(mission_name).strip().lower()

    # Override specifici per missione
    if 'unifil' in n:
        return 'Head of Mission'

    # Pattern matching
    if 'head of mission' in c:
        return 'Head of Mission'
    if re.search(r'advisory.*training|training.*advisory', c):
        return 'Advisory/Training'
    if re.search(r'logistical.*support|support.*logistical', c):
        return 'Logistical Support & Advisory'

    # Troops variants
    if 'naval' in c or 'eunavfor' in n or 'irini' in n:
        return 'Troops (naval)'
    if 'air' in c and 'air defense' not in c:
        return 'Troops (air)'
    if re.search(r'troops\s*\(ground\s*forces\)', c) or 'ground' in c:
        return 'Troops (ground forces)'
    if 'isaf' in n or 'kfor' in n or 'inherent resolve' in n:
        return 'Troops (ground forces)'
    if 'logistical' in c or 'support' in c:
        return 'Troops (logistical support)'
    if 'eumm' in n or 'mfo' in n or 'unmogip' in n or 'unficyp' in n:
        return 'Troops (logistical support)'
    if 'unama' in n or 'euam' in n or 'eulex' in n or 'unami' in n:
        return 'Troops (logistical support)'
    if 'troops' in c:
        return 'Troops'

    return commitment.strip()


# =============================================================================
# NORMALIZZAZIONE COLONNE EXCEL
# =============================================================================

# Mapping completo di tutte le varianti di colonne → schema canonico
COLUMN_MAPPING = {
    # Nome missione
    'mission': 'nome', 'nome_missione': 'nome', 'missione': 'nome',
    'nome missione': 'nome', 'mission name': 'nome', 'mission_name': 'nome',
    'nome': 'nome',

    # Paese
    'country': 'paese', 'paese': 'paese', 'stato': 'paese',

    # Regione
    'region': 'regione', 'regione': 'regione', 'area_geografica': 'regione',
    'area geografica': 'regione',

    # Sub-regione
    'sub_regione': 'sub_regione', 'sub regione': 'sub_regione',
    'area_specifica': 'sub_regione', 'area specifica': 'sub_regione',

    # Tipo partecipazione
    'tipo_partecipazione': 'tipo_partecipazione',
    'tipo partecipazione': 'tipo_partecipazione',
    'partecipazione': 'tipo_partecipazione',

    # Date
    'date_start': 'data_inizio', 'data_inizio': 'data_inizio',
    'data inizio': 'data_inizio', 'inizio': 'data_inizio',
    'starting year': 'data_inizio', 'start_date': 'data_inizio',

    'date_end': 'data_fine', 'data_fine': 'data_fine',
    'data fine': 'data_fine', 'fine': 'data_fine',
    'end/extension year': 'data_fine', 'end_date': 'data_fine',

    # Personale
    'personale_militare': 'personale_militare', 'personale militare': 'personale_militare',
    'militari': 'personale_militare',
    'personale_civile': 'personale_civile', 'personale civile': 'personale_civile',
    'civili': 'personale_civile',
    'personale_totale': 'personale_totale', 'personale totale': 'personale_totale',
    'totale_personale': 'personale_totale', 'personale': 'personale_totale',
    'absolute recorded maximum personnel': 'personale_totale',

    # Costo
    'costo_totale': 'costo_totale', 'costo totale': 'costo_totale',
    'costo': 'costo_totale', 'costo_€': 'costo_totale',

    # Tipo missione / organizzazione
    'tipo_missione': 'tipo_missione', 'tipo missione': 'tipo_missione',
    'organizzazione': 'tipo_missione', 'framework': 'tipo_missione',
    'type': 'tipo_missione',

    # Commitment
    'commitment': 'commitment', 'tipo_commitment': 'commitment',
    'tipo commitment': 'commitment',

    # Booleani
    'ended mission': 'ended_mission',
    'coop_un': 'coop_un', 'coop_nato': 'coop_nato',
    'coop_asean': 'coop_asean', 'coop_au': 'coop_au',

    # Mandato e note
    'mandato': 'mandato', 'mandate': 'mandato',
    'note': 'note', 'notes': 'note',
    'link_documento': 'link_documento',
}


def normalize_column_name(col_name: str) -> str:
    """Normalizza un nome di colonna allo schema canonico."""
    key = str(col_name).strip().lower()
    return COLUMN_MAPPING.get(key, col_name)


def normalize_columns(columns: list[str]) -> dict[str, str]:
    """
    Dato un elenco di nomi colonna, restituisce un dict di rinomina
    {nome_originale: nome_canonico} per le colonne riconosciute.
    """
    mapping = {}
    for col in columns:
        canonical = normalize_column_name(col)
        if canonical != col:
            mapping[col] = canonical
    return mapping


# =============================================================================
# NORMALIZZAZIONE PAESE
# =============================================================================

def normalize_country(country: str) -> str:
    """Normalizza il nome di un paese."""
    if not country or not isinstance(country, str):
        return "Non specificato"
    return country.strip()


# =============================================================================
# CLASSIFICAZIONE PERIODO STORICO
# =============================================================================

_PERIODS = [
    (1948, 1990, "1948-1990"),
    (1991, 2001, "1991-2001"),
    (2002, 2015, "2002-2015"),
    (2016, 2100, "2016-oggi"),
]


def classify_period(year: Optional[int]) -> str:
    """Classifica un anno nel periodo storico corrispondente."""
    if year is None:
        return "Non specificato"
    for start, end, label in _PERIODS:
        if start <= year <= end:
            return label
    if year < 1948:
        return "Pre-1948"
    return "2016-oggi"
