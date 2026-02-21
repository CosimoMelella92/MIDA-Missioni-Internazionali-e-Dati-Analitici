<p align="center">
  <a href="https://www.difesa.it/operazionimilitari/">
    <img src="docs/images/banner_sponsor.png" alt="Finanziato dall'Unione Europea – NextGenerationEU, MUR, Italia Domani PNRR" width="700"/>
  </a>
</p>
<p align="center">
  <em>Progetto finanziato dall'Unione Europea – NextGenerationEU, Ministero dell'Università e della Ricerca, Italia Domani – PNRR</em>
</p>

<p align="center">
  <img src="docs/images/logo_unict.jpg" alt="Università di Catania" width="180"/>
  <br/>
  <b>Progetto sviluppato presso l'Università di Catania</b>
</p>

<br/>

<div align="center">
  <h1>MIDA — Missioni Internazionali e Dati Analitici</h1>
  <p><b>Piattaforma di analisi delle missioni internazionali italiane (1948-2026)</b></p>

  ![Python](https://img.shields.io/badge/Python-3.11+-3D4F1E?style=flat-square&logo=python&logoColor=white)
  ![React](https://img.shields.io/badge/React-18_Frontend-61DAFB?style=flat-square&logo=react&logoColor=white)
  ![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-1B3A5C?style=flat-square&logo=streamlit&logoColor=white)
  ![Tests](https://img.shields.io/badge/Tests-188%20passing-6B8C2A?style=flat-square)
  ![Missions](https://img.shields.io/badge/Missioni-234%20totali-8B1A1A?style=flat-square)
  ![Active](https://img.shields.io/badge/Attive-40%20(2026)-4A5D23?style=flat-square)
</div>

---

## Autore

**Cosimo Melella** — Università di Catania

## Panoramica

MIDA aggrega dati da **5 fonti** (CSV + 4 file Excel), producendo un dataset unificato di **234 missioni** dopo deduplicazione automatica, correzione dati vs fonti ufficiali ([difesa.it](https://www.difesa.it/operazionimilitari/)) e validazione Pydantic.

| Metrica | Valore |
|---------|--------|
| Fonti aggregate | 5 (384 righe raw) |
| Missioni dopo pipeline | **234** |
| Missioni attive (2026) | **40** (verificate vs difesa.it + analisidifesa.it) |
| Personale totale | ~8.800 unità |
| Paesi coinvolti | 75 |
| Missioni con data | **234/234** (0 mancanti) |
| Errori validazione | 0 |
| Test | 188 passing |

## Avvio Rapido

```bash
# Installa dipendenze Python
pip install -r requirements.txt

# Rigenera il dataset
python -m core.aggregator

# Esporta dati JSON per il frontend React
python export_frontend_data.py

# Avvia la dashboard Streamlit
streamlit run dashboard/app.py

# Avvia il frontend React
cd frontend && npm install && npm run dev

# Esegui i test
python -m pytest tests/ -v
```

- Dashboard Streamlit: **http://localhost:8501**
- Frontend React: **http://localhost:5173**

## Struttura del Progetto

```
MIDA/
├── .github/workflows/       # CI/CD + scraping automatico settimanale
├── .streamlit/config.toml   # Tema militare italiano
├── config/
│   └── sources.yaml         # Registry fonti dati con mapping colonne
├── core/
│   ├── models.py            # Modelli Pydantic (Mission, SourceConfig, PipelineResult)
│   ├── normalizer.py        # Normalizzazioni (nomi, org, regioni, commitment)
│   ├── aggregator.py        # Pipeline: load → normalize → dedup → enrich → validate → save
│   ├── scrapers/            # Web scrapers (difesa, camera, senato, NATO, ONU, UE, esteri)
│   └── pdf_extractor/       # Estrazione documenti PDF/DOCX
├── dashboard/               # Dashboard Streamlit (Python)
│   ├── app.py               # Entry point + tema CSS militare
│   ├── data_loader.py       # Caricamento dati con cache Streamlit
│   ├── filters.py, charts.py, analysis.py
│   ├── pdf_report.py        # Generatore report PDF (fpdf2)
│   ├── views/               # overview, missions, timeline, maps_page
│   └── maps/                # Componenti Folium (5 mappe)
├── frontend/                # Frontend React (TypeScript)
│   ├── src/
│   │   ├── App.tsx              # Router + Layout
│   │   ├── pages/               # Home, Dashboard, Missions, Timeline, Map
│   │   ├── components/          # KpiCard, OrgDonut, RegionBar, Navbar, Footer
│   │   ├── hooks/               # useMissions, useAnimatedCounter
│   │   └── lib/                 # types, constants, utils
│   └── public/data/             # missions.json, stats.json (generati da pipeline)
├── data/
│   ├── raw/                 # Fonti Excel/CSV originali
│   └── processed/           # missioni_complete.csv (output pipeline)
├── tests/                   # 188 test (normalizer, models, aggregator, scrapers, E2E)
├── export_frontend_data.py  # CSV → JSON per frontend React
├── docs/images/             # Banner istituzionali
├── requirements.txt
└── README.md
```

## Pipeline Dati (v3.7)

```mermaid
graph LR
    A[5 fonti<br/>384 righe] --> B[Load]
    B --> C[Normalize<br/>312 righe]
    C --> D[Dedup<br/>273 righe]
    D --> E[Enrich + Correzioni<br/>234 righe]
    E --> F[Validate<br/>Pydantic 0 errori]
    F --> G[Save<br/>18 colonne]
    G --> H[Dashboard<br/>Streamlit]
    G --> I[Frontend<br/>React]
```

### Step della pipeline

1. **Load** — Carica fonti da `config/sources.yaml`, applica mapping colonne
2. **Normalize** — Nomi missione, organizzazioni (ONU/NATO/UE), regioni (6 macro), commitment
3. **Dedup** — Chiave = nome normalizzato strict. Vince la fonte con più dati
4. **Enrich** — Correzioni ufficiali difesa.it (nomi, paesi, personale, costi), cross-reference 40 missioni attive, iniezione missioni mancanti
5. **Validate** — Ogni record passa per il modello Pydantic `Mission`
6. **Save** — 18 colonne canoniche in `data/processed/missioni_complete.csv`

### Colonne del dataset

| Colonna | Tipo | Descrizione |
|---------|------|-------------|
| `nome` | str | Nome univoco missione |
| `paese` | str | Paese (normalizzato IT) |
| `regione` | str | Africa, Europa, Medio Oriente, Asia, America |
| `tipo_missione` | str | ONU, NATO, UE, ITA, Bilateral, Multinational, Coalizione |
| `commitment` | str | Troops, Head of Mission, Advisory/Training, ecc. |
| `data_inizio` / `data_fine` | date | Periodo missione |
| `personale_militare` / `civile` / `totale` | float | Personale impiegato |
| `costo_totale` | float | Costo in euro (quota italiana) |
| `is_active` | bool | Attiva nel 2026 (verificata vs difesa.it) |

## Frontend React

SPA moderna costruita con **React 18 + TypeScript + Vite + Tailwind CSS**.

### Pagine

| Pagina | Descrizione |
|--------|-------------|
| **Home** | Hero, 5 KPI animati, lista missioni attive, grafici donut/barre/decennio |
| **Dashboard** | Grafici Recharts interattivi con filtri per org/regione/stato |
| **Missioni** | Tabella sortabile con ricerca, filtri, export CSV |
| **Timeline** | Barre orizzontali colorate per durata (1948-2026), tooltip hover |
| **Mappa** | Leaflet full-screen, marker missioni attive, linee Roma → missioni |

### Stack Frontend

| Tecnologia | Uso |
|------------|-----|
| React 18 + TypeScript | Framework UI |
| Vite 5 | Build tool |
| Tailwind CSS 3 | Styling utility-first |
| Recharts | Grafici (donut, barre, treemap) |
| Leaflet | Mappe interattive (no API key) |
| Framer Motion | Animazioni KPI |
| Lucide React | Icone |

### Architettura dati

```
Pipeline Python → missioni_complete.csv → export_frontend_data.py → JSON statici
                                                                    ├─ missions.json (234 missioni)
                                                                    ├─ active.json (40 attive)
                                                                    └─ stats.json (KPI pre-calcolati)
```

Nessun backend necessario — i dati sono pre-processati e serviti come JSON statici.

## Dashboard Streamlit

### Pagine

- **Panoramica** — 8 KPI card, grafici per periodo/organizzazione/regione, distribuzione personale e costi
- **Missioni e Dati** — Tabella interattiva completa, export CSV filtrato
- **Timeline** — Timeline per organizzazione, regione, durata; scatter interattivo con slider temporale
- **Mappe** — 5 tab: Missioni Attive 2026, Mappa Mondo, Calore, Timeline Geografica, Cluster

### Tema visivo

Palette militare italiana: verde oliva, blu marina, sabbia, rosso esercito, grigio acciaio. **Dark mode** attivabile dal toggle nella sidebar.

### Export

- **CSV** — Dataset filtrato
- **Excel** — Multi-sheet (Missioni, Organizzazioni, Commitment, Regioni)
- **PDF** — Report sintetico con KPI, distribuzione org/regione, missioni attive, top 10 personale

## Test

```bash
# Tutti i test
python -m pytest tests/ -v

# Solo normalizer (51 test)
python -m pytest tests/test_normalizer.py -v

# Solo modelli (21 test)
python -m pytest tests/test_models.py -v

# Solo pipeline (12 test)
python -m pytest tests/test_aggregator.py -v
```

## Tecnologie

| Categoria | Tecnologie |
|-----------|------------|
| **Core** | Python 3.11+, Pandas, NumPy, Pydantic |
| **Frontend** | React 18, TypeScript, Vite 5, Tailwind CSS, Recharts, Leaflet, Framer Motion |
| **Dashboard** | Streamlit, Plotly, Folium |
| **Test** | pytest (188 test) |
| **CI/CD** | GitHub Actions (lint + test + scraping settimanale) |
| **Scraping** | requests, BeautifulSoup, lxml |
| **Documenti** | PyMuPDF, python-docx |

## Fonti Dati

| Fonte | Tipo | Contenuto |
|-------|------|----------|
| [difesa.it](https://www.difesa.it/operazionimilitari/) | Scraper + manuale | Missioni in corso, personale, costi |
| [analisidifesa.it](https://www.analisidifesa.it/) | Manuale | Analisi annuali missioni, dati Delibera CdM |
| [Camera dei Deputati](https://www.camera.it/) | Scraper | Delibere missioni internazionali |
| [Senato](https://www.senato.it/) | Scraper | Documenti parlamentari |
| [EEAS (UE)](https://www.eeas.europa.eu/) | Scraper | Missioni CSDP civili e militari |
| [NATO](https://www.nato.int/) | Scraper | Operazioni e missioni NATO |
| [ONU](https://peacekeeping.un.org/) | Scraper | Peacekeeping operations |

## Scraping Automatico

GitHub Actions esegue ogni lunedì alle 06:00 UTC:
1. Scraping difesa.it per aggiornamenti missioni
2. Rigenerazione dataset via pipeline
3. Esecuzione test suite
4. Auto-commit se il dataset è cambiato

Configurazione: `.github/workflows/scrape.yml`

## Come aggiungere dati

1. Aggiungi il file Excel/CSV in `data/raw/Excel/`
2. Registra la fonte in `config/sources.yaml` con il mapping colonne
3. Esegui `python -m core.aggregator` — la pipeline normalizza, deduplica e valida automaticamente
4. Esegui `python export_frontend_data.py` per aggiornare i JSON del frontend
5. Avvia `streamlit run dashboard/app.py` o `cd frontend && npm run dev` per visualizzare

## Contribuire

1. Fork del repository
2. `git checkout -b feature/nome-feature`
3. `git commit -am 'Aggiunta feature'`
4. `git push origin feature/nome-feature`
5. Pull Request

## Licenza

Distribuito con licenza MIT.

---

<div align="center">
  <sub>MIDA — Missioni Internazionali e Dati Analitici &middot; Università di Catania &middot; Dati: <a href="https://www.difesa.it/operazionimilitari/">Ministero della Difesa</a></sub>
</div>