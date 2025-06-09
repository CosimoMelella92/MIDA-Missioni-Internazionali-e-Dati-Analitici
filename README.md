# MIDA - Missioni Internazionali e Dati Analitici

## 📊 Panoramica del Progetto

Questo progetto automatizza la raccolta, l'analisi e la classificazione dei dati relativi alle missioni internazionali italiane da diverse fonti istituzionali.

### 🎯 Obiettivi

1. **Raccolta Dati**
   - Estrarre automaticamente dati da fonti ufficiali
   - Standardizzare i dati in un formato unificato
   - Mantenere uno storico delle missioni

2. **Classificazione**
   - Categorizzare le missioni per tipo (UE, NATO, ONU, etc.)
   - Identificare missioni ibride/multilaterali
   - Tracciare evoluzione temporale delle missioni

3. **Analisi**
   - Generare report automatici
   - Visualizzare trend e pattern
   - Supportare decisioni strategiche

### 📈 Schema di Classificazione

#### 1. 🇪🇺 Missioni UE (CSDP/PESD)
- **Civili (CP) CSDP**
  - EUPOL, EUBAM, EULEX
  - Focus: Formazione polizia, stato di diritto
- **Militari (MP) CSDP**
  - EUTM Mali, EUNAVFOR MED
  - Focus: Training eserciti, anti-pirateria

#### 2. 🌐 Missioni NATO
- **Peacekeeping & Deterrence**
  - KFOR Kosovo, NATO Air Policing
- **Training & Support**
  - NATO Iraq Mission, Resolute Support
- **Security Operations**
  - Enhanced Forward Presence, Sea Guardian

#### 3. 🕊️ Missioni ONU
- **Peacekeeping tradizionale**
  - UNIFIL (Libano)
- **Osservazione/monitoraggio**
  - MINURSO, UNSMIS

#### 4. 🇮🇹 Missioni bilaterali/nazionali
- **Bilateral ITA-led**
  - MISIN Niger, Libia
- **Supporto umanitario/sanitario**
  - Missione in Mozambico
- **Antiterrorismo & sicurezza marittima**
  - Golfo di Guinea

#### 5. ⚙️ Missioni multi-framework/ibride
- **UE + NATO**
  - Bosnia (ALTHEA + NATO HQ)
- **ONU + ITA**
  - UNIFIL + comando sett. ovest ITA
- **NATO + USA-led**
  - Iraq (post-ISAF)

## Struttura del Progetto

```
MIDA/
├── config/
│   ├── config.yaml           # Configurazione principale
│   └── scheduler_config.yaml # Configurazione scheduler
├── data/
│   └── raw/                  # Dati grezzi
├── logs/                     # Log del sistema
├── reports/                  # Report generati
├── scripts/
│   ├── automation/           # Sistema di automazione
│   │   ├── scheduler.py      # Scheduler principale
│   │   ├── install_service.py # Installazione servizio Windows
│   │   └── README.md         # Documentazione automazione
│   ├── dashboard/            # Dashboard interattiva
│   │   └── app.py           # Applicazione Streamlit
│   ├── reports/             # Generazione report
│   │   ├── report_generator.py
│   │   └── templates/
│   │       └── report_template.html
│   ├── scrapers/            # Moduli di scraping
│   │   ├── base_scraper.py
│   │   ├── maec_scraper.py
│   │   └── un_scraper.py
│   ├── utils/               # Utility
│   │   ├── data_validator.py
│   │   └── logger.py
│   └── main.py              # Script principale
└── README.md                # Questo file
```

## Requisiti

```bash
pip install -r requirements.txt
```

## Configurazione

1. Copia il file di configurazione di esempio:
```bash
cp config/config.yaml.example config/config.yaml
```

2. Modifica `config/config.yaml` con le tue impostazioni:
   - Percorsi delle directory
   - Configurazione dei proxy
   - Impostazioni di logging
   - Configurazione email per le notifiche

## Funzionalità

### 1. Scraping dei Dati
- Supporto per MAEC e UN
- Validazione dei dati
- Gestione degli errori
- Logging dettagliato

### 2. Dashboard Interattiva
- Visualizzazione dati in tempo reale
- Filtri per data, tipo missione, paese
- Grafici interattivi
- Statistiche generali

### 3. Generazione Report
- Report HTML e PDF
- Grafici e statistiche
- Dettagli missioni
- Personalizzazione template

### 4. Automazione
- Scheduler per attività periodiche
- Backup automatico
- Pulizia dati
- Notifiche email

## Utilizzo

### Dashboard
```bash
streamlit run scripts/dashboard/app.py
```

### Generazione Report
```bash
python scripts/reports/report_generator.py
```

### Automazione
1. Configura `config/scheduler_config.yaml`
2. Installa il servizio Windows:
```powershell
python scripts/automation/install_service.py install
python scripts/automation/install_service.py start
```

## Attività Programmate

- **Scraping**: Ogni giorno alle 2:00
- **Backup**: Ogni domenica alle 3:00
- **Pulizia**: Ogni lunedì alle 4:00

## Log

I log sono salvati in:
- `logs/scraper_YYYYMMDD.log`: Log dello scraping
- `logs/scheduler_YYYYMMDD.log`: Log dello scheduler
- `logs/service.log`: Log del servizio Windows

## Risoluzione dei Problemi

### Scraping
1. Verifica la connessione internet
2. Controlla i log in `logs/scraper_*.log`
3. Verifica la configurazione dei proxy

### Dashboard
1. Assicurati che Streamlit sia installato
2. Verifica che i dati siano presenti in `data/`
3. Controlla i permessi delle directory

### Report
1. Verifica che wkhtmltopdf sia installato
2. Controlla i template in `scripts/reports/templates/`
3. Verifica i permessi di scrittura

### Automazione
1. Controlla i log del servizio
2. Verifica la configurazione email
3. Assicurati che il servizio sia in esecuzione

## Contribuire

1. Fork del repository
2. Crea un branch per la feature
3. Commit delle modifiche
4. Push al branch
5. Crea una Pull Request

## 👥 Autore

- **Cosimo Melella**


## Licenza

Questo progetto è distribuito con licenza MIT. Vedi il file `LICENSE` per maggiori dettagli. 