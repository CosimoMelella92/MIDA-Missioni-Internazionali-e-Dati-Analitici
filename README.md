![Sponsorship Banner](docs/images/banner_sponsor.png)

<div align="center">
  <b>Progetto finanziato dall'Unione Europea – NextGenerationEU, Ministero dell'Università e della Ricerca, Italia Domani – PNRR</b>
</div>

# MIDA - Missioni Internazionali e Dati Analitici

## 👨‍💻 Autore
**Cosimo Melella**

## 📊 Panoramica
MIDA è una piattaforma avanzata per l'analisi e la visualizzazione delle missioni internazionali italiane. Il sistema è progettato per accogliere dati da fonti eterogenee (Excel, CSV, PDF, dati parlamentari) e strutturarli in modo robusto e coerente, garantendo qualità, deduplicazione automatica e analisi interattiva tramite dashboard.

Attualmente il sistema gestisce **208 missioni uniche** dal **1948 al 2027**, coprendo un arco temporale di **79 anni** di impegno internazionale dell'Italia, dalla Guerra Fredda ai giorni nostri.

## 🏗️ Struttura del Progetto
```
MIDA/
├── config/
│   └── config.yaml           # Configurazione del sistema
├── data/
│   ├── raw/                  # Dati grezzi
│   │   ├── Excel/           # File Excel originali
│   │   └── PDF/             # Documenti PDF originali
│   ├── documents/           # Documenti PDF processati
│   └── processed/           # Dati elaborati
│       └── missioni_complete.csv  # Dataset completo aggiornato
├── src/
│   ├── main.py              # Script principale
│   ├── document_processor.py # Elaborazione documenti
│   ├── data_processor.py    # Elaborazione dati
│   ├── missioni_dashboard.py # Dashboard Streamlit principale
│   ├── map_utils.py         # Funzioni per le mappe interattive
│   └── run_dashboard.py     # Script di avvio dashboard
├── docs/
│   ├── images/              # Screenshot e immagini
│   └── missioni_analizzate.md # Documentazione missioni
└── requirements.txt         # Dipendenze Python
```

## 🔄 Flusso dei Dati
```mermaid
graph LR
    A[Documenti PDF] --> B[Document Processor]
    C[File Excel] --> D[Data Processor]
    E[Fonti parlamentari 2025] --> D
    B --> F[Data Enrichment]
    D --> F
    F --> G[Dashboard Interattiva]
    G --> H[Mappe Avanzate]
    G --> I[Analisi Organizzazioni]
```

## 📁 Struttura dei Dati

### File Principale (`missioni_complete.csv`)
Il dataset strutturato contiene queste colonne obbligatorie:
- **nome**: Nome univoco della missione
- **paese**: Paese di destinazione
- **regione**: Regione geografica
- **sub_regione**: Sub-regione specifica
- **tipo_partecipazione**: (mil, civ, civmil)
- **data_inizio**: Data di inizio (YYYY-MM-DD)
- **data_fine**: Data di fine (YYYY-MM-DD)
- **personale_militare**: Numero personale militare
- **personale_civile**: Numero personale civile
- **personale_totale**: Totale personale
- **costo_totale**: Budget complessivo
- **tipo_missione**: Organizzazione (ONU, UE, NATO, ITA, ecc.)
- **commitment**: Classificazione impegno (calcolata se mancante)

**Nota:** Il sistema accetta anche file Excel con colonne diverse, purché mappabili su queste. La pipeline normalizza e arricchisce i dati automaticamente.

### Documenti PDF
I documenti PDF vengono elaborati per estrarre:
- Testo completo
- Date rilevanti
- Informazioni sul personale
- Dettagli finanziari
- Riferimenti normativi

## 🧹 Pipeline Dati e Deduplicazione
- **Caricamento**: I dati vengono caricati da `data/processed/missioni_complete.csv` e, se presenti, da nuovi file Excel in `data/raw/Excel/` e dalle fonti parlamentari 2025.
- **Normalizzazione**: I dati vengono convertiti nel formato standard, con colonne e tipi coerenti.
- **Deduplicazione automatica**: Missioni con nomi simili (ignorando spazi, trattini, maiuscole/minuscole), stesso paese vengono mantenute una sola volta.
- **Pulizia colonne**: Vengono rimosse colonne duplicate e valori incoerenti.
- **Validazione**: Il sistema segnala missioni con dati mancanti, date incoerenti o costi anomali.

## ➕ Come aggiungere nuovi dati
1. **Aggiungi il file Excel/CSV** in `data/raw/Excel/`.
2. **Assicurati che le colonne siano mappabili** su quelle richieste (vedi sopra). Se usi nomi diversi, la pipeline li mapperà automaticamente se riconoscibili.
3. **Evita duplicati**: la pipeline li rimuove automaticamente, ma è buona pratica controllare che i nomi missione siano coerenti.
4. **Avvia la dashboard**: i dati verranno integrati e puliti automaticamente.

## 🛡️ Qualità e Controlli
- **Script di controllo duplicati**: `python test_check_duplicates.py` segnala missioni simili e anomalie sui costi.
- **Controllo qualità dati**: la dashboard segnala missioni con date o campi chiave mancanti.
- **Best practice**: usa sempre nomi missione chiari e coerenti, verifica i dati prima di aggiungerli.

## 📈 Dashboard Avanzata

### 🎯 Metriche Principali
- **📊 Missioni Totali**: Numero complessivo di missioni (**208 uniche**)
- **👥 Personale Totale**: Somma di tutto il personale impiegato
- **💰 Costo Totale**: Budget complessivo investito
- **🟢 Missioni Attive**: Missioni attualmente in corso

### 📅 Analisi per Periodi Temporali
- **1948-1990**: Guerra Fredda e prime missioni ONU
- **1991-2001**: Post Guerra Fredda
- **2001-2015**: Guerra al Terrorismo  
- **2015-Presente**: Crisi Migratoria e Stabilizzazione

### 🎯 Analisi per Tipo di Partecipazione
- **🎖️ Militare (mil)**: Operazioni di combattimento, training militare
- **👔 Civile (civ)**: Capacity building, assistenza tecnica
- **🎖️👔 Mista (civmil)**: Operazioni di pace, stabilizzazione

### 🏛️ Analisi per Organizzazione
- **🏛️ ONU:** 60 missioni  
  Esempi: UNIFIL, MINURSO, UNMISS, UNPROFOR, UNMIK, MONUSCO, UNOCI, UNAMID, MINUSTAH, UNTAET, UNFICYP, ecc.
- **🇪🇺 UE:** 51 missioni  
  Esempi: EUTM Mali, EUBAM Libya, EUFOR ALTHEA, EUTM Somalia, EUTM RCA, EUNAVFOR ATALANTA, IRINI, EUMM, EUAM Iraq, EULEX Kosovo, EUCAP Somalia, EUAM Ukraine, EUMA Armenia, EUPM Moldova, EUBAM Moldova-Ukraine, EUBAM Rafah, EUPOL COPPS, EUSDI Gulf of Guinea, EUNAVFOR Aspides, MPCC UE, CRRTs UE, ecc.
- **🏛️ NATO:** 50 missioni  
  Esempi: KFOR, ISAF, IFOR, SFOR, NATO Mission Iraq, Enhanced Vigilance Activities, Forward Land Forces, Sea Guardian, Air Policing, ecc.
- **🤝 Bilateral:** 28 missioni  
  Esempi: MIBIL, MIADIT, MIASIT, Cooperazione tecnica Angola, ecc.
- **🤝 Multinational:** 18 missioni  
  Esempi: (vedi dashboard per elenco completo)
- **🇮🇹 ITA:** 1 missione  
  Esempi: MISIN

*Distribuzione delle missioni per organizzazione internazionale*

### 🌍 Analisi per Regione e Sub-Regione
- **Balkans**: 32 missioni (KFOR, EUFOR ALTHEA, IFOR, SFOR, ecc.)
- **Sub-Saharan Africa**: 29 missioni (MISIN, EUTM Mali, MINUSMA, ecc.)
- **Africa**: 26 missioni (MONUSCO, MINURSO, MINUSCA, ecc.)
- **Middle East**: 23 missioni (UNIFIL, MIBIL, MIADIT, Operation Inherent Resolve, ecc.)
- **Rest of Europe**: 22 missioni (EUMM Georgia, MPCC UE, ecc.)
- **Europa**: 18 missioni (EULEX Kosovo, EUBAM Moldova-Ukraine, ecc.)
- **Medio Oriente**: 15 missioni (UNSMIL, EUAM Iraq, ecc.)
- **Asia**: 14 missioni (ISAF, UNAMA, UNTAET, ecc.)
- **Northern Africa and Meditterranean**: 13 missioni (EUBAM Libya, MIASIT, ecc.)
- **America**: 6 missioni (MINUSTAH, MINUJUSTH, BINUH, ecc.)
- **Americas**: 4 missioni (MIPONUH, UNTMIH, ecc.)
- **Eurasia**: 2 missioni (EUMA Armenia, ecc.)
- **Mediterraneo**: 1 missione (IRINI)
- **Africa/Asia**: 1 missione (EUNAVFOR Aspides)
- **Nord Africa**: 1 missione (Cooperazione tecnica Angola)
- **Africa Sub-sahariana**: 1 missione (Cooperazione tecnica Angola)

---

### 🆕 Note aggiornate

- **Copertura temporale:** la dashboard ora copre missioni dal 1948 al 2027, incluse tutte le missioni attive e pianificate per il 2025 secondo i dati parlamentari più recenti, con alcune missioni UE estese fino al 2027 (EUCAP Somalia, EUMA Armenia).
- **Nuove missioni 2025:** integrate e visibili nella dashboard (es. EUFOR ALTHEA, Enhanced Vigilance Activities, Forward Land Forces, Sea Guardian, Air Policing, MPCC UE, CRRTs UE, ecc.).
- **Missioni estese:** alcune missioni UE sono state estese oltre il 2025 per riflettere gli impegni a lungo termine.
- **Deduplicazione e qualità:** il dataset è stato deduplicato e validato, senza anomalie o doppioni.

## 🗺️ Mappe Interattive Avanzate

### 🌍 Mappa del Mondo
![Mappa del Mondo](docs/images/mappa_mondo.png)
- **Colori per organizzazione**: 🔵 ONU, 🟠 UE, 🟢 NATO, 🔴 ITA, 🟡 Bilateral, 🟣 Multinational
- **Marker intelligenti**: Dimensioni basate sul personale
- **Hover ricchi**: Tutti i dettagli della missione con emoji
- **Legenda integrata**: Visibile e ben posizionata

### 🔥 Mappa di Calore
- **Densità personale**: Visualizzazione della concentrazione di personale
- **Scala colori**: Blu (basso) → Rosso (alto)
- **Radius ottimizzato**: 40px per migliore visualizzazione
- **Organizzazioni**: Tutte le 6 organizzazioni rappresentate (ONU, UE, NATO, ITA, Bilateral, Multinational)

### ⏰ Timeline Geografica
- **Slider temporale**: Navigazione anno per anno
- **Evoluzione missioni**: Come si sono sviluppate nel tempo
- **Colori mantenuti**: Organizzazioni sempre distinguibili (ONU, UE, NATO, ITA, Bilateral, Multinational)

### 📍 Mappa Cluster
- **Raggruppamento automatico**: Missioni vicine raggruppate
- **Popup HTML ricchi**: Informazioni complete con styling
- **Layer control**: Attiva/disattiva organizzazioni (ONU, UE, NATO, ITA, Bilateral, Multinational)

## 🚀 Installazione e Utilizzo

### 📋 Prerequisiti
- **Python 3.11+** installato
- **Git** per clonare il repository
- **Browser web** moderno

### 🔧 Installazione Dipendenze

#### Windows
```powershell
# Installa Python (se non già installato)
# Scarica da https://www.python.org/downloads/

# Clona il repository
git clone https://github.com/username/MIDA-Missioni-Internazionali-e-Dati-Analitici.git
cd MIDA-Missioni-Internazionali-e-Dati-Analitici

# Crea ambiente virtuale (opzionale ma consigliato)
python -m venv venv
venv\Scripts\activate

# Installa dipendenze
pip install -r requirements.txt

# Dipendenze aggiuntive per le mappe
pip install folium>=0.14.0 geopandas>=0.12.0 pydeck>=0.8.0 geopy>=2.3.0
```

#### Linux/macOS
```bash
# Installa Python (se non già installato)
# Ubuntu/Debian: sudo apt install python3 python3-pip
# macOS: brew install python3

# Clona il repository
git clone https://github.com/username/MIDA-Missioni-Internazionali-e-Dati-Analitici.git
cd MIDA-Missioni-Internazionali-e-Dati-Analitici

# Crea ambiente virtuale (opzionale ma consigliato)
python3 -m venv venv
source venv/bin/activate

# Installa dipendenze
pip install -r requirements.txt

# Dipendenze aggiuntive per le mappe
pip install folium>=0.14.0 geopandas>=0.12.0 pydeck>=0.8.0 geopy>=2.3.0
```

### ⚙️ Configurazione
```yaml
# config/config.yaml
configurazione:
  excel_path: "data/raw/Excel/Matrice dati 1AGG.xlsx"
  documenti: "data/documents"
  processed_data: "data/processed"
```

### 🚀 Avvio Dashboard

#### Metodo 1 (Consigliato)
```bash
# Windows
python run_dashboard.py

# Linux/macOS
python3 run_dashboard.py
```

#### Metodo 2 (Alternativo)
```bash
# Windows
python -m streamlit run src/missioni_dashboard.py

# Linux/macOS
python3 -m streamlit run src/missioni_dashboard.py
```

### 🌐 Accesso Dashboard
- **URL**: http://localhost:8501
- **Porta**: 8501 (configurabile)
- **Browser**: Apri il link nel tuo browser preferito

### 🔄 Ricaricamento Dati
- **Pulsante "🔄 Ricarica Dati"** nella sidebar per forzare l'aggiornamento
- **Cache automatica**: I dati si aggiornano ogni 60 secondi
- **Debug info**: Controlla la sidebar per informazioni tecniche

## 🛠️ Tecnologie Utilizzate

### Backend
- **Python 3.11+**: Linguaggio principale
- **Pandas**: Manipolazione e analisi dati
- **NumPy**: Calcoli numerici avanzati

### Frontend & Visualizzazioni
- **Streamlit**: Dashboard interattiva
- **Plotly**: Grafici e mappe interattive
- **Folium**: Mappe geografiche avanzate

### Elaborazione Documenti
- **PyMuPDF**: Estrazione testo da PDF
- **BeautifulSoup**: Parsing HTML
- **httpx**: Download documenti

### Geografia & Mappe
- **Geopandas**: Dati geografici
- **Geopy**: Geocoding e coordinate
- **PyDeck**: Visualizzazioni 3D

## 📊 Funzionalità Dashboard

### 🔍 Filtri Avanzati
- **Anno di inizio**: Dal 1948 al 2027
- **Tipo di partecipazione**: Militare, Civile, Misto
- **Regione**: Africa, Europa, Medio Oriente, Asia, America
- **Tipo missione**: ONU, UE, NATO, ITA, Bilateral, Multinational
- **Organizzazione**: Filtro specifico per organizzazione

### 📈 Visualizzazioni
- **Grafici a barre**: Missioni per periodo/organizzazione (208 missioni totali)
- **Grafici a torta**: Distribuzione budget e personale
- **Timeline**: Evoluzione temporale delle missioni (1948-2027)
- **Tabelle interattive**: Dati completi con formattazione
- **Mappe interattive**: 4 tipi di mappe avanzate con 6 organizzazioni

### 📥 Esportazione Dati
- **CSV**: Download dati filtrati
- **Excel**: Export completo con multiple sheet
- **PDF**: Report completi con statistiche
- **Formattazione**: Valori monetari e numerici formattati

## 🐛 Risoluzione Problemi

### Problemi Comuni

#### 1. **Porta 8501 già in uso**
```bash
# Windows
netstat -ano | findstr :8501
taskkill /PID <PID> /F

# Linux/macOS
lsof -i :8501
kill -9 <PID>
```

#### 2. **Errori di Installazione**
```bash
pip install --upgrade pip
pip install -r requirements.txt --no-cache-dir
```

#### 3. **Mappe non visibili**
```bash
# Installa dipendenze mappe
pip install folium>=0.14.0 geopandas>=0.12.0 pydeck>=0.8.0 geopy>=2.3.0
```

#### 4. **StreamlitDuplicateElementId**
- ✅ **Risolto**: Tutti i plotly_chart hanno chiavi uniche
- Se persiste, riavvia la dashboard

#### 5. **Problemi con i PDF**
- Verificare che i PDF non siano protetti da password
- Assicurarsi che i PDF siano in formato testo (non scansione)

### Debug Dashboard
- Attiva "🔧 Debug Info" nella sidebar per informazioni tecniche
- Controlla i log nella console per errori dettagliati

## 🤝 Contribuire
1. Fork del repository
2. Creazione branch per feature (`git checkout -b feature/nome-feature`)
3. Commit delle modifiche (`git commit -am 'Aggiunta feature'`)
4. Push del branch (`git push origin feature/nome-feature`)
5. Creazione Pull Request

## 📄 Licenza
Questo progetto è distribuito con licenza MIT. Vedi il file `LICENSE` per maggiori dettagli.

---

## 📸 Screenshots Dashboard

### Panoramica Generale
![Panoramica Missioni](docs/images/panoramica%20missioni.png)

### Dettagli Missioni
![Dettagli Missioni](docs/images/dettagli%20missioni.png)

### Mappe Interattive
![Mappe](docs/images/mappe_interattive.png)

![Numero di missioni per tipo di commitment](docs/images/numero%20di%20missioni%20per%20tipo%20di%20commitment.png)
*Numero di missioni suddivise per tipologia di commitment (Head of Mission, Forze terrestri/navali/aeree, Supporto logistico, ecc.)*

---

🌍 MIDA - Analisi completa delle missioni internazionali italiane dal 1948 al 2027

📊 **Dataset integrato con 208 missioni uniche** dal 1948 al 2027, con normalizzazione organizzazioni e deduplicazione automatica.

🚀 **Come si usa**
1. **Avvia la dashboard**: `python run_dashboard.py`
2. **Apri il browser su**: http://localhost:8501
3. **Usa i filtri** nella sidebar per personalizzare l'analisi
4. **Clicca "🔄 Ricarica Dati"** per aggiornamenti

🗂️ **Struttura dati aggiornata**
Il sistema integra automaticamente:
- **68 missioni** dal CSV principale
- **134 missioni** da fonti Excel aggiuntive
- **208 missioni uniche** dopo deduplicazione

🆕 **Funzionalità principali**
- **Dashboard interattiva** con 208 missioni integrate
- **Mappe avanzate** con 4 tipologie diverse
- **Timeline temporali** con slider interattivi
- **Analisi per organizzazione** (NATO: 48, ONU: 34, UE: 27, Bilateral: 27, Multinational: 16)
- **Export dati** in CSV, Excel e PDF
- **Sistema di debug** integrato nella sidebar

🛠️ **Debug e supporto**
- Se il numero di missioni non è 208, usa il pulsante "🔄 Ricarica Dati"
- Per integrare nuovi dati, aggiungi file Excel in `data/raw/Excel/`
- Per problemi, controlla la sezione debug nella sidebar

---

> Ultimo aggiornamento: luglio 2025

---

**Progetto sviluppato presso l'Università degli Studi di Catania**  
<img src="docs/images/logo_unict.jpg" alt="Logo Università di Catania" width="180"/> 

## 🐞 Debug e raccomandazioni sulle date

- Le colonne `data_inizio` e `data_fine` devono essere sempre in formato `YYYY-MM-DD` (esempio: 2024-07-02).
- La dashboard forza la conversione automatica delle date e segnala nella sidebar quante missioni hanno date non valide.
- Se vedi missioni mancanti nelle analisi temporali, controlla che tutte le date siano corrette e senza valori vuoti.
- In caso di errore, correggi il file CSV e ricarica la dashboard.