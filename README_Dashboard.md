# 🌍 MIDA Dashboard - Missioni Internazionali e Dati Analitici

## 📊 Panoramica

Questa dashboard Streamlit fornisce un'analisi completa delle missioni internazionali italiane, organizzata secondo i seguenti criteri:

### 🎯 Funzionalità Principali

1. **📅 Analisi per Periodi Temporali**
   - 1991-2001
   - 2001-2015  
   - 2015-ad oggi

2. **🎯 Tipo di Partecipazione**
   - Militare (mil)
   - Civile (civ)
   - Misto (civmil)

3. **👥 Personale Impiegato**
   - Totale per periodo
   - Differenziazione civile/militare

4. **💰 Budget Impiegato**
   - Totale per periodo
   - Distribuzione per tipo di missione

5. **🌍 Analisi Geografica**
   - Numero missioni per regione
   - Numero missioni per sub-regione
   - Mappe di calore interattive

## 🚀 Come Eseguire la Dashboard

### Metodo 1: Script Python
```bash
python run_dashboard.py
```

### Metodo 2: Streamlit Diretto
```bash
streamlit run src/missioni_dashboard.py
```

### Metodo 3: Con Parametri Personalizzati
```bash
streamlit run src/missioni_dashboard.py --server.port 8501 --server.address localhost
```

## 📁 Struttura dei Dati

La dashboard utilizza i seguenti file di dati:

- `data/processed/missioni_complete.csv` - Dataset completo con tutti i campi
- `data/processed/missioni.csv` - Dataset di fallback

### Campi del Dataset

| Campo | Descrizione | Tipo |
|-------|-------------|------|
| `nome` | Nome della missione | String |
| `paese` | Paese di destinazione | String |
| `regione` | Regione geografica | String |
| `sub_regione` | Sub-regione geografica | String |
| `tipo_partecipazione` | Tipo (mil/civ/civmil) | String |
| `data_inizio` | Data di inizio missione | Date |
| `data_fine` | Data di fine missione | Date |
| `personale_militare` | Numero personale militare | Integer |
| `personale_civile` | Numero personale civile | Integer |
| `personale_totale` | Totale personale | Integer |
| `costo_totale` | Costo totale in euro | Float |
| `tipo_missione` | Tipo (ONU/NATO/UE/ITA) | String |

## 🔍 Filtri Disponibili

La dashboard include filtri interattivi nella sidebar:

- **Anno di inizio**: Filtra per anno specifico
- **Tipo di partecipazione**: Filtra per mil/civ/civmil
- **Regione**: Filtra per regione geografica

## 📈 Visualizzazioni

### 1. Metriche Principali
- Numero totale missioni
- Personale totale impiegato
- Costo totale
- Missioni attive

### 2. Analisi Temporale
- Grafici a barre per numero missioni per periodo
- Grafici a torta per distribuzione budget
- Grafici stacked per personale militare/civile

### 3. Analisi Partecipazione
- Distribuzione per tipo di partecipazione
- Analisi personale per tipo

### 4. Analisi Geografica
- Missioni per regione
- Mappe di calore per sub-regioni
- Costi e personale per regione

### 5. Timeline Interattiva
- Timeline completa delle missioni
- Hover con dettagli completi

## 🛠️ Requisiti Tecnici

### Dipendenze Python
```
streamlit>=1.22.0
pandas>=1.5.0
plotly>=5.13.0
numpy>=1.21.0
```

### Installazione
```bash
pip install -r requirements.txt
```

## 🎨 Personalizzazione

### Tema
Il tema è configurato in `.streamlit/config.toml`:
- Colore primario: #1E88E5
- Sfondo: #FFFFFF
- Layout: Wide

### CSS Personalizzato
La dashboard include CSS personalizzato per:
- Header principali
- Card delle metriche
- Intestazioni delle sezioni

## 📊 Esportazione Dati

I dati visualizzati possono essere esportati:
- Copiando le tabelle interattive
- Utilizzando le funzionalità di download di Streamlit
- Accedendo direttamente ai file CSV

## 🔧 Risoluzione Problemi

### Errore "File non trovato"
Assicurati che i file di dati siano presenti in `data/processed/`

### Errore "Porta già in uso"
Cambia la porta nel comando:
```bash
streamlit run src/missioni_dashboard.py --server.port 8502
```

### Errore "Dipendenze mancanti"
Installa le dipendenze:
```bash
pip install streamlit pandas plotly numpy
```

## 📞 Supporto

Per problemi o richieste:
1. Controlla i log di Streamlit
2. Verifica la struttura dei dati
3. Controlla le dipendenze installate

## 🔄 Aggiornamenti

La dashboard si aggiorna automaticamente quando:
- I file di dati vengono modificati
- Il codice viene aggiornato
- I filtri vengono cambiati

---

**🌍 MIDA - Missioni Internazionali e Dati Analitici**
*Dashboard per l'analisi delle missioni internazionali italiane* 