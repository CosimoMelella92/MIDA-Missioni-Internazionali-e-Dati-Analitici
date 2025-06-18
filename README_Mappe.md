# 🗺️ Mappe Interattive - MIDA Dashboard

## 📍 Panoramica

La dashboard MIDA include una sezione dedicata alle **mappe interattive** che permettono di visualizzare geograficamente le missioni internazionali italiane. Queste mappe forniscono una prospettiva spaziale unica sui dati delle missioni.

## 🎯 Tipologie di Mappe Disponibili

### 1. 🌍 Mappa del Mondo
**Funzionalità:**
- Visualizzazione globale di tutte le missioni
- Marker colorati per regione geografica
- Dimensioni dei marker basate sul personale impiegato
- Hover interattivo con dettagli completi

**Caratteristiche:**
- Mappa interattiva con zoom e pan
- Legenda automatica per regioni
- Popup informativi al click
- Responsive design

### 2. 🔥 Mappa di Calore
**Funzionalità:**
- Visualizzazione della densità del personale
- Gradiente di colori per intensità
- Identificazione delle aree di maggiore impegno

**Caratteristiche:**
- Scala di colori Viridis
- Radius adattivo
- Overlay su mappa base
- Controlli interattivi

### 3. ⏰ Timeline Geografica
**Funzionalità:**
- Evoluzione temporale delle missioni
- Animazione per anno
- Controlli play/pause
- Slider temporale

**Caratteristiche:**
- Frame per ogni anno dal 1991
- Animazione fluida
- Controlli di navigazione
- Visualizzazione dell'espansione geografica

### 4. 📍 Mappa con Cluster
**Funzionalità:**
- Raggruppamento automatico di missioni vicine
- Zoom per dettagli
- Popup informativi
- Gestione di grandi dataset

**Caratteristiche:**
- Cluster dinamici
- Marker individuali al zoom
- Informazioni dettagliate
- Performance ottimizzata

## 🛠️ Tecnologie Utilizzate

### Librerie Principali
- **Plotly**: Mappe interattive e grafici
- **Folium**: Mappe avanzate con OpenStreetMap
- **GeoPandas**: Analisi geospaziali
- **Streamlit**: Integrazione dashboard

### Dati Geografici
- **Coordinate**: Latitudine/Longitudine per ogni paese
- **Regioni**: Classificazione geografica
- **Sub-regioni**: Dettaglio territoriale

## 📊 Struttura Dati Geografici

### File: `data/geo/paesi_coordinate.csv`
```csv
paese,latitudine,longitudine,regione,sub_regione
Libano,33.8547,35.8623,Medio Oriente,Levante
Mali,17.5707,-3.9962,Africa,Africa Occidentale
...
```

### Campi:
- `paese`: Nome del paese
- `latitudine`: Coordinata latitudine
- `longitudine`: Coordinata longitudine
- `regione`: Regione geografica
- `sub_regione`: Sub-regione geografica

## 🎨 Personalizzazione

### Colori per Regioni
```python
colors = {
    'Africa': '#1f77b4',
    'Europa': '#ff7f0e', 
    'Medio Oriente': '#2ca02c',
    'Asia': '#d62728',
    'America': '#9467bd'
}
```

### Colori per Tipo Partecipazione
```python
color_map = {
    'mil': 'red',      # Militare
    'civ': 'blue',     # Civile
    'civmil': 'green'  # Misto
}
```

## 🔧 Installazione Dipendenze

```bash
# Dipendenze per le mappe
pip install folium>=0.14.0
pip install geopandas>=0.12.0
pip install pydeck>=0.8.0
pip install geopy>=2.3.0
```

## 📱 Utilizzo nella Dashboard

### Accesso alle Mappe
1. Apri la dashboard MIDA
2. Scorri fino alla sezione "🗺️ Mappe Interattive"
3. Seleziona il tab desiderato:
   - 🌍 Mappa del Mondo
   - 🔥 Mappa di Calore
   - ⏰ Timeline
   - 📍 Cluster

### Filtri Applicabili
- **Anno di inizio**: Filtra missioni per periodo
- **Tipo partecipazione**: Militare/Civile/Misto
- **Regione**: Filtra per area geografica
- **Tipo missione**: ONU/NATO/UE/ITA

## 🎯 Casi d'Uso

### 1. Analisi Geografica
- Identificare concentrazioni di missioni
- Analizzare distribuzione regionale
- Valutare copertura geografica

### 2. Analisi Temporale
- Tracciare evoluzione delle missioni
- Identificare trend geografici
- Analizzare espansione territoriale

### 3. Analisi Risorse
- Visualizzare distribuzione personale
- Analizzare costi per area
- Identificare aree di maggiore impegno

### 4. Reporting
- Creare mappe per presentazioni
- Esportare visualizzazioni
- Generare report geografici

## 🔄 Aggiornamento Dati

### Aggiungere Nuovi Paesi
1. Aggiungi coordinate in `data/geo/paesi_coordinate.csv`
2. Assicurati che il nome del paese corrisponda ai dati missioni
3. Riavvia la dashboard

### Modificare Classificazioni
1. Aggiorna i campi `regione` e `sub_regione`
2. Modifica i colori se necessario
3. Riavvia la dashboard

## 🐛 Risoluzione Problemi

### Mappa Non Visualizzata
- Verifica installazione Folium: `pip install folium`
- Controlla coordinate nel file CSV
- Verifica connessione internet per tile maps

### Performance Lente
- Riduci numero di marker
- Usa clustering per grandi dataset
- Ottimizza query dati

### Errori Coordinate
- Verifica formato lat/lon
- Controlla valori validi (-90 a 90 per lat, -180 a 180 per lon)
- Rimuovi righe con coordinate mancanti

## 📈 Estensioni Future

### Funzionalità Pianificate
- **Mappe 3D**: Visualizzazione tridimensionale
- **Routing**: Percorsi tra missioni
- **Satellite**: Immagini satellitari
- **Real-time**: Aggiornamenti in tempo reale

### Integrazioni
- **OpenStreetMap**: Dati geografici dettagliati
- **Google Maps**: API avanzate
- **ArcGIS**: Strumenti professionali
- **QGIS**: Analisi geospaziali avanzate

---

**🗺️ Mappe Interattive MIDA**
*Visualizzazione geografica delle missioni internazionali italiane* 