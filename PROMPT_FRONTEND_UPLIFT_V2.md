# MIDA Frontend Uplift V2 — Prompt per Redesign Completo

## Contesto

Sei un senior frontend engineer e UI/UX designer specializzato in **instructional design per applicazioni militari/istituzionali**. Devi fare un uplift importante del frontend React di MIDA (Missioni Internazionali e Dati Analitici), una piattaforma di analisi delle 234 missioni militari internazionali italiane dal 1948 al 2026.

## Stato Attuale

Il frontend è funzionante ma visivamente basico:
- **Stack**: React 18 + TypeScript + Vite 5 + Tailwind CSS 3 + Recharts + Leaflet + Framer Motion
- **Dati**: JSON statici pre-generati (missions.json, active.json, stats.json) — nessun backend
- **5 pagine**: Home (Situazione), Dashboard (Analisi), Missioni, Timeline (Cronologia), Mappa (Teatro Operativo)
- **Tema**: Palette militare italiana (olive #4A5D23, navy #1B3A5C, sand #F5F3EE, red #8B1A1A, steel #5A5F63)
- **Problemi attuali**: Layout piatto, card tutte uguali, nessuna gerarchia visiva forte, grafici generici Recharts, mappa basica, nessun storytelling

## Dati Disponibili (JSON)

```typescript
interface Mission {
  nome: string           // "UNIFIL", "KFOR", "Mare Sicuro"...
  paese: string          // "Libano", "Kosovo", "Mediterraneo"...
  regione: string        // "Europa", "Medio Oriente", "Africa", "Asia"
  tipo_missione: string  // "ONU", "NATO", "UE", "ITA", "Bilateral", "Multinational", "Coalizione"
  data_inizio: string    // "1978-03-19"
  data_fine: string|null // "2024-12-31" o null se attiva
  personale_totale: number
  personale_militare: number
  personale_civile: number
  costo_totale: number
  is_active: boolean
  commitment: string     // "Troops", "Advisory/Training"...
  tipo_partecipazione: string // "mil", "civ", "civmil"
}

interface Stats {
  total: 234, active: 40, personnel: 8526, countries: 26, organizations: 7, regions: 5,
  by_org: { NATO: 10, UE: 11, ONU: 5, Bilateral: 6, ITA: 3, Multinational: 4, Coalizione: 1 },
  by_region: { Europa: 14, "Medio Oriente": 15, Africa: 10, Asia: 1 },
  by_decade: { "1940": 6, "1950": 3, "1960": 7, "1970": 2, "1980": 11, "1990": 53, "2000": 61, "2010": 60, "2020": 31 }
}
```

## Obiettivo dell'Uplift

Trasformare il frontend da "dashboard funzionale" a **piattaforma di intelligence militare visivamente impressionante** che:

1. **Racconta una storia** — Progressive disclosure: prima il quadro strategico, poi i dettagli operativi
2. **Sembra un sistema C4I** — Command, Control, Communications, Computers & Intelligence
3. **È istituzionale** — Adatto a una presentazione al Ministero della Difesa o in ambito accademico
4. **È data-dense** — Mostra molte informazioni senza sembrare caotico

## Requisiti Specifici per Pagina

### 1. HOME — "Rapporto Situazione" (la pagina più importante)

**Layout target**: Ispirato a un briefing room militare / situation report

- **Hero Section**: Background con pattern topografico militare sottile (CSS only, no immagini). Titolo con effetto "classified document" — linea rossa sopra, timbro "RAPPORTO SITUAZIONE" stilizzato
- **KPI Strip**: 5 card con micro-sparkline dentro (trend ultimi decenni), non solo numeri piatti. Ogni card ha un bordo sinistro colorato per org, icona militare, numero grande animato, label piccola uppercase
- **Mini-mappa inline**: Piccola mappa Leaflet embedded (300px altezza) che mostra i 26 teatri operativi con marker proporzionali al personale. Non una pagina separata — è il "colpo d'occhio" strategico
- **Dispositivo Operativo**: Le 40 missioni attive raggruppate per organizzazione (ONU, NATO, UE, ITA, Bilateral, Multinational, Coalizione) con header colorato per ogni gruppo. Ogni missione mostra: nome, paese, personale, barra proporzionale al personale
- **Sezione "Impegno Storico"**: Area chart (Recharts AreaChart) che mostra il numero di missioni attive per anno dal 1948 al 2026 — il "battito cardiaco" dell'impegno italiano. Colore olive con gradient trasparente
- **Footer sezione**: Citazione istituzionale + fonti ufficiali

### 2. DASHBOARD — "Analisi Operativa"

- **Sidebar filtri collassabile** (non inline): Filtri per organizzazione, regione, periodo, stato (attiva/conclusa). Sidebar scura (olive-dark) con toggle per aprire/chiudere
- **Grid 2x2 grafici principali**: Donut org (con percentuali dentro), barre regione orizzontali, area chart decennale, treemap paesi (top 15 per numero missioni)
- **Sezione "Top 10"**: Barre orizzontali per le 10 missioni con più personale, con foto-bandiera del paese (emoji flag) e barra proporzionale
- **Tutti i grafici reagiscono ai filtri** in tempo reale

### 3. MISSIONI — "Registro Operazioni"

- **Tabella professionale**: Header fisso, righe alternate (sand/white), sorting su tutte le colonne, ricerca globale
- **Colonna "Stato"**: Indicatore LED verde pulsante per attive, grigio per concluse
- **Colonna "Durata"**: Barra orizzontale proporzionale alla durata (anni)
- **Click su riga**: Pannello laterale (drawer) con dettaglio completo della missione: mappa del paese, timeline, tutti i dati
- **Export**: Bottoni CSV + Excel styled come bottoni militari (olive, uppercase, icona)
- **Contatore in tempo reale**: "Mostrando X di 234 missioni" che si aggiorna con i filtri

### 4. CRONOLOGIA — "Linea del Tempo Operativa"

- **Gantt chart orizzontale**: Ogni missione è una barra colorata per organizzazione. Asse X = anni (1948-2026). Asse Y = missioni raggruppate per org
- **Zoom**: Slider per selezionare range temporale (es. 1990-2010)
- **Hover**: Tooltip ricco con nome, paese, durata, personale
- **Marker eventi chiave**: Linee verticali per eventi storici (Guerra del Golfo 1991, 11 Settembre 2001, Primavera Araba 2011, etc.)
- **Contatore missioni attive per anno**: Linea sovrapposta che mostra quante missioni erano attive in ogni momento

### 5. MAPPA — "Teatro Operativo Globale"

- **Mappa full-bleed** (edge-to-edge, no padding): Occupa tutto lo schermo sotto la navbar
- **Tile layer scuro**: CartoDB dark_matter o Stamen Toner per effetto "war room"
- **Marker proporzionali**: Cerchi la cui dimensione è proporzionale al personale totale nel paese
- **Linee animate**: Polyline da Roma (COI) a ogni teatro con animazione CSS (dash-offset)
- **Pannello laterale sovrapposto**: Lista missioni attive raggruppate per regione, semi-trasparente, scrollabile
- **Cluster per paesi con più missioni**: Es. Libano ha UNIFIL + MIBIL + MTC4L → un unico marker grande con popup che lista tutte
- **Legenda**: Overlay in basso a sinistra con colori per organizzazione + scala dimensioni

## Design System

### Palette (mantenere quella esistente, raffinare)
```
Olive:  #3D4F1E (dark), #4A5D23 (primary), #6B8C2A (light)
Navy:   #1B3A5C (primary), #2C5F8A (light)
Sand:   #F5F3EE (bg), #EAE6DC (card), #D4CFC3 (border)
Red:    #8B1A1A (accent, alert)
Steel:  #5A5F63 (text), #8B9298 (muted)
Khaki:  #7D6B3A (secondary accent)
```

### Tipografia
- **Headings**: Inter 700, uppercase, letter-spacing: 0.05em
- **Body**: Inter 400
- **Data/numeri**: JetBrains Mono 500
- **Labels**: Inter 600, 10-11px, uppercase, tracking-widest

### Componenti UI da creare
- `StatusLed` — Pallino verde pulsante (attiva) o grigio (conclusa)
- `OrgBadge` — Badge colorato per organizzazione con bordo arrotondato
- `PersonnelBar` — Barra orizzontale proporzionale al personale
- `MiniMap` — Mappa Leaflet inline piccola (per HomePage)
- `FilterSidebar` — Sidebar scura collassabile con filtri
- `MissionDrawer` — Pannello laterale per dettaglio missione
- `SparkLine` — Mini grafico inline per trend nelle KPI card
- `TopoBg` — Pattern topografico CSS per background hero

### Animazioni (Framer Motion)
- KPI counter: Conteggio animato da 0 al valore finale (ease-out cubic)
- Card stagger: Le card appaiono una dopo l'altra con 50ms di delay
- Page transition: Fade + slide-up leggero (200ms)
- Mappa: Marker che appaiono con scale animation dal centro
- Hover card: Leggero lift (translateY -2px) + shadow increase

## Vincoli Tecnici

- **NO backend** — Tutti i dati vengono da JSON statici in `/public/data/`
- **NO API key** — Usare Leaflet + OpenStreetMap/CARTO (no Mapbox)
- **NO nuove dipendenze pesanti** — Usare solo: React, Recharts, Leaflet, Framer Motion, Lucide, Tailwind
- **Build deve passare** — `npx tsc --noEmit` deve dare 0 errori
- **Responsive** — Mobile-first, ma ottimizzato per desktop (target: presentazione su schermo grande)
- **Performance** — 234 missioni non sono tante, ma evitare re-render inutili

## File da modificare

```
frontend/src/
├── index.css                    # Aggiungere pattern topografico, animazioni CSS
├── App.tsx                      # Invariato
├── components/
│   ├── layout/Navbar.tsx        # Raffinare con logo SVG inline
│   ├── layout/Footer.tsx        # Aggiungere citazione istituzionale
│   ├── cards/KpiCard.tsx        # Aggiungere sparkline + bordo sinistro colorato
│   ├── charts/OrgDonut.tsx      # Aggiungere percentuali al centro
│   ├── charts/RegionBar.tsx     # Barre orizzontali con label inline
│   ├── charts/DecadeBar.tsx     # Convertire in AreaChart con gradient
│   └── [NUOVI] StatusLed, OrgBadge, PersonnelBar, MiniMap, FilterSidebar, MissionDrawer
├── pages/
│   ├── HomePage.tsx             # Redesign completo (hero + mini-mappa + grouped missions + area chart)
│   ├── DashboardPage.tsx        # Aggiungere sidebar filtri + treemap + top 10
│   ├── MissionsPage.tsx         # Aggiungere drawer dettaglio + barra durata + LED stato
│   ├── TimelinePage.tsx         # Gantt chart + zoom + eventi storici
│   └── MapPage.tsx              # Full-bleed + dark tiles + pannello laterale + cluster
├── hooks/
│   ├── useMissions.ts           # Invariato
│   └── useAnimatedCounter.ts    # Invariato
└── lib/
    ├── constants.ts             # Aggiungere HISTORICAL_EVENTS, COUNTRY_FLAGS
    ├── types.ts                 # Invariato
    └── utils.ts                 # Aggiungere formatDuration, getCountryFlag
```

## Risultato Atteso

Un frontend che quando lo apri dici "wow, sembra un sistema di intelligence militare vero". Professionale, data-dense, con storytelling visivo che guida l'utente dal quadro strategico (Home) ai dettagli operativi (Missioni/Mappa). Adatto a:
- Presentazione accademica (tesi, conferenza)
- Briefing istituzionale (Ministero della Difesa)
- Portfolio professionale di data visualization

## Istruzioni di Implementazione

1. Modifica i file esistenti — NON creare un progetto nuovo
2. Mantieni la struttura attuale (5 pagine, stesso routing)
3. I dati JSON non cambiano — usa quelli esistenti
4. Ogni pagina deve funzionare indipendentemente
5. Testa con `npx tsc --noEmit` e `npm run build` prima di considerare finito
6. Il frontend deve caricare in <2 secondi su localhost
