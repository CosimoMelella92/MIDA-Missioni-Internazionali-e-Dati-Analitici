# MIDA Frontend V3 — Redesign Istituzionale Professionale

## Ruolo

Sei un senior frontend engineer specializzato in **portali istituzionali governativi e militari**. Il tuo riferimento visivo sono:
- Il portale dello **Stato Maggiore della Difesa** (difesa.it)
- I report annuali del **Ministero della Difesa** (Documento Programmatico Pluriennale)
- Le dashboard operative **NATO ACO** (Allied Command Operations)
- I briefing paper dell'**ISPI** (Istituto per gli Studi di Politica Internazionale)

NON sei un "vibe coder". NON usi emoji come decorazione, NON usi icone Lucide ovunque, NON usi effetti hover appariscenti. Produci interfacce **sobrie, dense di dati, tipograficamente impeccabili** — il tipo di cosa che un Generale di Corpo d'Armata guarderebbe senza alzare un sopracciglio.

---

## Progetto

MIDA (Missioni Internazionali e Dati Analitici) — piattaforma di analisi delle **234 missioni militari internazionali italiane** dal 1948 al 2026. 40 missioni attualmente in corso, 8.526 unità di personale impiegato.

## Stack Tecnico (non modificare)

- React 18 + TypeScript + Vite 5 + Tailwind CSS 3
- Recharts (grafici), Leaflet (mappe), Framer Motion (animazioni sobrie)
- Dati: JSON statici in `/public/data/` — nessun backend

## Cosa NON Fare

1. **NO emoji nelle UI** — Niente bandiere emoji (🇮🇹), niente onde (🌊), niente icone decorative sparse. Le bandiere si rappresentano con il nome del paese, punto.
2. **NO icone Lucide ovunque** — Le icone si usano SOLO nella navbar e nei bottoni azione. Mai come decorazione di card o sezioni.
3. **NO effetti "wow"** — Niente animazioni appariscenti, niente glow, niente pulse esagerato. Le animazioni sono solo: fade-in al caricamento, contatori numerici, transizioni pagina.
4. **NO colori saturi** — La palette è desaturata, istituzionale. Mai colori pieni al 100% di opacità su grandi superfici.
5. **NO card con bordi arrotondati grandi** — Bordi `rounded` (4px), mai `rounded-xl` o `rounded-2xl`. Questo è un portale governativo, non un'app consumer.
6. **NO font display** — Solo Inter per il testo, JetBrains Mono per i numeri. Nessun font decorativo.

## Cosa Fare

### Identità Visiva Istituzionale

1. **Emblema della Repubblica Italiana** — Scaricare il file SVG ufficiale dell'emblema e posizionarlo nella navbar accanto al nome MIDA. Dimensione: 28x28px. File: `frontend/public/emblema_repubblica.svg`. Se non disponibile, creare un componente SVG inline semplificato (stella + corona d'alloro + ruota dentata) in monocromo bianco.

2. **Foto istituzionali** — Aggiungere 2-3 foto nella HomePage come elementi di contesto visivo:
   - Hero background: foto sfocata (CSS blur + overlay scuro) di militari italiani in operazione (usare un placeholder gradient se la foto non è disponibile — NON lasciare spazi vuoti)
   - Le foto vanno in `frontend/public/images/` e devono essere referenziate con path relativo
   - Se le foto non sono disponibili, usare un **gradient istituzionale** navy→olive come fallback, MAI un pattern decorativo

3. **Tipografia da documento ufficiale**:
   - Titoli sezione: `font-size: 14px`, `font-weight: 700`, `text-transform: uppercase`, `letter-spacing: 0.12em`, `color: #1B3A5C`, con una linea sottile sotto (`border-bottom: 1px solid #D4CFC3`)
   - NON usare font-size grandi per i titoli sezione (mai > 16px). I titoli grandi sono SOLO per il numero nelle KPI card.
   - Corpo testo: 13px, colore #5A5F63
   - Numeri/dati: JetBrains Mono 500, colore #1B3A5C

### Palette (invariata, ma usata con disciplina)

```
Navy:   #1B3A5C — titoli, header tabelle, navbar
Olive:  #4A5D23 — accenti positivi, barre, indicatori "attivo"
Sand:   #F5F3EE — sfondo pagina
White:  #FFFFFF — sfondo card
Border: #D4CFC3 — bordi card, separatori
Steel:  #5A5F63 — testo corpo
Muted:  #8B9298 — testo secondario, label
Red:    #8B1A1A — SOLO per alert, errori, o il marker Roma sulla mappa
```

Regola d'oro: **il 90% della pagina è bianco/sabbia/grigio**. I colori si usano con parsimonia chirurgica.

---

## Specifiche per Pagina

### 1. HOME — "Quadro Situazione"

**Riferimento**: Prima pagina di un Documento Programmatico Pluriennale della Difesa.

**Struttura dall'alto in basso:**

```
┌─────────────────────────────────────────────────────────┐
│ HERO: Gradient navy→olive, titolo bianco, sottotitolo   │
│ "MINISTERO DELLA DIFESA — QUADRO SITUAZIONE FEB 2026"   │
│ Emblema Repubblica in alto a destra (piccolo, 24px)      │
│ Nessuna foto se non disponibile — solo gradient pulito   │
├─────────────────────────────────────────────────────────┤
│ KPI: 5 numeri in riga, sfondo bianco, bordo sottile      │
│ Solo numero grande (JetBrains Mono 700, 28px) + label    │
│ sotto (10px uppercase tracking-widest). NESSUNA icona.   │
│ Separati da linee verticali sottili, non da gap.         │
├─────────────────────────────────────────────────────────┤
│ DUE COLONNE:                                             │
│ [Sinistra 60%] Tabella missioni attive (compatta)        │
│   - Colonne: Nome | Teatro | Org | Personale             │
│   - Header navy, righe alternate, font 11px              │
│   - Indicatore pallino 6px (olive=attiva) prima del nome │
│   - Ordinata per personale decrescente                   │
│                                                          │
│ [Destra 40%] Mini-mappa Leaflet (dark tiles)             │
│   - Altezza fissa 400px, bordo 1px #D4CFC3               │
│   - Marker cerchio proporzionali, colore per org         │
│   - Roma: quadrato rosso piccolo                         │
│   - Linee tratteggiate Roma→teatri (opacity 0.15)        │
│   - NO popup, NO interazione — è una mappa di contesto  │
├─────────────────────────────────────────────────────────┤
│ GRAFICO: Area chart "Missioni attive per anno"           │
│ Sfondo bianco, bordo sottile. Titolo 14px uppercase.     │
│ Area olive con opacity 0.15, linea olive 1.5px.          │
│ Asse X: anni (ogni 10), asse Y: numeri. Font 10px.      │
│ Altezza: 180px. Nessun tooltip appariscente.             │
├─────────────────────────────────────────────────────────┤
│ TRE COLONNE: Donut org | Barre regione | Barre decennio │
│ Ogni grafico in card bianca con bordo, titolo 14px.      │
│ Altezza uniforme 240px. Colori desaturati.               │
└─────────────────────────────────────────────────────────┘
```

**Cosa NON deve avere la HomePage:**
- Nessun "stamp" o effetto documento classificato (è kitsch)
- Nessuna emoji bandiera
- Nessun LED pulsante (troppo appariscente per la home)
- Nessun raggruppamento per organizzazione con header colorati (troppo pesante visivamente)

### 2. DASHBOARD — "Analisi"

**Riferimento**: Dashboard analitica NATO/ISPI.

- **Barra filtri orizzontale** in alto (NON sidebar): 3 select inline (Org, Regione, Stato) + bottone "Reset" + contatore "234 missioni"
- **Grid 2×2 grafici**: Donut org, barre regione, barre decennio, barre top 10 personale
- **Sotto**: Tabella riassuntiva per organizzazione (righe: ONU, NATO, UE... | colonne: Totali, Attive, Personale, % del totale)
- Tutti i grafici reagiscono ai filtri
- Nessun treemap (troppo decorativo) — usare tabelle e barre

### 3. MISSIONI — "Registro"

- **Tabella densa**: font 11px, righe alte 32px, header fisso navy
- **Colonne**: Stato (pallino 6px) | Nome | Teatro | Org (testo, non badge colorato) | Inizio | Fine | Personale | Durata (testo "12 anni", non barra)
- **Click riga** → drawer laterale (320px) con scheda missione:
  - Header navy con nome missione
  - Griglia dati: Teatro, Regione, Organizzazione, Periodo, Personale (mil/civ/tot), Costo, Commitment
  - Ogni dato su una riga: label a sinistra (10px uppercase muted), valore a destra (13px navy)
  - Nessuna icona nel drawer
- **Filtri**: Search + select org + checkbox "Solo attive" — inline, compatti
- **Export CSV**: Bottone piccolo, testo "ESPORTA CSV", olive, uppercase 10px

### 4. CRONOLOGIA — "Timeline"

- **Gantt chart**: Barre orizzontali, colore per org (opacity 0.7 concluse, 1.0 attive)
- **Asse X**: anni con tick ogni 10 anni, font mono 9px
- **Zoom**: Due input range per selezionare periodo (start/end)
- **Linee verticali** per eventi storici: tratteggio sottile (#8B1A1A opacity 0.2), label 7px in alto
- **Tooltip on hover**: Box bianco con bordo, nome + teatro + periodo + personale. Font 11px. Nessuna emoji.
- **NO overlay SVG area** (troppo decorativo per questa vista)

### 5. MAPPA — "Dispositivo"

- **Full-bleed**: La mappa occupa tutto lo spazio sotto la navbar
- **Tile**: CartoDB dark_all (war room)
- **Marker**: Cerchi proporzionali al personale. Colore per org principale del paese. Bordo bianco 1.5px.
- **Roma**: Quadrato rosso 10px, tooltip "COI — Comando Operativo di Vertice Interforze"
- **Linee**: Roma→teatri, olive opacity 0.15, tratteggio 4 6
- **Popup on click**: Box con sfondo scuro (#1A1A1A 90%), testo chiaro. Nome paese (uppercase 9px muted), lista missioni con org + personale. Font 10-11px.
- **Pannello laterale** (280px, sfondo #1A1A1A 85%, backdrop-blur):
  - Header: "DISPOSITIVO OPERATIVO" + contatore missioni + personale totale
  - Lista missioni raggruppate per regione
  - Ogni missione: pallino org + nome + personale (font 10px)
  - Scrollabile, nessuna animazione
- **Legenda**: In basso a sinistra, sfondo scuro, pallini colorati + nome org (font 8px)

---

## Navbar

```
┌──────────────────────────────────────────────────────────┐
│ [Emblema 24px] MIDA  │ Situazione │ Analisi │ Registro │ │
│                       │ Timeline │ Dispositivo           │
│                                          Stato Maggiore  │
└──────────────────────────────────────────────────────────┘
```

- Sfondo: #1B3A5C (navy pieno)
- Altezza: 48px
- Logo: Emblema Repubblica (SVG bianco 24px) + "MIDA" (Inter 700, 14px, bianco, tracking 0.15em)
- Link: Inter 600, 11px, uppercase, tracking 0.1em, colore #D4CFC3, attivo: sfondo #4A5D23
- "Stato Maggiore Difesa" a destra: 9px, uppercase, tracking 0.2em, #8B9298
- **NESSUNA icona** nei link di navigazione — solo testo

## Footer

- Sfondo: #1B3A5C
- Altezza: 40px
- Testo: "MIDA — Missioni Internazionali e Dati Analitici · Università di Catania · Dati: Ministero della Difesa"
- Font: 9px, uppercase, tracking 0.15em, #8B9298
- Nessun link, nessuna icona

---

## Immagini da Aggiungere

### 1. Emblema della Repubblica Italiana
- File: `frontend/public/emblema_repubblica.svg`
- Usato in: Navbar (24px), Hero HomePage (opzionale, 40px in alto a destra)
- Se non hai il file SVG, crea un componente React `EmblemaSvg` con un SVG inline semplificato: stella a 5 punte bianca dentro corona d'alloro, tutto monocromo bianco. Non deve essere perfetto — deve essere riconoscibile.

### 2. Foto hero (opzionale)
- File: `frontend/public/images/hero_military.jpg`
- Usato in: HomePage hero come background-image con `filter: blur(2px) brightness(0.3)` e overlay gradient navy→transparent
- Se la foto non è disponibile: gradient lineare `#1B3A5C → #3D4F1E` (navy→olive). MAI lasciare il fallback come sfondo piatto monocolore.
- La foto deve essere di militari italiani in operazione (caschi blu, mezzi militari, etc.) — NON foto generiche stock

### 3. Nessun'altra immagine
- Il resto dell'interfaccia è puro dato + tipografia + colore. Le immagini sono SOLO per il contesto istituzionale (emblema + hero).

---

## Animazioni (Framer Motion — uso MINIMO)

- **Contatori KPI**: Da 0 al valore finale in 800ms, ease-out. UNICA animazione visibile.
- **Fade-in pagina**: opacity 0→1 in 200ms al mount. Nessun slide.
- **Nessun stagger** sulle card o righe tabella.
- **Nessun hover lift** sulle card.
- **Nessun pulse** tranne il pallino stato attivo (e anche quello: `animation-duration: 3s`, molto lento e sottile).

---

## Vincoli

- **NO nuove dipendenze** — Usa solo React, Recharts, Leaflet, Framer Motion, Tailwind
- **NO backend** — JSON statici in `/public/data/`
- **NO API key** — Leaflet + CARTO tiles
- **Build pulita**: `npx tsc --noEmit` = 0 errori, `npm run build` = successo
- **Responsive**: Funziona su mobile, ma ottimizzato per desktop 1440px+
- **Performance**: <2s load su localhost

## File da Modificare

```
frontend/
├── public/
│   ├── emblema_repubblica.svg       # NUOVO — Emblema Repubblica
│   └── images/
│       └── hero_military.jpg        # NUOVO — Foto hero (opzionale)
├── src/
│   ├── index.css                    # Rimuovere stamp, topo-bg. Aggiungere stili sobri.
│   ├── App.tsx                      # Invariato
│   ├── components/
│   │   ├── layout/Navbar.tsx        # Emblema + no icone link
│   │   ├── layout/Footer.tsx        # Testo semplice navy
│   │   ├── cards/KpiCard.tsx        # Solo numero + label, no icona
│   │   ├── charts/OrgDonut.tsx      # Colori desaturati
│   │   ├── charts/RegionBar.tsx     # Barre con label inline
│   │   └── charts/DecadeBar.tsx     # Area chart con gradient sottile
│   ├── pages/
│   │   ├── HomePage.tsx             # Hero gradient + tabella attive + mini-mappa + grafici
│   │   ├── DashboardPage.tsx        # Filtri inline + 2x2 grafici + tabella org
│   │   ├── MissionsPage.tsx         # Tabella densa + drawer senza icone
│   │   ├── TimelinePage.tsx         # Gantt + zoom + eventi storici
│   │   └── MapPage.tsx              # Full-bleed dark + pannello laterale
│   ├── hooks/                       # Invariati
│   └── lib/
│       ├── constants.ts             # Rimuovere COUNTRY_FLAGS emoji
│       ├── types.ts                 # Invariato
│       └── utils.ts                 # Invariato
```

## Risultato Atteso

Un portale che sembra prodotto dal **Centro Innovazione Difesa** o dall'**ISPI**. Quando un professore universitario, un ufficiale dello Stato Maggiore, o un analista di politica internazionale lo apre, la reazione deve essere: "questo è un lavoro serio". Non "wow che bello" — ma "questo è credibile, professionale, e i dati sono immediatamente leggibili".

Il test finale: stampa una pagina in PDF. Se sembra un documento ufficiale del Ministero della Difesa, hai fatto bene. Se sembra uno screenshot di un'app web, devi rifare.
