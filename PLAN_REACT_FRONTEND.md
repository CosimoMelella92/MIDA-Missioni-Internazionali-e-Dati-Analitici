# MIDA Frontend React — Piano di Implementazione

## 🧠 Chain of Thinking / Reasoning

### Problema
La dashboard Streamlit attuale è funzionale ma limitata in termini di:
- **UX/UI**: Streamlit impone un layout lineare, sidebar fissa, nessun routing reale
- **Performance**: Ogni interazione ricarica l'intera pagina (no SPA)
- **Instructional Design**: Non c'è storytelling visivo, nessuna narrazione guidata dei dati
- **Branding**: Limitata personalizzazione grafica (CSS injection fragile)
- **Mobile**: Streamlit non è ottimizzato per mobile

### Analisi delle Opzioni

| Criterio | React+Vite | Next.js | Angular |
|----------|-----------|---------|---------|
| Velocità setup | ⭐⭐⭐ | ⭐⭐ | ⭐ |
| Bundle size | ~150KB | ~250KB | ~500KB |
| Data viz ecosystem | Recharts, D3, Nivo | Idem | ng2-charts |
| Mappe | Mapbox GL, Leaflet | Idem | Idem |
| Styling | Tailwind + shadcn/ui | Idem | Angular Material |
| Deploy | Netlify/Vercel (statico) | Vercel (SSR) | Firebase |
| Curva apprendimento | Bassa | Media | Alta |

**Decisione**: React + Vite + Tailwind + shadcn/ui + Recharts

### Perché NON Next.js?
- Non serve SSR: i dati sono statici (JSON pre-generato dalla pipeline)
- Non serve API routing: nessun backend dinamico
- Vite è più veloce in dev e produce bundle più piccoli

### Architettura Dati
```
Pipeline Python (aggregator.py)
    ↓ genera
data/processed/missioni_complete.csv
    ↓ script di export
frontend/public/data/missions.json    ← dati completi
frontend/public/data/active.json      ← solo missioni attive
frontend/public/data/stats.json       ← KPI pre-calcolati
```
**Nessun backend necessario** — i dati sono pre-processati e serviti come JSON statici.

---

## 📐 Struttura del Progetto React

```
frontend/
├── public/
│   ├── data/
│   │   ├── missions.json          # Dataset completo (237 missioni)
│   │   ├── active.json            # 38 missioni attive
│   │   └── stats.json             # KPI aggregati
│   └── favicon.ico
├── src/
│   ├── main.tsx                   # Entry point
│   ├── App.tsx                    # Router + Layout
│   ├── index.css                  # Tailwind base
│   │
│   ├── components/
│   │   ├── ui/                    # shadcn/ui components
│   │   ├── layout/
│   │   │   ├── Navbar.tsx         # Navigation bar
│   │   │   ├── Sidebar.tsx        # Filtri laterali
│   │   │   └── Footer.tsx
│   │   ├── cards/
│   │   │   ├── KpiCard.tsx        # Card KPI animata
│   │   │   └── MissionCard.tsx    # Card missione
│   │   ├── charts/
│   │   │   ├── OrgDonut.tsx       # Donut per organizzazione
│   │   │   ├── TimelineBar.tsx    # Barre temporali
│   │   │   ├── RegionTreemap.tsx  # Treemap regioni
│   │   │   ├── PersonnelBar.tsx   # Barre personale
│   │   │   └── TrendLine.tsx      # Trend storico
│   │   ├── maps/
│   │   │   ├── ActiveMissionsMap.tsx  # Mappa Mapbox missioni attive
│   │   │   ├── HeatmapLayer.tsx       # Layer heatmap
│   │   │   └── MissionPopup.tsx       # Popup missione
│   │   └── tables/
│   │       └── MissionsTable.tsx  # Tabella filtrabili TanStack
│   │
│   ├── pages/
│   │   ├── HomePage.tsx           # Hero + KPI + mappa attive
│   │   ├── DashboardPage.tsx      # Dashboard analitica completa
│   │   ├── MissionsPage.tsx       # Elenco missioni + filtri + export
│   │   ├── TimelinePage.tsx       # Timeline interattiva
│   │   ├── MapPage.tsx            # Mappe full-screen
│   │   └── AboutPage.tsx          # Info progetto + metodologia
│   │
│   ├── hooks/
│   │   ├── useMissions.ts         # Fetch + cache dati missioni
│   │   ├── useFilters.ts          # Stato filtri globale
│   │   └── useAnimatedCounter.ts  # Counter animato per KPI
│   │
│   ├── lib/
│   │   ├── types.ts               # TypeScript interfaces
│   │   ├── constants.ts           # Colori, config
│   │   ├── utils.ts               # Formattazione, helpers
│   │   └── filters.ts             # Logica filtri
│   │
│   └── styles/
│       └── theme.ts               # Tema MIDA (colori, font)
│
├── package.json
├── vite.config.ts
├── tailwind.config.ts
├── tsconfig.json
└── README.md
```

---

## 🎨 Design System — Instructional Design

### Principi
1. **Progressive Disclosure**: Mostra prima i KPI chiave, poi i dettagli
2. **Visual Hierarchy**: Hero → KPI → Mappa → Grafici → Tabella
3. **Storytelling**: Ogni pagina racconta una storia con i dati
4. **Consistency**: Stessa palette colori, stessi pattern ovunque
5. **Accessibility**: WCAG 2.1 AA, contrasto minimo 4.5:1

### Palette Colori (MIDA Theme)
```typescript
const MIDA_COLORS = {
  primary: '#264653',      // Blu scuro (header, nav)
  secondary: '#2A9D8F',    // Teal (accenti, CTA)
  accent: '#E9C46A',       // Oro (highlight)
  danger: '#E76F51',       // Rosso (alert)
  success: '#06D6A0',      // Verde (attivo)

  org: {
    ONU: '#1F77B4',
    NATO: '#2CA02C',
    UE: '#FF7F0E',
    ITA: '#D62728',
    Bilateral: '#9467BD',
    Multinational: '#8C564B',
    Coalizione: '#E377C2',
  },

  bg: {
    primary: '#FAFAFA',
    card: '#FFFFFF',
    dark: '#1A1A2E',
  }
};
```

### Tipografia
- **Headings**: Inter (700, 600)
- **Body**: Inter (400)
- **Data**: JetBrains Mono (tabelle, numeri)

---

## 📄 Pagine — Dettaglio

### 1. HomePage (Hero + Overview)
```
┌─────────────────────────────────────────────────┐
│  MIDA — Missioni Internazionali Italiane        │
│  [Hero con mappa animata sullo sfondo]           │
│  "237 missioni dal 1949 · 38 attive nel 2026"   │
├─────────────────────────────────────────────────┤
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐      │
│  │ 237 │ │  38 │ │8.8K │ │  10 │ │   5 │      │
│  │Total│ │Attiv│ │Pers.│ │NATO │ │Regio│      │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘      │
├─────────────────────────────────────────────────┤
│  [Mappa missioni attive — Mapbox GL full-width]  │
│  Linee animate Roma → ogni missione              │
├─────────────────────────────────────────────────┤
│  [Donut Org] [Barre Regioni] [Timeline mini]    │
└─────────────────────────────────────────────────┘
```

### 2. DashboardPage (Analisi completa)
- Filtri in sidebar collassabile
- Grid di grafici: donut org, barre regioni, treemap, sunburst
- Tutti i grafici reagiscono ai filtri in tempo reale

### 3. MissionsPage (Tabella + Dettaglio)
- TanStack Table con sorting, filtering, pagination
- Click su riga → drawer laterale con dettaglio missione
- Export CSV/Excel

### 4. TimelinePage
- Timeline orizzontale scrollabile (1949-2027)
- Barre colorate per durata missione
- Zoom in/out, filtro per organizzazione

### 5. MapPage
- Mapbox GL full-screen
- Layer toggle: attive, storiche, heatmap, cluster
- Popup ricchi con dettagli missione

---

## 🔧 Stack Tecnico

### Dependencies
```json
{
  "dependencies": {
    "react": "^18.3",
    "react-dom": "^18.3",
    "react-router-dom": "^6.26",
    "recharts": "^2.12",
    "mapbox-gl": "^3.5",
    "react-map-gl": "^7.1",
    "@tanstack/react-table": "^8.20",
    "framer-motion": "^11.5",
    "lucide-react": "^0.441",
    "clsx": "^2.1",
    "tailwind-merge": "^2.5"
  },
  "devDependencies": {
    "vite": "^5.4",
    "@vitejs/plugin-react": "^4.3",
    "typescript": "^5.5",
    "tailwindcss": "^3.4",
    "autoprefixer": "^10.4",
    "postcss": "^8.4",
    "@types/react": "^18.3"
  }
}
```

### shadcn/ui Components da installare
- Button, Card, Badge, Tabs, Table, Select, Input
- Sheet (drawer), Dialog, Tooltip, Separator
- DropdownMenu, Command (search)

---

## 📊 Script di Export Dati

Aggiungere a `core/aggregator.py` o come script separato:

```python
# export_frontend_data.py
import pandas as pd
import json

df = pd.read_csv("data/processed/missioni_complete.csv")

# missions.json — dataset completo
missions = df.to_dict(orient="records")
with open("frontend/public/data/missions.json", "w") as f:
    json.dump(missions, f, default=str)

# active.json — solo attive
active = df[df["is_active"] == True].to_dict(orient="records")
with open("frontend/public/data/active.json", "w") as f:
    json.dump(active, f, default=str)

# stats.json — KPI pre-calcolati
stats = {
    "total": len(df),
    "active": len(active),
    "personnel": int(df[df["is_active"]==True]["personale_totale"].sum()),
    "countries": df[df["is_active"]==True]["paese"].nunique(),
    "organizations": df["tipo_missione"].nunique(),
    "regions": df["regione"].nunique(),
    "by_org": df[df["is_active"]==True].groupby("tipo_missione").size().to_dict(),
    "by_region": df[df["is_active"]==True].groupby("regione").size().to_dict(),
}
with open("frontend/public/data/stats.json", "w") as f:
    json.dump(stats, f)
```

---

## 🚀 Piano di Implementazione (Fasi)

### Fase 1 — Setup + HomePage (2-3h)
1. `npm create vite@latest frontend -- --template react-ts`
2. Installare Tailwind, shadcn/ui, Recharts, Framer Motion
3. Creare layout base (Navbar, Footer)
4. Implementare HomePage con KPI animati
5. Export dati JSON dalla pipeline

### Fase 2 — Mappa Attive (2h)
1. Integrare Mapbox GL / react-map-gl
2. Creare ActiveMissionsMap con marker colorati
3. Linee animate Roma → missioni
4. Popup ricchi

### Fase 3 — Dashboard Analitica (2-3h)
1. Implementare grafici Recharts (donut, barre, treemap)
2. Sistema filtri globale con useFilters hook
3. Grid responsive di grafici

### Fase 4 — Tabella Missioni (1-2h)
1. TanStack Table con sorting/filtering
2. Drawer dettaglio missione
3. Export CSV

### Fase 5 — Timeline (1-2h)
1. Timeline orizzontale scrollabile
2. Barre colorate per durata
3. Zoom e filtri

### Fase 6 — Polish + Deploy (1h)
1. Animazioni Framer Motion
2. Dark mode
3. Responsive mobile
4. Deploy su Netlify/Vercel

**Tempo totale stimato: 10-14 ore**

---

## ⚠️ Note Importanti

1. **Mapbox GL richiede un token API** (gratuito fino a 50K richieste/mese)
   - Alternativa gratuita: Leaflet + OpenStreetMap (meno bello ma zero costi)
2. **I dati sono statici** — nessun backend necessario
3. **La dashboard Streamlit resta attiva** — il frontend React è un'aggiunta, non una sostituzione
4. **shadcn/ui non è un pacchetto npm** — si copia i componenti nel progetto (zero dipendenze runtime)

---

## 🎯 Risultato Atteso

Un frontend moderno, veloce e visivamente impressionante che:
- Racconta la storia delle missioni italiane con dati interattivi
- Mostra le 38 missioni attive su una mappa animata
- Permette analisi approfondite con filtri e grafici
- Funziona perfettamente su mobile
- Si aggiorna automaticamente quando la pipeline rigenera i dati JSON
