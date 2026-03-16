# PROMPT — MIDA Frontend V4: Next Level

## Contesto
Il frontend React attuale (v5.2) è un MVP istituzionale deployato su https://mida-missioni.netlify.app.
Stack: React 18 + TypeScript + Vite 5 + Tailwind CSS 3 + Recharts + Leaflet + Framer Motion.
Dati: 3 file JSON statici (`missions.json`, `active.json`, `stats.json`) in `public/data/`.
19 file sorgente, 5 pagine, 7 componenti. Build: 0 errori TS. Deploy: Netlify gratuito.

Il frontend funziona ma è ancora un MVP: manca responsive mobile, i grafici sono basici,
la UX è lineare senza interazioni avanzate, non c'è ricerca globale, non c'è export PDF,
non ci sono test E2E, e il codice non è ottimizzato (bundle 872KB, nessun code splitting).

## Obiettivo
Portare il frontend da MVP 9/10 a prodotto professionale 10/10.
Il risultato finale deve sembrare un portale istituzionale del Ministero della Difesa,
non un progetto universitario. Deve essere veloce, accessibile, stampabile, e mobile-first.

---

## 1. ARCHITETTURA E PERFORMANCE

### 1.1 Code Splitting
- Lazy load di ogni pagina con `React.lazy()` + `Suspense`
- Separare Leaflet e Recharts in chunk dedicati via `manualChunks` in `vite.config.ts`
- Target: bundle iniziale < 200KB gzipped (attuale: 255KB)

### 1.2 Prefetch dati
- Creare un `DataProvider` context che carica i 3 JSON una sola volta all'avvio
- Eliminare il fetch duplicato in ogni pagina (attualmente `useMissions` fetcha 3 volte per pagina)
- Aggiungere `stale-while-revalidate` caching con timestamp

### 1.3 SEO e Meta
- Aggiungere `react-helmet-async` per meta tag dinamici per pagina
- Open Graph tags per condivisione social (titolo, descrizione, immagine preview)
- `robots.txt` e `sitemap.xml` statici in `public/`

---

## 2. RESPONSIVE E MOBILE

### 2.1 Breakpoint strategy
- Mobile first: 320px → 768px → 1024px → 1440px
- Navbar: hamburger menu su mobile con slide-in panel
- KPI strip: 2 colonne su mobile (non 5), scroll orizzontale opzionale
- Tabelle: card layout su mobile (< 768px), tabella su desktop
- Mappa: full screen su mobile con bottom sheet per dettagli
- Timeline: scroll orizzontale con touch gesture su mobile

### 2.2 Touch
- Swipe tra pagine su mobile (opzionale)
- Tap su marker mappa → bottom sheet con dettaglio missione
- Pull-to-refresh gesture (opzionale)

---

## 3. NUOVE FEATURE

### 3.1 Ricerca Globale (Command Palette)
- `Ctrl+K` / `Cmd+K` apre una command palette (come VS Code)
- Cerca tra missioni, paesi, organizzazioni
- Risultati raggruppati per tipo con navigazione diretta
- Implementare con un semplice modal + fuzzy search (no dipendenze esterne)
- Keyboard navigation: frecce + Enter

### 3.2 Export PDF
- Bottone "Esporta PDF" nella HomePage e DashboardPage
- Usa `html2canvas` + `jsPDF` per generare un PDF A4 landscape
- Header: emblema Repubblica + "MIDA — Quadro Situazione [mese anno]"
- Contenuto: KPI strip + tabella missioni attive + mappa (screenshot) + grafici
- Footer: "Fonte: Ministero della Difesa — Elaborazione MIDA"
- Il PDF deve sembrare un documento ufficiale stampabile

### 3.3 Confronto Temporale
- Nella DashboardPage, aggiungere un selettore "Confronta con anno X"
- Mostra delta (▲ +5 missioni, ▼ -200 personale) accanto ai KPI
- Calcolo basato sui dati esistenti (data_inizio/data_fine)

### 3.4 Dettaglio Missione (pagina dedicata)
- Route: `/missions/:nome` (URL-encoded)
- Contenuto: tutte le info della missione, mini-mappa centrata sul paese, timeline della singola missione, link a fonte ufficiale
- Breadcrumb: Home > Registro > Nome Missione
- Accessibile dal drawer della MissionsPage e dalla tabella HomePage

### 3.5 Pagina "Informazioni"
- Route: `/about`
- Contenuto: metodologia, fonti dati, pipeline, contatti, licenza
- Link a GitHub repo
- Crediti: Università di Catania, Ministero della Difesa

---

## 4. UI/UX AVANZATA

### 4.1 Micro-interazioni (sobrie)
- Numeri KPI: contatore animato (già presente, mantenere)
- Pagine: fade-in 200ms (già presente, mantenere)
- Tabelle: highlight riga on hover con transizione 100ms
- Grafici: tooltip segue il mouse con transizione fluida
- NO: bounce, slide, scale, rotate, glow, pulse

### 4.2 Accessibilità (WCAG 2.1 AA)
- Tutti i colori devono avere contrasto ≥ 4.5:1
- Focus ring visibile su tutti gli elementi interattivi
- `aria-label` su bottoni icona, grafici, mappa
- Skip-to-content link nascosto
- Tabelle con `scope="col"` e `scope="row"`
- Mappa: fallback testuale per screen reader

### 4.3 Skeleton Loading
- Sostituire il testo "Caricamento dati..." con skeleton placeholder
- Rettangoli grigi animati (pulse) che mimano il layout finale
- Uno skeleton per ogni sezione: KPI strip, tabella, mappa, grafici

### 4.4 Empty States
- Se un filtro non produce risultati: illustrazione minimale + messaggio
- Se i dati non si caricano: messaggio di errore con retry button

### 4.5 Breadcrumb
- Sotto la navbar, breadcrumb testuale: Home > Pagina corrente
- Font 10px, uppercase, tracking wide, colore muted

---

## 5. GRAFICI AVANZATI

### 5.1 OrgDonut
- Aggiungere label percentuale dentro ogni fetta (se > 5%)
- Centro: numero totale missioni
- Click su fetta → filtra la tabella sottostante

### 5.2 RegionBar
- Ordinare per valore decrescente (non alfabetico)
- Aggiungere valore numerico alla fine di ogni barra
- Click su barra → filtra

### 5.3 Area Chart (HomePage)
- Aggiungere brush (range selector) sotto il grafico per zoom temporale
- Tooltip: mostra anno + numero missioni + evento storico se presente
- Linea verticale tratteggiata per ogni evento storico (1991, 1999, 2001, etc.)

### 5.4 Nuovo: Sankey Diagram (DashboardPage)
- Flusso: Organizzazione → Regione → Stato (attiva/conclusa)
- Mostra come le missioni si distribuiscono
- Usa `recharts` Sankey o implementa con SVG custom

### 5.5 Nuovo: Heatmap Calendario (TimelinePage)
- Griglia anno × mese con intensità colore = numero missioni attive
- Ispirato a GitHub contribution graph
- Hover: tooltip con dettaglio

---

## 6. MAPPA AVANZATA

### 6.1 Cluster markers
- Quando ci sono più missioni nello stesso paese, raggruppare in cluster
- Cluster mostra numero missioni, click → zoom + espandi

### 6.2 Filtri mappa
- Dropdown sopra la mappa: filtra per organizzazione
- Toggle: mostra/nascondi linee Roma→missione
- Toggle: mostra/nascondi missioni concluse (marker grigio semitrasparente)

### 6.3 Popup migliorati
- Click su marker → popup con: nome missione, paese, org, personale, data inizio
- Link "Vedi dettaglio" → pagina missione dedicata
- Stile popup: bianco, bordo sottile, font 11px, no ombra eccessiva

### 6.4 Layer switcher
- Base layers: Dark (attuale), Light (CartoDB Positron), Satellite (Esri)
- Bottone discreto in alto a destra

---

## 7. TEST

### 7.1 Unit test
- Vitest + React Testing Library
- Test per ogni hook (`useMissions`, `useAnimatedCounter`)
- Test per KpiCard, Navbar (rendering, props)
- Target: 80% coverage su hooks e utils

### 7.2 E2E test
- Playwright
- Test: homepage carica, KPI visibili, tabella ha righe, mappa renderizza
- Test: navigazione tra pagine funziona
- Test: filtri dashboard funzionano
- Test: export CSV funziona
- Eseguire in CI (GitHub Actions)

---

## 8. PULIZIA CODICE

### 8.1 Rimuovere dead code
- `COUNTRY_FLAGS` in `constants.ts` — non più usato (emoji rimosse in V3)
- `MILITARY` object — usato solo come reference, i colori sono hardcoded nelle pagine
- Unificare: o si usano le costanti ovunque, o si rimuovono

### 8.2 Estrarre componenti riutilizzabili
- `SectionTitle` — h2 con stile istituzionale (usato 10+ volte con classi duplicate)
- `DataTable` — tabella generica con sorting, filtering, pagination
- `StatCard` — versione generica di KpiCard con varianti
- `FilterBar` — barra filtri inline riutilizzabile (usata in Dashboard e Missions)
- `PageLayout` — wrapper con max-w-7xl, padding, fade-in

### 8.3 Costanti colore
- Creare un file `theme.ts` con tutti i colori usati nel frontend
- Sostituire tutti i `#1B3A5C`, `#4A5D23`, etc. hardcoded con riferimenti a theme
- Preparare per dark mode futuro

---

## 9. DARK MODE (opzionale)

- Toggle in navbar (icona sole/luna, unica eccezione alla regola "no icone")
- Persistenza in localStorage
- Colori dark: bg #0F1419, card #1A2332, text #E8E6E3, border #2D3748
- Mappa: già dark, nessun cambio
- Grafici: invertire colori asse e tooltip

---

## VINCOLI TECNICI

- **NO nuove dipendenze** eccetto: `html2canvas`, `jspdf`, `react-helmet-async`, `vitest`, `@playwright/test`
- **NO backend** — tutto client-side con JSON statici
- **NO emoji** in nessun punto della UI
- **NO icone decorative** — solo navbar toggle mobile, bottoni azione, e dark mode toggle
- **Build**: `npx tsc --noEmit` = 0 errori, `npm run build` = successo
- **Bundle**: < 300KB gzipped totale (con code splitting)
- **Lighthouse**: Performance ≥ 90, Accessibility ≥ 95, Best Practices ≥ 90
- **Deploy**: `npx netlify-cli deploy --prod --dir=dist` dopo ogni modifica

---

## FILE DA CREARE

```
frontend/src/context/DataProvider.tsx          — context per dati centralizzati
frontend/src/components/ui/SectionTitle.tsx     — titolo sezione riutilizzabile
frontend/src/components/ui/DataTable.tsx        — tabella generica
frontend/src/components/ui/FilterBar.tsx        — barra filtri inline
frontend/src/components/ui/Skeleton.tsx         — skeleton loading
frontend/src/components/ui/CommandPalette.tsx   — ricerca globale Ctrl+K
frontend/src/components/ui/Breadcrumb.tsx       — breadcrumb navigazione
frontend/src/components/ui/EmptyState.tsx       — stato vuoto
frontend/src/components/export/PdfExport.tsx    — generatore PDF
frontend/src/pages/MissionDetailPage.tsx        — dettaglio singola missione
frontend/src/pages/AboutPage.tsx                — pagina informazioni
frontend/src/lib/theme.ts                       — costanti colore centralizzate
frontend/src/__tests__/                         — cartella test
frontend/e2e/                                   — cartella test E2E Playwright
frontend/public/robots.txt                      — SEO
frontend/public/sitemap.xml                     — SEO
```

## FILE DA MODIFICARE

```
frontend/src/App.tsx                — lazy loading, nuove route, DataProvider
frontend/src/main.tsx               — HelmetProvider wrapper
frontend/src/index.css              — dark mode variables, skeleton animations
frontend/src/lib/constants.ts       — rimuovere COUNTRY_FLAGS, pulire MILITARY
frontend/src/lib/theme.ts           — nuovo file colori centralizzati
frontend/src/components/layout/Navbar.tsx — hamburger mobile, dark mode toggle, breadcrumb
frontend/src/components/cards/KpiCard.tsx — skeleton variant, delta comparison
frontend/src/components/charts/*.tsx — click-to-filter, label migliorate
frontend/src/pages/HomePage.tsx     — skeleton, PDF export, breadcrumb
frontend/src/pages/DashboardPage.tsx — Sankey, confronto temporale, skeleton
frontend/src/pages/MissionsPage.tsx — link a dettaglio, mobile card layout
frontend/src/pages/TimelinePage.tsx — heatmap, mobile scroll
frontend/src/pages/MapPage.tsx      — cluster, filtri, layer switcher, popup migliorati
frontend/vite.config.ts            — manualChunks per code splitting
frontend/package.json              — nuove dipendenze dev + prod
```

## ORDINE DI ESECUZIONE

1. Pulizia codice (theme.ts, rimuovere dead code, estrarre componenti)
2. DataProvider context + code splitting
3. Responsive mobile (navbar hamburger, tabelle→card, mappa full screen)
4. Skeleton loading + empty states
5. Ricerca globale (Ctrl+K)
6. Dettaglio missione + About page
7. Grafici avanzati (brush, click-to-filter, Sankey, heatmap)
8. Mappa avanzata (cluster, filtri, layer switcher)
9. Export PDF
10. Accessibilità (WCAG 2.1 AA)
11. SEO (meta, OG, robots, sitemap)
12. Test (Vitest + Playwright)
13. Dark mode (opzionale, ultimo)
14. Build finale + Lighthouse audit + deploy

## TEST FINALE

Checklist prima del deploy:
- [ ] `npx tsc --noEmit` = 0 errori
- [ ] `npm run build` = successo
- [ ] Bundle < 300KB gzipped
- [ ] Lighthouse Performance ≥ 90
- [ ] Lighthouse Accessibility ≥ 95
- [ ] Tutte le pagine funzionano su mobile 375px
- [ ] PDF export genera documento leggibile
- [ ] Ctrl+K apre ricerca e trova missioni
- [ ] Mappa cluster funziona
- [ ] Test Vitest passing
- [ ] Test Playwright passing
- [ ] Deploy Netlify OK
