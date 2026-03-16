import { useMemo, useState, useCallback, useRef, useEffect } from 'react'
import { useData } from '../context/DataProvider'
import { GEOCODING, ROMA, ORG_COLORS, HISTORICAL_EVENTS } from '../lib/constants'
import type { Mission } from '../lib/types'
import DeckGL from '@deck.gl/react'
import { _GlobeView as GlobeView } from '@deck.gl/core'
import { ScatterplotLayer, ArcLayer, TextLayer } from '@deck.gl/layers'
import { TileLayer } from '@deck.gl/geo-layers'
import { BitmapLayer } from '@deck.gl/layers'

/* ─── Helpers ─── */
function fmtNum(n: number) { return Math.round(n).toLocaleString('it-IT') }
function fmtCoord(v: number, pos: string, neg: string) {
  const d = Math.abs(v)
  const deg = Math.floor(d)
  const min = Math.floor((d - deg) * 60)
  const sec = ((d - deg - min / 60) * 3600).toFixed(1)
  return `${deg}°${String(min).padStart(2, '0')}'${sec}"${v >= 0 ? pos : neg}`
}
function getMissionYear(d: string | null, fallback: number): number {
  if (!d || d === 'NaT') return fallback
  const y = new Date(d).getFullYear()
  return isNaN(y) ? fallback : y
}
function hexToRgb(hex: string): [number, number, number] {
  const r = parseInt(hex.slice(1, 3), 16)
  const g = parseInt(hex.slice(3, 5), 16)
  const b = parseInt(hex.slice(5, 7), 16)
  return [r, g, b]
}

/* ─── Tile URLs ─── */
const TILE_URLS: Record<string, string> = {
  dark:      'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
  light:     'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
  satellite: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
}

/* ─── SIGACT ─── */
function buildSigacts(missions: Mission[]) {
  const events: { date: string; type: 'START' | 'END'; name: string; paese: string; org: string; ts: number }[] = []
  missions.forEach(m => {
    if (m.data_inizio && m.data_inizio !== 'NaT') {
      const ts = new Date(m.data_inizio).getTime()
      if (!isNaN(ts)) events.push({ date: m.data_inizio.slice(0, 10), type: 'START', name: m.nome, paese: m.paese, org: m.tipo_missione, ts })
    }
    if (m.data_fine && m.data_fine !== 'NaT') {
      const ts = new Date(m.data_fine).getTime()
      if (!isNaN(ts)) events.push({ date: m.data_fine.slice(0, 10), type: 'END', name: m.nome, paese: m.paese, org: m.tipo_missione, ts })
    }
  })
  return events.sort((a, b) => b.ts - a.ts).slice(0, 30).map(({ ts: _, ...rest }) => rest)
}

/* ─── Types for deck data ─── */
type TheaterPoint = { paese: string; position: [number, number]; pers: number; mil: number; civ: number; missions: Mission[]; color: [number, number, number]; org: string }
type ArcDatum = { source: [number, number]; target: [number, number]; color: [number, number, number]; pers: number }

const INITIAL_VIEW = { latitude: 30, longitude: 20, zoom: 1.8, bearing: 0, pitch: 25 }
const GLOBE_VIEW = new GlobeView({ id: 'globe', resolution: 10 })

export default function MapPage() {
  const { missions, active, loading } = useData()
  const [panelOpen, setPanelOpen] = useState(true)
  const [orgFilter, setOrgFilter] = useState('')
  const [mode, setMode] = useState<'live' | 'temporal'>('live')
  const [selectedYear, setSelectedYear] = useState(2026)
  const [playing, setPlaying] = useState(false)
  const [speed, setSpeed] = useState(200)
  const playRef = useRef(false)
  const [tileKey, setTileKey] = useState<'dark' | 'light' | 'satellite'>('dark')
  const [showLines, setShowLines] = useState(true)
  const [showSigact, setShowSigact] = useState(false)
  const [viewState, setViewState] = useState(INITIAL_VIEW)
  const [tooltip, setTooltip] = useState<{ x: number; y: number; data: TheaterPoint } | null>(null)

  const missionsAtYear = useCallback((year: number) => {
    return missions.filter(m => {
      const start = getMissionYear(m.data_inizio, 9999)
      const end = getMissionYear(m.data_fine, 2026)
      return start <= year && end >= year
    })
  }, [missions])

  const displayMissions = useMemo(() => {
    const base = mode === 'live' ? active : missionsAtYear(selectedYear)
    return orgFilter ? base.filter(m => m.tipo_missione === orgFilter) : base
  }, [mode, active, missionsAtYear, selectedYear, orgFilter])

  const byRegion = useMemo(() => {
    const groups: Record<string, Mission[]> = {}
    displayMissions.forEach(m => {
      const r = m.regione || 'Altro'
      if (!groups[r]) groups[r] = []
      groups[r].push(m)
    })
    return Object.entries(groups).sort((a, b) => b[1].length - a[1].length)
  }, [displayMissions])

  const totalPers = useMemo(() => displayMissions.reduce((s, m) => s + (m.personale_totale || 0), 0), [displayMissions])
  const totalMil = useMemo(() => displayMissions.reduce((s, m) => s + (m.personale_militare || 0), 0), [displayMissions])
  const totalCiv = useMemo(() => displayMissions.reduce((s, m) => s + (m.personale_civile || 0), 0), [displayMissions])
  const orgs = [...new Set(missions.map(m => m.tipo_missione))].sort()
  const currentEvent = useMemo(() => HISTORICAL_EVENTS.find(e => e.year === selectedYear), [selectedYear])
  const sigacts = useMemo(() => buildSigacts(missions), [missions])
  const countries = useMemo(() => new Set(displayMissions.map(m => m.paese)), [displayMissions])

  // Aggregate by country for deck.gl layers
  const theaterData = useMemo<TheaterPoint[]>(() => {
    const byCountry: Record<string, Mission[]> = {}
    displayMissions.forEach(m => {
      const key = m.paese || 'Unknown'
      if (!byCountry[key]) byCountry[key] = []
      byCountry[key].push(m)
    })
    return Object.entries(byCountry).map(([paese, mList]) => {
      const coords = GEOCODING[paese]
      if (!coords) return null
      const pers = mList.reduce((s, m) => s + (m.personale_totale || 0), 0)
      const mil = mList.reduce((s, m) => s + (m.personale_militare || 0), 0)
      const civ = mList.reduce((s, m) => s + (m.personale_civile || 0), 0)
      const mainOrg = [...mList].sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0))[0].tipo_missione
      const color = hexToRgb(ORG_COLORS[mainOrg] || '#5A5F63')
      return { paese, position: [coords[1], coords[0]] as [number, number], pers, mil, civ, missions: mList, color, org: mainOrg }
    }).filter(Boolean) as TheaterPoint[]
  }, [displayMissions])

  const arcData = useMemo<ArcDatum[]>(() => {
    if (!showLines) return []
    return theaterData.map(t => ({
      source: [ROMA[1], ROMA[0]] as [number, number],
      target: t.position,
      color: t.color,
      pers: t.pers,
    }))
  }, [theaterData, showLines])

  // Build deck.gl layers
  const layers = useMemo(() => {
    const result = []

    // Base map tiles
    result.push(new TileLayer({
      id: 'base-tiles',
      data: TILE_URLS[tileKey],
      minZoom: 0,
      maxZoom: 19,
      tileSize: 256,
      renderSubLayers: (props: Record<string, unknown>) => {
        const { boundingBox } = props.tile as { boundingBox: [[number, number], [number, number]] }
        return new BitmapLayer({
          ...props,
          data: undefined,
          image: props.data as string,
          bounds: [boundingBox[0][0], boundingBox[0][1], boundingBox[1][0], boundingBox[1][1]],
        })
      },
    }))

    // 3D Arcs: Roma → theaters
    if (showLines) {
      result.push(new ArcLayer<ArcDatum>({
        id: 'arcs-c2',
        data: arcData,
        getSourcePosition: (d: ArcDatum) => d.source,
        getTargetPosition: (d: ArcDatum) => d.target,
        getSourceColor: [107, 140, 42, 180],
        getTargetColor: (d: ArcDatum) => [...d.color, 200] as [number, number, number, number],
        getWidth: (d: ArcDatum) => Math.max(1, Math.min(4, Math.sqrt(d.pers) / 8)),
        greatCircle: true,
        numSegments: 50,
        getHeight: 0.3,
        pickable: false,
      }))
    }

    // Roma COI marker (always visible)
    result.push(new ScatterplotLayer({
      id: 'roma-coi',
      data: [{ position: [ROMA[1], ROMA[0]], size: 12000 }],
      getPosition: (d: { position: [number, number] }) => d.position,
      getRadius: (d: { size: number }) => d.size,
      getFillColor: [139, 26, 26, 220],
      getLineColor: [255, 255, 255, 200],
      lineWidthMinPixels: 2,
      stroked: true,
      radiusUnits: 'meters',
      radiusMinPixels: 6,
      radiusMaxPixels: 14,
      pickable: false,
    }))

    // Theater markers
    result.push(new ScatterplotLayer<TheaterPoint>({
      id: 'theater-markers',
      data: theaterData,
      getPosition: (d: TheaterPoint) => d.position,
      getRadius: (d: TheaterPoint) => Math.max(8000, Math.min(60000, Math.sqrt(d.pers) * 600)),
      getFillColor: (d: TheaterPoint) => [...d.color, 190] as [number, number, number, number],
      getLineColor: (d: TheaterPoint) => [...d.color, 255] as [number, number, number, number],
      lineWidthMinPixels: 1.5,
      stroked: true,
      radiusUnits: 'meters',
      radiusMinPixels: 4,
      radiusMaxPixels: 24,
      pickable: true,
      autoHighlight: true,
      highlightColor: [107, 140, 42, 100],
      onHover: (info: { object?: TheaterPoint; x: number; y: number }) => {
        if (info.object) setTooltip({ x: info.x, y: info.y, data: info.object })
        else setTooltip(null)
      },
    }))

    // Country labels for larger theaters
    result.push(new TextLayer<TheaterPoint>({
      id: 'theater-labels',
      data: theaterData.filter(t => t.pers > 50),
      getPosition: (d: TheaterPoint) => d.position,
      getText: (d: TheaterPoint) => d.paese.toUpperCase(),
      getSize: 10,
      getColor: [245, 243, 238, 180],
      getAngle: 0,
      getTextAnchor: 'start',
      getAlignmentBaseline: 'center',
      getPixelOffset: [14, 0],
      fontFamily: 'monospace',
      fontWeight: '700',
      outlineWidth: 3,
      outlineColor: [15, 20, 25, 200],
      billboard: true,
      sizeUnits: 'pixels',
      sizeMinPixels: 8,
      sizeMaxPixels: 12,
      pickable: false,
    }))

    return result
  }, [tileKey, arcData, theaterData, showLines])

  // Play animation
  useEffect(() => {
    playRef.current = playing
    if (!playing) return
    let year = selectedYear
    const tick = () => {
      if (!playRef.current) return
      year++
      if (year > 2026) year = 1948
      setSelectedYear(year)
      setTimeout(tick, speed)
    }
    const t = setTimeout(tick, speed)
    return () => clearTimeout(t)
  }, [playing, speed])

  if (loading) return (
    <div className="flex items-center justify-center bg-[#0F1419]" style={{ height: 'calc(100vh - 48px)' }}>
      <div className="text-center">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-[#4A5D23] border-t-transparent mx-auto" />
        <p className="mt-3 text-[10px] text-[#8B9298] uppercase tracking-[0.2em] font-mono">Inizializzazione GEOINT 3D...</p>
      </div>
    </div>
  )

  return (
    <div className="relative select-none" style={{ height: 'calc(100vh - 48px)', background: '#0a0e13' }}>
      {/* 3D Globe */}
      <DeckGL
        views={GLOBE_VIEW}
        viewState={viewState}
        onViewStateChange={({ viewState: vs }) => setViewState(vs as unknown as typeof viewState)}
        layers={layers}
        controller={{ dragRotate: true, scrollZoom: true, touchRotate: true, keyboard: true }}
        getCursor={({ isHovering }: { isHovering: boolean }) => isHovering ? 'pointer' : 'grab'}
        style={{ position: 'absolute', inset: '0' }}
      />

      {/* Tooltip overlay */}
      {tooltip && (
        <div className="pointer-events-none absolute z-[1001]" style={{ left: tooltip.x + 12, top: tooltip.y - 12 }}>
          <div className="bg-[#0F1419]/95 backdrop-blur-sm border border-[#3D4F1E]/60 rounded-lg p-3 text-white min-w-[200px] max-w-[280px]">
            <div className="text-[7px] font-mono uppercase tracking-[0.25em] text-[#6B8C2A] mb-1">GEOINT — Teatro Operativo</div>
            <div className="text-[13px] font-bold">{tooltip.data.paese}</div>
            <div className="text-[9px] text-[#D4CFC3] mt-0.5 font-mono">{tooltip.data.missions.length} missioni · {fmtNum(tooltip.data.pers)} unita</div>
            <div className="flex gap-3 mt-2 text-[8px] font-mono">
              <span className="text-[#4A5D23]">MIL {fmtNum(tooltip.data.mil)}</span>
              <span className="text-[#2C5F8A]">CIV {fmtNum(tooltip.data.civ)}</span>
            </div>
            <div className="mt-2 border-t border-[#3D4F1E]/30 pt-1.5">
              {tooltip.data.missions.slice(0, 5).map(m => (
                <div key={m.nome} className="flex items-center gap-1.5 py-0.5">
                  <div className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: ORG_COLORS[m.tipo_missione] || '#999' }} />
                  <span className="text-[8px] truncate">{m.nome}</span>
                  <span className="text-[7px] text-[#8B9298] font-mono ml-auto flex-shrink-0">{m.personale_totale ? fmtNum(m.personale_totale) : ''}</span>
                </div>
              ))}
              {tooltip.data.missions.length > 5 && (
                <div className="text-[7px] text-[#8B9298] font-mono mt-0.5">+{tooltip.data.missions.length - 5} altre</div>
              )}
            </div>
            <div className="mt-1.5 text-[7px] font-mono text-[#8B9298]">
              {fmtCoord(tooltip.data.position[1], 'N', 'S')} {fmtCoord(tooltip.data.position[0], 'E', 'W')}
            </div>
          </div>
        </div>
      )}

      {/* HUD corner brackets */}
      <div className="hidden md:block pointer-events-none absolute inset-0 z-[999]">
        <div className="absolute top-14 left-2 w-6 h-6 border-l-2 border-t-2 border-[#4A5D23]/40" />
        <div className="absolute top-14 right-2 w-6 h-6 border-r-2 border-t-2 border-[#4A5D23]/40" />
        <div className="absolute bottom-2 left-2 w-6 h-6 border-l-2 border-b-2 border-[#4A5D23]/40" />
        <div className="absolute bottom-2 right-2 w-6 h-6 border-r-2 border-b-2 border-[#4A5D23]/40" />
      </div>

      {/* Classification banner */}
      <div className="hidden md:flex absolute bottom-1 left-1/2 -translate-x-1/2 z-[999] pointer-events-none">
        <span className="text-[7px] font-mono uppercase tracking-[0.3em] text-[#4A5D23]/50">Non Classificato — OSINT — Fonti Aperte</span>
      </div>

      {/* C2 GEOINT top bar */}
      <div className="absolute top-0 left-0 right-0 z-[1000] bg-[#0F1419]/90 backdrop-blur-sm border-b border-[#3D4F1E]/40 px-3 py-1.5">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-3 min-w-0">
            <div className={`w-2 h-2 rounded-full flex-shrink-0 ${mode === 'live' ? 'bg-[#4A5D23] animate-pulse' : 'bg-[#7D6B3A]'}`} />
            <div className="min-w-0">
              <p className="text-[10px] font-bold uppercase tracking-[0.15em] text-white truncate font-mono">
                {mode === 'live' ? `GEOINT 3D — ${displayMissions.length} Missioni Attive` : `GEOINT ${selectedYear} — ${displayMissions.length} Missioni`}
              </p>
              <p className="text-[8px] text-[#8B9298] font-mono">
                {fmtNum(totalPers)} UNITA · MIL {fmtNum(totalMil)} · CIV {fmtNum(totalCiv)} · {countries.size} PAESI
              </p>
            </div>
          </div>
          <div className="flex items-center gap-1.5 flex-shrink-0">
            <div className="flex bg-[#1A2332] rounded overflow-hidden border border-[#3D4F1E]/50">
              <button onClick={() => { setMode('live'); setPlaying(false) }} className={`px-2 py-1 text-[7px] uppercase tracking-[0.1em] font-bold font-mono transition-colors ${mode === 'live' ? 'bg-[#4A5D23] text-white' : 'text-[#8B9298] hover:text-white'}`}>Live</button>
              <button onClick={() => { setMode('temporal'); setSelectedYear(2026) }} className={`px-2 py-1 text-[7px] uppercase tracking-[0.1em] font-bold font-mono transition-colors ${mode === 'temporal' ? 'bg-[#7D6B3A] text-white' : 'text-[#8B9298] hover:text-white'}`}>Temporale</button>
            </div>
            <select value={orgFilter} onChange={e => setOrgFilter(e.target.value)} className="bg-[#1A2332] border border-[#3D4F1E]/50 text-[8px] text-white rounded px-1.5 py-1 outline-none font-mono">
              <option value="">TUTTE ORG.</option>
              {orgs.map(o => <option key={o} value={o}>{o}</option>)}
            </select>
          </div>
        </div>
        <div className="flex items-center justify-between mt-1 gap-2">
          <div className="flex items-center gap-2">
            <button onClick={() => setShowLines(!showLines)} className={`text-[7px] font-mono uppercase tracking-wider px-1.5 py-0.5 rounded border transition-colors ${showLines ? 'border-[#6B8C2A]/60 text-[#6B8C2A] bg-[#6B8C2A]/10' : 'border-[#3D4F1E]/30 text-[#5A5F63]'}`}>Archi C2</button>
            <button onClick={() => setShowSigact(!showSigact)} className={`text-[7px] font-mono uppercase tracking-wider px-1.5 py-0.5 rounded border transition-colors ${showSigact ? 'border-[#7D6B3A]/60 text-[#7D6B3A] bg-[#7D6B3A]/10' : 'border-[#3D4F1E]/30 text-[#5A5F63]'}`}>SIGACT</button>
            <button onClick={() => setPanelOpen(!panelOpen)} className={`text-[7px] font-mono uppercase tracking-wider px-1.5 py-0.5 rounded border transition-colors ${panelOpen ? 'border-[#2C5F8A]/60 text-[#2C5F8A] bg-[#2C5F8A]/10' : 'border-[#3D4F1E]/30 text-[#5A5F63]'}`}>Intel</button>
          </div>
          <div className="flex items-center gap-1">
            {(Object.keys(TILE_URLS) as Array<'dark'|'light'|'satellite'>).map(k => (
              <button key={k} onClick={() => setTileKey(k)} className={`text-[7px] font-mono uppercase tracking-wider px-1.5 py-0.5 rounded border transition-colors ${tileKey === k ? 'border-[#8B9298]/60 text-white bg-[#8B9298]/15' : 'border-[#3D4F1E]/30 text-[#5A5F63]'}`}>
                {k === 'dark' ? 'DRK' : k === 'light' ? 'LGT' : 'SAT'}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* SIGACT Feed */}
      {showSigact && (
        <div className="hidden md:block absolute top-[72px] right-3 z-[1000] w-64 max-h-[calc(100vh-160px)] overflow-y-auto bg-[#0F1419]/92 backdrop-blur-sm rounded-lg border border-[#3D4F1E]/40 text-white">
          <div className="px-3 py-2 border-b border-[#3D4F1E]/40 flex items-center justify-between">
            <p className="text-[8px] font-mono uppercase tracking-[0.2em] text-[#7D6B3A] font-bold">SIGACT — Eventi Missioni</p>
            <div className="w-1.5 h-1.5 rounded-full bg-[#7D6B3A] animate-pulse" />
          </div>
          {sigacts.map((s, i) => (
            <div key={i} className="px-3 py-1.5 border-b border-[#3D4F1E]/15 hover:bg-[#3D4F1E]/10 transition-colors">
              <div className="flex items-center gap-2">
                <span className={`text-[7px] font-mono font-bold px-1 py-0.5 rounded ${s.type === 'START' ? 'bg-[#4A5D23]/20 text-[#6B8C2A]' : 'bg-[#8B1A1A]/20 text-[#8B1A1A]'}`}>
                  {s.type === 'START' ? 'INIZIO' : 'FINE'}
                </span>
                <span className="text-[7px] font-mono text-[#8B9298]">{s.date}</span>
              </div>
              <p className="text-[9px] font-semibold truncate mt-0.5">{s.name}</p>
              <p className="text-[7px] text-[#8B9298] font-mono">{s.paese} · {s.org}</p>
            </div>
          ))}
        </div>
      )}

      {/* TEMPORAL TIMELINE BAR */}
      {mode === 'temporal' && (
        <div className="absolute bottom-0 left-0 right-0 z-[1000] bg-[#0F1419]/95 backdrop-blur-sm border-t border-[#3D4F1E]/40">
          <div className="flex items-center justify-between px-4 pt-2">
            <div className="flex items-center gap-3">
              <span className="text-[28px] md:text-[36px] font-mono font-bold text-white leading-none">{selectedYear}</span>
              <div>
                <p className="text-[11px] font-bold text-[#6B8C2A] font-mono">{displayMissions.length} missioni</p>
                <p className="text-[9px] text-[#8B9298] font-mono">{fmtNum(totalPers)} unita · {countries.size} paesi</p>
              </div>
            </div>
            {currentEvent && (
              <div className="hidden md:block text-right">
                <p className="text-[9px] font-bold uppercase tracking-[0.1em] text-[#8B1A1A] font-mono">{currentEvent.label}</p>
              </div>
            )}
            <div className="flex items-center gap-2">
              <select value={speed} onChange={e => setSpeed(+e.target.value)} className="bg-[#1A2332] border border-[#3D4F1E]/50 text-[8px] text-white rounded px-1.5 py-1 outline-none font-mono">
                <option value={500}>0.5x</option>
                <option value={200}>1x</option>
                <option value={100}>2x</option>
                <option value={50}>4x</option>
              </select>
              <button
                onClick={() => setPlaying(!playing)}
                className={`w-8 h-8 rounded-full flex items-center justify-center border-2 transition-colors ${playing ? 'border-[#8B1A1A] bg-[#8B1A1A]/20 text-[#8B1A1A]' : 'border-[#4A5D23] bg-[#4A5D23]/20 text-[#4A5D23]'}`}
              >
                {playing ? (
                  <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor"><rect x="2" y="1" width="3" height="10" rx="0.5"/><rect x="7" y="1" width="3" height="10" rx="0.5"/></svg>
                ) : (
                  <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor"><path d="M3 1.5v9l7-4.5z"/></svg>
                )}
              </button>
            </div>
          </div>
          <div className="px-4 pb-3 pt-1">
            <input
              type="range" min={1948} max={2026} value={selectedYear}
              onChange={e => { setSelectedYear(+e.target.value); setPlaying(false) }}
              className="w-full h-1.5 appearance-none bg-[#1A2332] rounded-full outline-none cursor-pointer"
              style={{ background: `linear-gradient(to right, #4A5D23 0%, #4A5D23 ${((selectedYear - 1948) / (2026 - 1948)) * 100}%, #1A2332 ${((selectedYear - 1948) / (2026 - 1948)) * 100}%, #1A2332 100%)` }}
            />
            <div className="flex justify-between mt-1">
              {[1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020].map(d => (
                <button key={d} onClick={() => { setSelectedYear(d); setPlaying(false) }} className="text-[7px] text-[#8B9298] hover:text-white font-mono transition-colors">{d}</button>
              ))}
            </div>
            <div className="relative h-2 mt-0.5">
              {HISTORICAL_EVENTS.filter(e => e.year >= 1948 && e.year <= 2026).map(e => (
                <button key={e.year} onClick={() => { setSelectedYear(e.year); setPlaying(false) }} title={`${e.year} — ${e.label}`}
                  className="absolute w-1 h-2 rounded-full bg-[#8B1A1A]/60 hover:bg-[#8B1A1A] transition-colors"
                  style={{ left: `${((e.year - 1948) / (2026 - 1948)) * 100}%` }} />
              ))}
            </div>
          </div>
          {currentEvent && (
            <div className="md:hidden px-4 pb-2 -mt-1">
              <p className="text-[8px] font-bold uppercase tracking-[0.1em] text-[#8B1A1A] font-mono">{currentEvent.label}</p>
            </div>
          )}
        </div>
      )}

      {/* Intelligence panel */}
      {panelOpen && (
        <div className={`absolute z-[1000] md:w-80 overflow-y-auto bg-[#0F1419]/92 backdrop-blur-sm md:rounded-lg border-t md:border border-[#3D4F1E]/40 text-white ${
          mode === 'temporal'
            ? 'hidden md:block md:top-[72px] md:left-3 md:bottom-auto md:right-auto md:max-h-[calc(100vh-220px)]'
            : 'bottom-0 left-0 right-0 md:bottom-auto md:right-auto md:top-[72px] md:left-3 max-h-[45vh] md:max-h-[calc(100vh-130px)]'
        }`}>
          {mode === 'live' && (
            <div className="md:hidden flex justify-center py-2">
              <div className="w-10 h-1 rounded-full bg-[#3D4F1E]" />
            </div>
          )}
          <div className="px-3 py-2 border-b border-[#3D4F1E]/40">
            <div className="flex items-center justify-between">
              <p className="text-[7px] font-mono uppercase tracking-[0.2em] text-[#6B8C2A] font-bold">
                {mode === 'live' ? 'Composizione Forze' : `Forze ${selectedYear}`}
              </p>
              <p className="text-[7px] text-[#8B9298] font-mono">{byRegion.length} TEATRI · {countries.size} AOR</p>
            </div>
            <div className="flex gap-2 mt-2">
              <div className="flex-1 bg-[#1A2332] rounded p-1.5">
                <p className="text-[14px] font-mono font-bold text-white leading-none">{fmtNum(totalMil)}</p>
                <p className="text-[6px] uppercase tracking-[0.1em] text-[#4A5D23] mt-0.5 font-mono">Militari</p>
              </div>
              <div className="flex-1 bg-[#1A2332] rounded p-1.5">
                <p className="text-[14px] font-mono font-bold text-white leading-none">{fmtNum(totalCiv)}</p>
                <p className="text-[6px] uppercase tracking-[0.1em] text-[#2C5F8A] mt-0.5 font-mono">Civili</p>
              </div>
              <div className="flex-1 bg-[#1A2332] rounded p-1.5">
                <p className="text-[14px] font-mono font-bold text-[#6B8C2A] leading-none">{fmtNum(totalPers)}</p>
                <p className="text-[6px] uppercase tracking-[0.1em] text-[#8B9298] mt-0.5 font-mono">Totale</p>
              </div>
            </div>
            <div className="flex h-1 rounded-full overflow-hidden mt-2">
              <div className="bg-[#4A5D23]" style={{ width: totalPers ? `${(totalMil / totalPers) * 100}%` : '0' }} />
              <div className="bg-[#2C5F8A]" style={{ width: totalPers ? `${(totalCiv / totalPers) * 100}%` : '0' }} />
            </div>
          </div>
          {byRegion.map(([region, mList]) => {
            const regMil = mList.reduce((s, m) => s + (m.personale_militare || 0), 0)
            const regCiv = mList.reduce((s, m) => s + (m.personale_civile || 0), 0)
            const regTot = mList.reduce((s, m) => s + (m.personale_totale || 0), 0)
            return (
              <div key={region} className="border-b border-[#3D4F1E]/25">
                <div className="px-3 py-1.5 bg-[#1A2332]/60 flex items-center justify-between">
                  <div className="flex items-center gap-1.5">
                    <span className="text-[8px] font-bold uppercase tracking-widest text-[#6B8C2A] font-mono">{region}</span>
                    <span className="text-[7px] text-[#8B9298] font-mono">({mList.length})</span>
                  </div>
                  <span className="text-[7px] font-mono text-[#8B9298]">{fmtNum(regTot)} · M{fmtNum(regMil)} C{fmtNum(regCiv)}</span>
                </div>
                {[...mList].sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0)).map(m => (
                  <div key={m.nome} className="px-3 py-1 flex items-center gap-2 hover:bg-[#3D4F1E]/15 transition-colors">
                    <div className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${mode === 'live' ? 'bg-[#4A5D23] animate-pulse' : 'bg-[#7D6B3A]'}`} />
                    <div className="min-w-0 flex-1">
                      <p className="text-[9px] font-semibold truncate">{m.nome}</p>
                      <div className="flex items-center gap-2 text-[7px] text-[#8B9298] font-mono">
                        <span>{m.paese}</span>
                        <span>{m.personale_totale ? fmtNum(m.personale_totale) : '—'}</span>
                        {m.personale_militare ? <span className="text-[#4A5D23]">M{fmtNum(m.personale_militare)}</span> : null}
                        {m.personale_civile ? <span className="text-[#2C5F8A]">C{fmtNum(m.personale_civile)}</span> : null}
                      </div>
                    </div>
                    <div className="w-1.5 h-5 rounded-full flex-shrink-0" style={{ backgroundColor: ORG_COLORS[m.tipo_missione] || '#999' }} />
                  </div>
                ))}
              </div>
            )
          })}
          <div className="p-3">
            <p className="text-[6px] uppercase tracking-widest text-[#8B9298] mb-1.5 font-mono">Organizzazioni</p>
            <div className="flex flex-wrap gap-x-3 gap-y-1">
              {Object.entries(ORG_COLORS).filter(([k]) => k !== 'Altro').map(([org, color]) => (
                <div key={org} className="flex items-center gap-1">
                  <div className="w-2 h-2 rounded-full" style={{ backgroundColor: color }} />
                  <span className="text-[7px] font-bold uppercase tracking-wider font-mono">{org}</span>
                </div>
              ))}
            </div>
            <div className="flex gap-3 mt-2 text-[6px] text-[#8B9298] uppercase tracking-wider font-mono">
              <span><span className="inline-block w-3 h-1 bg-[#4A5D23] rounded mr-1" />Militari</span>
              <span><span className="inline-block w-3 h-1 bg-[#2C5F8A] rounded mr-1" />Civili</span>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
