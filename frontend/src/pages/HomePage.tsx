import { useMemo, useEffect, useRef } from 'react'
import { motion } from 'framer-motion'
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts'
import KpiCard from '../components/cards/KpiCard'
import OrgDonut from '../components/charts/OrgDonut'
import RegionBar from '../components/charts/RegionBar'
import DecadeBar from '../components/charts/DecadeBar'
import { useData } from '../context/DataProvider'
import { ORG_COLORS, GEOCODING, ROMA, HISTORICAL_EVENTS } from '../lib/constants'
import { SkeletonKpiStrip, SkeletonTable, SkeletonChart, SkeletonMap } from '../components/ui/Skeleton'
import L from 'leaflet'

export default function HomePage() {
  const { missions, active, stats, loading } = useData()
  const miniMapRef = useRef<HTMLDivElement>(null)
  const miniMapInstance = useRef<L.Map | null>(null)

  // Personnel per year (for the troop strength chart)
  const personnelData = useMemo(() => {
    if (!missions.length) return []
    const years: Record<number, { missions: number; personnel: number }> = {}
    for (let y = 1948; y <= 2026; y++) years[y] = { missions: 0, personnel: 0 }
    missions.forEach(m => {
      if (!m.data_inizio) return
      const start = new Date(m.data_inizio).getFullYear()
      const end = m.data_fine && m.data_fine !== 'NaT' ? new Date(m.data_fine).getFullYear() : 2026
      const pers = m.personale_totale || 0
      for (let y = Math.max(start, 1948); y <= Math.min(end, 2026); y++) {
        years[y].missions++
        years[y].personnel += pers
      }
    })
    return Object.entries(years).map(([y, v]) => ({ year: +y, missioni: v.missions, personale: v.personnel })).sort((a, b) => a.year - b.year)
  }, [missions])

  // Find peak personnel year for annotation
  const peakYear = useMemo(() => {
    if (!personnelData.length) return null
    return personnelData.reduce((max, d) => d.personale > max.personale ? d : max, personnelData[0])
  }, [personnelData])

  const sortedActive = useMemo(() => [...active].sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0)), [active])

  // Org breakdown for active missions
  const activeByOrg = useMemo(() => {
    const c: Record<string, number> = {}
    active.forEach(m => { c[m.tipo_missione] = (c[m.tipo_missione] || 0) + 1 })
    return Object.entries(c).sort((a, b) => b[1] - a[1])
  }, [active])

  useEffect(() => {
    if (loading || !miniMapRef.current || miniMapInstance.current) return
    const map = L.map(miniMapRef.current, { zoomControl: false, attributionControl: false, dragging: false, scrollWheelZoom: false, doubleClickZoom: false }).setView([30, 20], 2)
    miniMapInstance.current = map
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', { maxZoom: 6 }).addTo(map)
    L.circleMarker(ROMA, { radius: 3, color: '#8B1A1A', fillColor: '#8B1A1A', fillOpacity: 1, weight: 0 }).addTo(map)
    const byCountry: Record<string, number> = {}
    active.forEach(m => { byCountry[m.paese] = (byCountry[m.paese] || 0) + (m.personale_totale || 0) })
    Object.entries(byCountry).forEach(([paese, pers]) => {
      const coords = GEOCODING[paese]
      if (!coords) return
      const r = Math.max(3, Math.min(10, Math.sqrt(pers) / 3))
      const mainOrg = active.filter(m => m.paese === paese).sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0))[0]?.tipo_missione
      L.circleMarker(coords, { radius: r, color: ORG_COLORS[mainOrg] || '#5A5F63', fillColor: ORG_COLORS[mainOrg] || '#5A5F63', fillOpacity: 0.7, weight: 1.5 }).addTo(map)
      L.polyline([ROMA, coords], { color: '#4A5D23', weight: 0.8, opacity: 0.15, dashArray: '4 6' }).addTo(map)
    })
    return () => { map.remove(); miniMapInstance.current = null }
  }, [active, loading])

  if (loading || !stats) {
    return (
      <div>
        <div className="bg-gradient-to-r from-[#1B3A5C] to-[#3D4F1E] px-4 py-6 md:py-10">
          <div className="max-w-7xl mx-auto">
            <div className="h-3 w-64 bg-white/10 rounded animate-pulse mb-2" />
            <div className="h-7 w-80 bg-white/15 rounded animate-pulse mb-2" />
            <div className="h-3 w-56 bg-white/10 rounded animate-pulse" />
          </div>
        </div>
        <div className="max-w-7xl mx-auto px-4 py-4 md:py-6 space-y-5 md:space-y-6">
          <SkeletonKpiStrip />
          <div className="grid grid-cols-1 lg:grid-cols-5 gap-5">
            <div className="lg:col-span-3"><SkeletonTable rows={8} /></div>
            <div className="lg:col-span-2"><SkeletonMap /></div>
          </div>
          <SkeletonChart />
        </div>
      </div>
    )
  }

  const currentPers = personnelData.length ? personnelData[personnelData.length - 1].personale : 0
  const peakPers = peakYear?.personale || 0
  const drawdownPct = peakPers > 0 ? Math.round(((peakPers - currentPers) / peakPers) * 100) : 0

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.2 }}>
      {/* HERO */}
      <div className="bg-gradient-to-r from-[#1B3A5C] to-[#3D4F1E] px-4 py-6 md:py-10">
        <div className="max-w-7xl mx-auto flex items-start justify-between">
          <div>
            <p className="text-[9px] md:text-[10px] uppercase tracking-[0.2em] text-[#8B9298]">Ministero della Difesa — Quadro Situazione 2026</p>
            <h1 className="text-xl md:text-3xl font-bold text-white tracking-tight mt-1 md:mt-2">
              Missioni Internazionali Italiane
            </h1>
            <p className="text-[11px] md:text-[13px] text-[#D4CFC3] mt-1 md:mt-2 max-w-lg">
              {stats.total} operazioni dal 1948 · {stats.active} in corso · {stats.personnel.toLocaleString('it-IT')} unità
            </p>
          </div>
          <img src="/emblema_repubblica.svg" alt="" className="w-8 h-8 md:w-10 md:h-10 opacity-60 hidden sm:block" />
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-4 md:py-6 space-y-5 md:space-y-6">
        {/* KPI STRIP — responsive grid */}
        <div className="bg-white border border-[#D4CFC3] rounded grid grid-cols-3 md:grid-cols-5 divide-y md:divide-y-0 md:divide-x divide-[#D4CFC3] -mt-6 md:-mt-8 relative z-10 shadow-sm">
          <KpiCard label="Totali" value={stats.total} />
          <KpiCard label="In Corso" value={stats.active} />
          <KpiCard label="Personale" value={stats.personnel} />
          <KpiCard label="Teatri" value={stats.countries} />
          <KpiCard label="Org." value={stats.organizations} />
        </div>

        {/* TWO COLUMNS: Table + Mini-map */}
        <div className="grid grid-cols-1 lg:grid-cols-5 gap-5">
          {/* Active missions table — card layout on mobile */}
          <div className="lg:col-span-3">
            <div className="flex items-center justify-between gap-2 border-b border-[#D4CFC3] pb-2 mb-3">
              <h2 className="text-[12px] md:text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] flex-shrink-0">
                In Corso — {active.length}
              </h2>
              <div className="flex gap-1 overflow-x-auto flex-shrink min-w-0 scrollbar-none">
                {activeByOrg.map(([org, n]) => (
                  <span key={org} className="text-[7px] md:text-[8px] font-bold uppercase px-1 md:px-1.5 py-0.5 rounded text-white whitespace-nowrap flex-shrink-0" style={{ backgroundColor: ORG_COLORS[org] || '#8B9298' }}>
                    {org} {n}
                  </span>
                ))}
              </div>
            </div>

            {/* Desktop table */}
            <div className="hidden md:block bg-white border border-[#D4CFC3] rounded overflow-hidden">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="bg-[#1B3A5C] text-white">
                    <th className="px-3 py-2 text-left text-[9px] uppercase tracking-[0.1em] font-semibold w-6"></th>
                    <th className="px-3 py-2 text-left text-[9px] uppercase tracking-[0.1em] font-semibold">Missione</th>
                    <th className="px-3 py-2 text-left text-[9px] uppercase tracking-[0.1em] font-semibold">Teatro</th>
                    <th className="px-3 py-2 text-left text-[9px] uppercase tracking-[0.1em] font-semibold">Org.</th>
                    <th className="px-3 py-2 text-right text-[9px] uppercase tracking-[0.1em] font-semibold">Pers.</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedActive.map((m, i) => (
                    <tr key={m.nome} className={`border-b border-[#EAE6DC] ${i % 2 ? 'bg-[#F5F3EE]' : ''} hover:bg-[#EAE6DC]/50 transition-colors`}>
                      <td className="px-3 py-1.5"><div className="w-1.5 h-1.5 rounded-full bg-[#4A5D23]" /></td>
                      <td className="px-3 py-1.5 font-medium text-[#1B3A5C]">{m.nome}</td>
                      <td className="px-3 py-1.5 text-[#5A5F63]">{m.paese}</td>
                      <td className="px-3 py-1.5 text-[#5A5F63]">{m.tipo_missione}</td>
                      <td className="px-3 py-1.5 text-right font-mono font-bold text-[#1B3A5C]">{m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Mobile card layout */}
            <div className="md:hidden space-y-2 max-h-[60vh] overflow-y-auto">
              {sortedActive.map(m => (
                <div key={m.nome} className="bg-white border border-[#D4CFC3] rounded p-3 flex items-center gap-3">
                  <div className="w-1.5 h-1.5 rounded-full bg-[#4A5D23] flex-shrink-0" />
                  <div className="flex-1 min-w-0">
                    <p className="text-[11px] font-bold text-[#1B3A5C] truncate">{m.nome}</p>
                    <p className="text-[9px] text-[#8B9298]">{m.paese} · {m.tipo_missione}</p>
                  </div>
                  <span className="text-[12px] font-mono font-bold text-[#1B3A5C] flex-shrink-0">
                    {m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') : '—'}
                  </span>
                </div>
              ))}
            </div>
          </div>

          {/* Mini-map */}
          <div className="lg:col-span-2">
            <h2 className="text-[13px] md:text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] pb-2 mb-3">
              Teatri Operativi
            </h2>
            <div ref={miniMapRef} className="w-full h-[220px] md:h-[400px] border border-[#D4CFC3] rounded" />
          </div>
        </div>

        {/* INTELLIGENCE BRIEFING: Troop Strength Trend */}
        <div className="bg-white border border-[#D4CFC3] rounded p-4">
          <div className="flex flex-col md:flex-row md:items-center md:justify-between mb-3 border-b border-[#D4CFC3] pb-2">
            <h2 className="text-[13px] md:text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C]">
              Impegno Operativo — Andamento Storico (1948–2026)
            </h2>
            {peakYear && (
              <div className="flex gap-3 mt-2 md:mt-0">
                <span className="text-[9px] uppercase tracking-[0.1em] text-[#8B9298]">
                  Picco: <b className="text-[#8B1A1A]">{peakYear.year}</b> ({peakYear.missioni} missioni)
                </span>
                <span className="text-[9px] uppercase tracking-[0.1em] text-[#8B9298]">
                  Riduzione: <b className="text-[#8B1A1A]">{drawdownPct}%</b> dal picco
                </span>
              </div>
            )}
          </div>
          <ResponsiveContainer width="100%" height={180}>
            <AreaChart data={personnelData} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#4A5D23" stopOpacity={0.2} />
                  <stop offset="95%" stopColor="#4A5D23" stopOpacity={0.01} />
                </linearGradient>
              </defs>
              <XAxis dataKey="year" tick={{ fontSize: 9 }} tickLine={false} axisLine={{ stroke: '#D4CFC3' }} interval={9} />
              <YAxis tick={{ fontSize: 9 }} tickLine={false} axisLine={false} width={28} />
              <Tooltip
                contentStyle={{ fontSize: 11, borderRadius: 2, border: '1px solid #D4CFC3', backgroundColor: '#fff' }}
                formatter={(v: number, name: string) => [v, name === 'missioni' ? 'Missioni attive' : 'Personale']}
                labelFormatter={(l) => `Anno ${l}`}
              />
              {/* Key historical event lines */}
              {HISTORICAL_EVENTS.filter(e => [1991, 1999, 2001, 2011, 2022].includes(e.year)).map(e => (
                <ReferenceLine key={e.year} x={e.year} stroke="#8B1A1A" strokeDasharray="3 3" strokeOpacity={0.4} />
              ))}
              <Area type="monotone" dataKey="missioni" stroke="#4A5D23" strokeWidth={2} fill="url(#areaGrad)" />
            </AreaChart>
          </ResponsiveContainer>
          {/* Event legend below chart */}
          <div className="flex flex-wrap gap-x-4 gap-y-1 mt-2 border-t border-[#EAE6DC] pt-2">
            {HISTORICAL_EVENTS.filter(e => [1991, 1999, 2001, 2011, 2022].includes(e.year)).map(e => (
              <span key={e.year} className="text-[8px] text-[#8B9298] uppercase tracking-[0.1em]">
                <b className="text-[#8B1A1A]">{e.year}</b> {e.label}
              </span>
            ))}
          </div>
        </div>

        {/* CHARTS */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <OrgDonut data={stats.by_org} />
          <RegionBar data={stats.by_region} />
          <DecadeBar data={stats.by_decade} />
        </div>
      </div>
    </motion.div>
  )
}
