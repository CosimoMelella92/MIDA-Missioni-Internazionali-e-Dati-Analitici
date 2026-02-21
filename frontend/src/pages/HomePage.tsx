import { useMemo, useEffect, useRef } from 'react'
import { motion } from 'framer-motion'
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import KpiCard from '../components/cards/KpiCard'
import OrgDonut from '../components/charts/OrgDonut'
import RegionBar from '../components/charts/RegionBar'
import DecadeBar from '../components/charts/DecadeBar'
import { useMissions } from '../hooks/useMissions'
import { ORG_COLORS, GEOCODING, ROMA } from '../lib/constants'
import L from 'leaflet'

export default function HomePage() {
  const { missions, active, stats, loading } = useMissions()
  const miniMapRef = useRef<HTMLDivElement>(null)
  const miniMapInstance = useRef<L.Map | null>(null)

  const areaData = useMemo(() => {
    if (!missions.length) return []
    const years: Record<number, number> = {}
    for (let y = 1948; y <= 2026; y++) years[y] = 0
    missions.forEach(m => {
      if (!m.data_inizio) return
      const start = new Date(m.data_inizio).getFullYear()
      const end = m.data_fine && m.data_fine !== 'NaT' ? new Date(m.data_fine).getFullYear() : 2026
      for (let y = Math.max(start, 1948); y <= Math.min(end, 2026); y++) years[y] = (years[y] || 0) + 1
    })
    return Object.entries(years).map(([y, v]) => ({ year: +y, attive: v })).sort((a, b) => a.year - b.year)
  }, [missions])

  const sortedActive = useMemo(() => [...active].sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0)), [active])

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
      <div className="flex items-center justify-center h-96 bg-[#F5F3EE]">
        <p className="text-[11px] text-[#8B9298] uppercase tracking-[0.15em]">Caricamento dati...</p>
      </div>
    )
  }

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.2 }}>
      {/* HERO */}
      <div className="bg-gradient-to-r from-[#1B3A5C] to-[#3D4F1E] px-4 py-8 md:py-10">
        <div className="max-w-7xl mx-auto flex items-start justify-between">
          <div>
            <p className="text-[10px] uppercase tracking-[0.2em] text-[#8B9298]">Ministero della Difesa — Quadro Situazione Febbraio 2026</p>
            <h1 className="text-2xl md:text-3xl font-bold text-white tracking-tight mt-2">
              Missioni Internazionali Italiane
            </h1>
            <p className="text-[13px] text-[#D4CFC3] mt-2 max-w-lg">
              {stats.total} operazioni dal 1948 · {stats.active} in corso · {stats.personnel.toLocaleString('it-IT')} unità di personale
            </p>
          </div>
          <img src="/emblema_repubblica.svg" alt="" className="w-10 h-10 opacity-60 hidden md:block" />
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-6 space-y-6">
        {/* KPI STRIP */}
        <div className="bg-white border border-[#D4CFC3] rounded flex divide-x divide-[#D4CFC3] -mt-8 relative z-10">
          <KpiCard label="Missioni Totali" value={stats.total} />
          <KpiCard label="In Corso" value={stats.active} />
          <KpiCard label="Personale" value={stats.personnel} />
          <KpiCard label="Teatri Operativi" value={stats.countries} />
          <KpiCard label="Organizzazioni" value={stats.organizations} />
        </div>

        {/* TWO COLUMNS: Table + Mini-map */}
        <div className="grid grid-cols-1 lg:grid-cols-5 gap-5">
          {/* Active missions table */}
          <div className="lg:col-span-3">
            <h2 className="text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] pb-2 mb-3">
              Missioni in Corso — {active.length}
            </h2>
            <div className="bg-white border border-[#D4CFC3] rounded overflow-hidden">
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
                    <tr key={m.nome} className={`border-b border-[#EAE6DC] ${i % 2 ? 'bg-[#F5F3EE]' : ''}`}>
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
          </div>

          {/* Mini-map */}
          <div className="lg:col-span-2">
            <h2 className="text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] pb-2 mb-3">
              Teatri Operativi
            </h2>
            <div ref={miniMapRef} className="w-full h-[400px] border border-[#D4CFC3] rounded" />
          </div>
        </div>

        {/* AREA CHART */}
        <div className="bg-white border border-[#D4CFC3] rounded p-4">
          <h2 className="text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] pb-2 mb-3">
            Missioni Attive per Anno (1948–2026)
          </h2>
          <ResponsiveContainer width="100%" height={180}>
            <AreaChart data={areaData} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#4A5D23" stopOpacity={0.15} />
                  <stop offset="95%" stopColor="#4A5D23" stopOpacity={0.01} />
                </linearGradient>
              </defs>
              <XAxis dataKey="year" tick={{ fontSize: 10 }} tickLine={false} axisLine={{ stroke: '#D4CFC3' }} interval={9} />
              <YAxis tick={{ fontSize: 10 }} tickLine={false} axisLine={false} width={28} />
              <Tooltip contentStyle={{ fontSize: 11, borderRadius: 2, border: '1px solid #D4CFC3' }} formatter={(v: number) => [v, 'Missioni attive']} labelFormatter={(l) => `${l}`} />
              <Area type="monotone" dataKey="attive" stroke="#4A5D23" strokeWidth={1.5} fill="url(#areaGrad)" />
            </AreaChart>
          </ResponsiveContainer>
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
