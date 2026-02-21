import { useMemo, useEffect, useRef } from 'react'
import { motion } from 'framer-motion'
import { Shield, Users, Globe, Crosshair, Building2, MapPin } from 'lucide-react'
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import KpiCard from '../components/cards/KpiCard'
import OrgDonut from '../components/charts/OrgDonut'
import RegionBar from '../components/charts/RegionBar'
import DecadeBar from '../components/charts/DecadeBar'
import { useMissions } from '../hooks/useMissions'
import { ORG_COLORS, MILITARY, GEOCODING, ROMA, COUNTRY_FLAGS } from '../lib/constants'
import L from 'leaflet'

export default function HomePage() {
  const { missions, active, stats, loading } = useMissions()
  const miniMapRef = useRef<HTMLDivElement>(null)
  const miniMapInstance = useRef<L.Map | null>(null)

  // Compute active-per-year for area chart
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

  // Group active missions by org
  const byOrg = useMemo(() => {
    const groups: Record<string, typeof active> = {}
    const order = ['ONU', 'NATO', 'UE', 'ITA', 'Bilateral', 'Multinational', 'Coalizione']
    order.forEach(o => { groups[o] = [] })
    active.forEach(m => {
      const key = m.tipo_missione || 'Altro'
      if (!groups[key]) groups[key] = []
      groups[key].push(m)
    })
    return Object.entries(groups).filter(([, v]) => v.length > 0)
  }, [active])

  // Max personnel for proportional bars
  const maxPers = useMemo(() => Math.max(...active.map(m => m.personale_totale || 0), 1), [active])

  // Mini-map
  useEffect(() => {
    if (loading || !miniMapRef.current || miniMapInstance.current) return
    const map = L.map(miniMapRef.current, { zoomControl: false, attributionControl: false, dragging: false, scrollWheelZoom: false, doubleClickZoom: false }).setView([30, 20], 2)
    miniMapInstance.current = map
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', { maxZoom: 6 }).addTo(map)
    L.circleMarker(ROMA, { radius: 4, color: '#8B1A1A', fillColor: '#8B1A1A', fillOpacity: 1, weight: 0 }).addTo(map)
    const byCountry: Record<string, number> = {}
    active.forEach(m => { byCountry[m.paese] = (byCountry[m.paese] || 0) + (m.personale_totale || 0) })
    Object.entries(byCountry).forEach(([paese, pers]) => {
      const coords = GEOCODING[paese]
      if (!coords) return
      const r = Math.max(3, Math.min(12, Math.sqrt(pers) / 2.5))
      L.circleMarker(coords, { radius: r, color: 'rgba(107,140,42,0.9)', fillColor: '#6B8C2A', fillOpacity: 0.7, weight: 1 }).addTo(map)
      L.polyline([ROMA, coords], { color: 'rgba(107,140,42,0.2)', weight: 1, dashArray: '3 5' }).addTo(map)
    })
    return () => { map.remove(); miniMapInstance.current = null }
  }, [active, loading])

  if (loading || !stats) {
    return (
      <div className="flex items-center justify-center h-96 bg-mil-sand">
        <div className="text-center">
          <div className="animate-spin rounded-full h-10 w-10 border-2 border-mil-olive border-t-transparent mx-auto" />
          <p className="mt-3 text-xs text-mil-steel uppercase tracking-[0.2em]">Caricamento dati operativi...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-0">
      {/* ═══ HERO — Topo Background ═══ */}
      <div className="topo-bg px-4 py-8 md:py-10">
        <div className="max-w-7xl mx-auto">
          <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-6">
            <motion.div initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }} className="flex-1">
              <div className="flex items-center gap-3 mb-3">
                <span className="stamp">Rapporto Situazione</span>
                <span className="text-[10px] text-mil-sand-deep uppercase tracking-[0.2em]">Febbraio 2026</span>
              </div>
              <h1 className="text-3xl md:text-4xl font-bold text-white tracking-tight leading-tight">
                Missioni Internazionali<br/>
                <span className="text-mil-olive-light">Italiane</span>
              </h1>
              <p className="text-sm text-mil-sand-dark mt-3 max-w-lg leading-relaxed">
                {stats.total} operazioni condotte dal 1948 · {stats.active} missioni in corso su {stats.countries} teatri operativi · {stats.personnel.toLocaleString('it-IT')} unità di personale impiegato
              </p>
              <div className="flex gap-4 mt-4">
                <a href="https://www.difesa.it/operazionimilitari/" className="text-[10px] text-mil-sand-deep hover:text-white uppercase tracking-widest underline" target="_blank" rel="noopener">Min. Difesa</a>
                <a href="https://www.analisidifesa.it/" className="text-[10px] text-mil-sand-deep hover:text-white uppercase tracking-widest underline" target="_blank" rel="noopener">Analisi Difesa</a>
              </div>
            </motion.div>
            {/* Mini-map */}
            <motion.div initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }} transition={{ delay: 0.2 }} className="w-full md:w-[380px] h-[220px] rounded-lg overflow-hidden border border-mil-navy-light/30 shadow-lg flex-shrink-0">
              <div ref={miniMapRef} className="w-full h-full" />
            </motion.div>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-6 space-y-6">
        {/* ═══ KPI STRIP ═══ */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3 -mt-8 relative z-10">
          <KpiCard label="Missioni Totali" value={stats.total} icon={Shield} color={MILITARY.navy} delay={0} />
          <KpiCard label="In Corso" value={stats.active} icon={Crosshair} color={MILITARY.olive} delay={0.05} />
          <KpiCard label="Personale" value={stats.personnel} icon={Users} color={MILITARY.red} delay={0.1} />
          <KpiCard label="Teatri Operativi" value={stats.countries} icon={Globe} color={MILITARY.khaki} delay={0.15} />
          <KpiCard label="Organizzazioni" value={stats.organizations} icon={Building2} color={MILITARY.steel} delay={0.2} />
        </div>

        {/* ═══ DISPOSITIVO OPERATIVO — Grouped by Org ═══ */}
        <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.25 }}>
          <h2 className="section-title">Dispositivo Operativo — {active.length} Missioni in Corso</h2>
          <div className="space-y-4">
            {byOrg.map(([org, missions]) => (
              <div key={org}>
                <div className="flex items-center gap-2 mb-2">
                  <div className="w-3 h-3 rounded-sm" style={{ backgroundColor: ORG_COLORS[org] }} />
                  <span className="text-xs font-bold uppercase tracking-widest text-mil-steel">{org}</span>
                  <span className="text-[10px] text-mil-steel-light">({missions.length})</span>
                </div>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-1.5">
                  {[...missions].sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0)).map(m => (
                    <div key={m.nome} className="flex items-center gap-2 p-2 rounded bg-white border border-mil-sand-dark hover:border-mil-olive transition-all group">
                      <div className="led-active flex-shrink-0" />
                      <div className="min-w-0 flex-1">
                        <p className="font-semibold text-[11px] truncate text-mil-navy group-hover:text-mil-olive-dark transition-colors">
                          {COUNTRY_FLAGS[m.paese] || ''} {m.nome}
                        </p>
                        <div className="flex items-center gap-1.5 mt-0.5">
                          <span className="text-[9px] text-mil-steel"><MapPin className="w-2.5 h-2.5 inline" /> {m.paese}</span>
                          <span className="text-[9px] font-mono font-bold text-mil-navy">{m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') : '—'}</span>
                        </div>
                        <div className="personnel-bar mt-1">
                          <div className="personnel-bar-fill" style={{ width: `${((m.personale_totale || 0) / maxPers) * 100}%`, backgroundColor: ORG_COLORS[org] }} />
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </motion.div>

        {/* ═══ IMPEGNO STORICO — Area Chart ═══ */}
        <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.35 }} className="card-elevated">
          <h2 className="section-title">Impegno Storico — Missioni Attive per Anno (1948-2026)</h2>
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={areaData} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#4A5D23" stopOpacity={0.4} />
                  <stop offset="95%" stopColor="#4A5D23" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <XAxis dataKey="year" tick={{ fontSize: 10 }} tickLine={false} axisLine={{ stroke: '#D4CFC3' }} interval={9} />
              <YAxis tick={{ fontSize: 10 }} tickLine={false} axisLine={false} width={30} />
              <Tooltip contentStyle={{ fontSize: 11, borderRadius: 4, border: '1px solid #D4CFC3' }} formatter={(v: number) => [v, 'Missioni attive']} labelFormatter={(l) => `Anno ${l}`} />
              <Area type="monotone" dataKey="attive" stroke="#4A5D23" strokeWidth={2} fill="url(#areaGrad)" />
            </AreaChart>
          </ResponsiveContainer>
        </motion.div>

        {/* ═══ ANALISI — Charts Grid ═══ */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <OrgDonut data={stats.by_org} />
          <RegionBar data={stats.by_region} />
          <DecadeBar data={stats.by_decade} />
        </div>
      </div>
    </div>
  )
}
