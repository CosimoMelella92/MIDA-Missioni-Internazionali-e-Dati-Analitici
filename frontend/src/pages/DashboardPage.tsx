import { useState, useMemo } from 'react'
import { motion } from 'framer-motion'
import { useData } from '../context/DataProvider'
import OrgDonut from '../components/charts/OrgDonut'
import RegionBar from '../components/charts/RegionBar'
import DecadeBar from '../components/charts/DecadeBar'
import { ORG_COLORS, REGION_COLORS, HISTORICAL_EVENTS } from '../lib/constants'
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, Cell, Area, ReferenceLine, ComposedChart } from 'recharts'

export default function DashboardPage() {
  const { missions, active, loading } = useData()
  const [orgFilter, setOrgFilter] = useState('')
  const [regionFilter, setRegionFilter] = useState('')
  const [activeOnly, setActiveOnly] = useState(false)

  const filtered = useMemo(() => {
    let m = missions
    if (orgFilter) m = m.filter(x => x.tipo_missione === orgFilter)
    if (regionFilter) m = m.filter(x => x.regione === regionFilter)
    if (activeOnly) m = m.filter(x => x.is_active)
    return m
  }, [missions, orgFilter, regionFilter, activeOnly])

  const byOrg = useMemo(() => {
    const c: Record<string, number> = {}
    filtered.forEach(m => { c[m.tipo_missione] = (c[m.tipo_missione] || 0) + 1 })
    return c
  }, [filtered])

  const byRegion = useMemo(() => {
    const c: Record<string, number> = {}
    filtered.forEach(m => { c[m.regione] = (c[m.regione] || 0) + 1 })
    return c
  }, [filtered])

  const byDecade = useMemo(() => {
    const c: Record<string, number> = {}
    filtered.forEach(m => {
      if (!m.data_inizio) return
      const d = Math.floor(new Date(m.data_inizio).getFullYear() / 10) * 10
      c[`${d}`] = (c[`${d}`] || 0) + 1
    })
    return c
  }, [filtered])

  const topPersonnel = useMemo(() => {
    return [...filtered]
      .filter(m => m.personale_totale > 0)
      .sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0))
      .slice(0, 10)
      .map(m => ({
        name: m.nome.length > 22 ? m.nome.slice(0, 20) + '…' : m.nome,
        fullName: m.nome,
        value: Math.round(m.personale_totale || 0),
        org: m.tipo_missione,
        paese: m.paese,
      }))
  }, [filtered])

  // INTELLIGENCE: Troop strength timeline with drawdown analysis
  const troopTimeline = useMemo(() => {
    const years: Record<number, { active: number; newStarts: number; ended: number }> = {}
    for (let y = 1948; y <= 2026; y++) years[y] = { active: 0, newStarts: 0, ended: 0 }
    missions.forEach(m => {
      if (!m.data_inizio) return
      const start = new Date(m.data_inizio).getFullYear()
      const end = m.data_fine && m.data_fine !== 'NaT' ? new Date(m.data_fine).getFullYear() : 2026
      if (years[start]) years[start].newStarts++
      if (end < 2026 && years[end]) years[end].ended++
      for (let y = Math.max(start, 1948); y <= Math.min(end, 2026); y++) {
        if (years[y]) years[y].active++
      }
    })
    return Object.entries(years).map(([y, v]) => ({
      year: +y,
      attive: v.active,
      nuove: v.newStarts,
      concluse: -v.ended,
    })).sort((a, b) => a.year - b.year)
  }, [missions])

  // Peak & drawdown stats
  const peakData = useMemo(() => {
    const peak = troopTimeline.reduce((max, d) => d.attive > max.attive ? d : max, troopTimeline[0] || { year: 0, attive: 0 })
    const current = troopTimeline[troopTimeline.length - 1]
    const drawdown = peak.attive > 0 ? Math.round(((peak.attive - (current?.attive || 0)) / peak.attive) * 100) : 0
    return { peak, current, drawdown }
  }, [troopTimeline])

  // Region distribution for active missions
  const activeByRegion = useMemo(() => {
    const c: Record<string, { count: number; personnel: number }> = {}
    active.forEach(m => {
      const r = m.regione || 'Altro'
      if (!c[r]) c[r] = { count: 0, personnel: 0 }
      c[r].count++
      c[r].personnel += m.personale_totale || 0
    })
    return Object.entries(c).sort((a, b) => b[1].personnel - a[1].personnel)
  }, [active])

  // Org summary table
  const orgTable = useMemo(() => {
    const order = ['ONU', 'NATO', 'UE', 'ITA', 'Bilateral', 'Multinational', 'Coalizione']
    return order.map(org => {
      const all = filtered.filter(m => m.tipo_missione === org)
      const act = all.filter(m => m.is_active)
      const pers = all.reduce((s, m) => s + (m.personale_totale || 0), 0)
      return { org, total: all.length, active: act.length, personnel: Math.round(pers), pct: filtered.length ? Math.round((all.length / filtered.length) * 100) : 0 }
    }).filter(r => r.total > 0)
  }, [filtered])

  const orgs = [...new Set(missions.map(m => m.tipo_missione))].sort()
  const regions = [...new Set(missions.map(m => m.regione))].sort()

  if (loading) return (
    <div className="flex items-center justify-center h-96 bg-[#F5F3EE]">
      <div className="text-center">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-[#4A5D23] border-t-transparent mx-auto" />
        <p className="mt-3 text-[10px] text-[#8B9298] uppercase tracking-[0.15em]">Elaborazione intelligence...</p>
      </div>
    </div>
  )

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.2 }} className="max-w-7xl mx-auto px-4 py-4 md:py-6 space-y-5">
      {/* Header + inline filters */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-3">
        <div>
          <h1 className="text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C]">Intelligence Dashboard</h1>
          <p className="text-[11px] text-[#8B9298]">{filtered.length} missioni · {active.length} in corso · Dati 1948–2026</p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <select value={orgFilter} onChange={e => setOrgFilter(e.target.value)} className="px-2 py-1.5 rounded border border-[#D4CFC3] bg-white text-[11px]">
            <option value="">Tutte le org.</option>
            {orgs.map(o => <option key={o} value={o}>{o}</option>)}
          </select>
          <select value={regionFilter} onChange={e => setRegionFilter(e.target.value)} className="px-2 py-1.5 rounded border border-[#D4CFC3] bg-white text-[11px]">
            <option value="">Tutte le regioni</option>
            {regions.map(r => <option key={r} value={r}>{r}</option>)}
          </select>
          <label className="flex items-center gap-1.5 cursor-pointer">
            <input type="checkbox" checked={activeOnly} onChange={e => setActiveOnly(e.target.checked)} className="accent-[#4A5D23]" />
            <span className="text-[11px] text-[#5A5F63]">Solo in corso</span>
          </label>
          {(orgFilter || regionFilter || activeOnly) && (
            <button onClick={() => { setOrgFilter(''); setRegionFilter(''); setActiveOnly(false) }} className="text-[10px] uppercase tracking-[0.1em] text-[#8B1A1A] font-bold hover:text-[#1B3A5C]">
              Reset filtri
            </button>
          )}
        </div>
      </div>

      {/* INTELLIGENCE PANEL: Troop Drawdown Analysis */}
      <div className="bg-white border border-[#D4CFC3] rounded overflow-hidden">
        <div className="bg-[#1B3A5C] px-4 py-3 flex flex-col md:flex-row md:items-center md:justify-between">
          <div>
            <h2 className="text-[12px] font-bold uppercase tracking-[0.15em] text-white">Analisi Forza Operativa — Missioni Attive per Anno</h2>
            <p className="text-[9px] text-[#8B9298] mt-0.5">Trend storico con identificazione del picco operativo e successivo drawdown</p>
          </div>
          <div className="flex gap-4 mt-2 md:mt-0">
            <div className="text-center">
              <p className="text-[18px] font-mono font-bold text-white">{peakData.peak.attive}</p>
              <p className="text-[8px] uppercase tracking-[0.1em] text-[#8B9298]">Picco ({peakData.peak.year})</p>
            </div>
            <div className="text-center">
              <p className="text-[18px] font-mono font-bold text-[#6B8C2A]">{peakData.current?.attive || 0}</p>
              <p className="text-[8px] uppercase tracking-[0.1em] text-[#8B9298]">Attuali (2026)</p>
            </div>
            <div className="text-center">
              <p className="text-[18px] font-mono font-bold text-[#8B1A1A]">-{peakData.drawdown}%</p>
              <p className="text-[8px] uppercase tracking-[0.1em] text-[#8B9298]">Drawdown</p>
            </div>
          </div>
        </div>
        <div className="p-4">
          <ResponsiveContainer width="100%" height={220}>
            <ComposedChart data={troopTimeline} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="troopGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#1B3A5C" stopOpacity={0.25} />
                  <stop offset="95%" stopColor="#1B3A5C" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <XAxis dataKey="year" tick={{ fontSize: 9 }} tickLine={false} axisLine={{ stroke: '#D4CFC3' }} interval={9} />
              <YAxis tick={{ fontSize: 9 }} tickLine={false} axisLine={false} width={28} />
              <Tooltip
                contentStyle={{ fontSize: 11, borderRadius: 2, border: '1px solid #D4CFC3', backgroundColor: '#fff' }}
                formatter={(v: number, name: string) => {
                  if (name === 'attive') return [v, 'Missioni attive']
                  if (name === 'nuove') return [v, 'Nuove missioni']
                  return [Math.abs(v), 'Missioni concluse']
                }}
                labelFormatter={(l) => `Anno ${l}`}
              />
              {HISTORICAL_EVENTS.filter(e => [1991, 1999, 2001, 2003, 2011, 2014, 2022].includes(e.year)).map(e => (
                <ReferenceLine key={e.year} x={e.year} stroke="#8B1A1A" strokeDasharray="3 3" strokeOpacity={0.3} />
              ))}
              <ReferenceLine x={peakData.peak.year} stroke="#8B1A1A" strokeWidth={1.5} strokeOpacity={0.6} label={{ value: 'PICCO', position: 'top', fontSize: 8, fill: '#8B1A1A' }} />
              <Area type="monotone" dataKey="attive" stroke="#1B3A5C" strokeWidth={2} fill="url(#troopGrad)" />
              <Bar dataKey="nuove" fill="#4A5D23" opacity={0.5} />
              <Bar dataKey="concluse" fill="#8B1A1A" opacity={0.3} />
            </ComposedChart>
          </ResponsiveContainer>
          <div className="flex flex-wrap gap-x-4 gap-y-1 mt-2 border-t border-[#EAE6DC] pt-2">
            <span className="flex items-center gap-1.5 text-[8px] text-[#8B9298] uppercase tracking-[0.1em]"><span className="w-3 h-1.5 bg-[#1B3A5C]/30 inline-block rounded" /> Missioni attive</span>
            <span className="flex items-center gap-1.5 text-[8px] text-[#8B9298] uppercase tracking-[0.1em]"><span className="w-3 h-1.5 bg-[#4A5D23]/50 inline-block rounded" /> Nuove aperture</span>
            <span className="flex items-center gap-1.5 text-[8px] text-[#8B9298] uppercase tracking-[0.1em]"><span className="w-3 h-1.5 bg-[#8B1A1A]/30 inline-block rounded" /> Chiusure</span>
            {HISTORICAL_EVENTS.filter(e => [1991, 1999, 2001, 2011, 2022].includes(e.year)).map(e => (
              <span key={e.year} className="text-[8px] text-[#8B9298] uppercase tracking-[0.1em]">
                <b className="text-[#8B1A1A]">{e.year}</b> {e.label}
              </span>
            ))}
          </div>
        </div>
      </div>

      {/* ACTIVE FORCE DISTRIBUTION by region */}
      <div className="bg-white border border-[#D4CFC3] rounded p-4">
        <h2 className="text-[13px] md:text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] pb-2 mb-3">
          Distribuzione Forza Operativa — Teatri Attivi
        </h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {activeByRegion.map(([region, data]) => (
            <div key={region} className="border border-[#D4CFC3] rounded p-3">
              <div className="flex items-center gap-2 mb-2">
                <div className="w-2 h-2 rounded-full" style={{ backgroundColor: REGION_COLORS[region] || '#8B9298' }} />
                <span className="text-[10px] font-bold uppercase tracking-[0.1em] text-[#1B3A5C]">{region}</span>
              </div>
              <p className="text-[22px] font-mono font-bold text-[#1B3A5C] leading-none">{data.count}</p>
              <p className="text-[9px] text-[#8B9298] mt-1">missioni · {Math.round(data.personnel).toLocaleString('it-IT')} pers.</p>
            </div>
          ))}
        </div>
      </div>

      {/* Charts 2x2 */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <OrgDonut data={byOrg} />
        <div className="bg-white border border-[#D4CFC3] rounded p-4">
          <h3 className="text-[13px] md:text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] pb-2 mb-3">Top 10 — Impiego Personale</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={topPersonnel} layout="vertical" margin={{ left: 100 }}>
              <XAxis type="number" tick={{ fontSize: 9 }} tickLine={false} axisLine={{ stroke: '#D4CFC3' }} />
              <YAxis type="category" dataKey="name" width={100} tick={{ fontSize: 9 }} tickLine={false} axisLine={false} />
              <Tooltip
                formatter={(v: number) => [`${v.toLocaleString('it-IT')} unità`, 'Personale']}
                contentStyle={{ fontSize: 11, borderRadius: 2, border: '1px solid #D4CFC3' }}
              />
              <Bar dataKey="value" radius={[0, 2, 2, 0]}>
                {topPersonnel.map((entry) => (
                  <Cell key={entry.name} fill={ORG_COLORS[entry.org] || '#8B9298'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
        <RegionBar data={byRegion} />
        <DecadeBar data={byDecade} />
      </div>

      {/* Org summary table */}
      <div className="bg-white border border-[#D4CFC3] rounded overflow-hidden">
        <h3 className="text-[13px] md:text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] p-4 pb-2">Riepilogo per Organizzazione</h3>
        {/* Desktop table */}
        <div className="hidden md:block">
          <table className="w-full text-[11px]">
            <thead>
              <tr className="bg-[#1B3A5C] text-white">
                <th className="px-4 py-2 text-left text-[9px] uppercase tracking-[0.1em]">Organizzazione</th>
                <th className="px-4 py-2 text-right text-[9px] uppercase tracking-[0.1em]">Totali</th>
                <th className="px-4 py-2 text-right text-[9px] uppercase tracking-[0.1em]">Attive</th>
                <th className="px-4 py-2 text-right text-[9px] uppercase tracking-[0.1em]">Personale</th>
                <th className="px-4 py-2 text-right text-[9px] uppercase tracking-[0.1em]">%</th>
              </tr>
            </thead>
            <tbody>
              {orgTable.map((r, i) => (
                <tr key={r.org} className={`border-b border-[#EAE6DC] ${i % 2 ? 'bg-[#F5F3EE]' : ''} hover:bg-[#EAE6DC]/50 transition-colors`}>
                  <td className="px-4 py-1.5 font-medium text-[#1B3A5C]">
                    <span className="inline-block w-2 h-2 rounded-full mr-2" style={{ backgroundColor: ORG_COLORS[r.org] || '#8B9298' }} />
                    {r.org}
                  </td>
                  <td className="px-4 py-1.5 text-right font-mono text-[#5A5F63]">{r.total}</td>
                  <td className="px-4 py-1.5 text-right font-mono text-[#4A5D23] font-bold">{r.active}</td>
                  <td className="px-4 py-1.5 text-right font-mono text-[#1B3A5C]">{r.personnel.toLocaleString('it-IT')}</td>
                  <td className="px-4 py-1.5 text-right">
                    <div className="flex items-center justify-end gap-2">
                      <div className="w-12 h-1.5 bg-[#EAE6DC] rounded overflow-hidden">
                        <div className="h-full rounded" style={{ width: `${r.pct}%`, backgroundColor: ORG_COLORS[r.org] || '#8B9298' }} />
                      </div>
                      <span className="font-mono text-[#8B9298] w-8 text-right">{r.pct}%</span>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {/* Mobile card layout */}
        <div className="md:hidden p-3 space-y-2">
          {orgTable.map(r => (
            <div key={r.org} className="border border-[#D4CFC3] rounded p-3 flex items-center gap-3">
              <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: ORG_COLORS[r.org] || '#8B9298' }} />
              <div className="flex-1">
                <p className="text-[11px] font-bold text-[#1B3A5C]">{r.org}</p>
                <p className="text-[9px] text-[#8B9298]">{r.total} totali · {r.active} attive</p>
              </div>
              <div className="text-right">
                <p className="text-[13px] font-mono font-bold text-[#1B3A5C]">{r.personnel.toLocaleString('it-IT')}</p>
                <p className="text-[9px] text-[#8B9298]">{r.pct}%</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </motion.div>
  )
}
