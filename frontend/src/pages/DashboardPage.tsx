import { useState, useMemo } from 'react'
import { Filter, X } from 'lucide-react'
import { useMissions } from '../hooks/useMissions'
import OrgDonut from '../components/charts/OrgDonut'
import RegionBar from '../components/charts/RegionBar'
import DecadeBar from '../components/charts/DecadeBar'
import { ORG_COLORS, COUNTRY_FLAGS } from '../lib/constants'
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, Cell } from 'recharts'

export default function DashboardPage() {
  const { missions, loading } = useMissions()
  const [orgFilter, setOrgFilter] = useState('')
  const [regionFilter, setRegionFilter] = useState('')
  const [activeOnly, setActiveOnly] = useState(false)
  const [sidebarOpen, setSidebarOpen] = useState(true)

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
        name: (COUNTRY_FLAGS[m.paese] || '') + ' ' + (m.nome.length > 22 ? m.nome.slice(0, 20) + '…' : m.nome),
        value: Math.round(m.personale_totale || 0),
        org: m.tipo_missione,
      }))
  }, [filtered])

  // Treemap data — top 15 countries by mission count
  const treemapData = useMemo(() => {
    const c: Record<string, number> = {}
    filtered.forEach(m => { c[m.paese] = (c[m.paese] || 0) + 1 })
    return Object.entries(c)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 15)
      .map(([name, size]) => ({ name: (COUNTRY_FLAGS[name] || '') + ' ' + name, size }))
  }, [filtered])

  const orgs = [...new Set(missions.map(m => m.tipo_missione))].sort()
  const regions = [...new Set(missions.map(m => m.regione))].sort()
  const activeFilters = [orgFilter, regionFilter, activeOnly].filter(Boolean).length

  if (loading) return <div className="flex items-center justify-center h-96 bg-mil-sand"><div className="animate-spin rounded-full h-10 w-10 border-2 border-mil-olive border-t-transparent" /></div>

  return (
    <div className="flex">
      {/* Sidebar Filters */}
      <div className={`${sidebarOpen ? 'w-56' : 'w-0'} flex-shrink-0 transition-all duration-200 overflow-hidden`}>
        <div className="w-56 bg-mil-olive-dark min-h-[calc(100vh-56px)] p-4 space-y-5">
          <div className="flex items-center justify-between">
            <span className="text-[10px] font-bold uppercase tracking-widest text-mil-sand-deep">Filtri</span>
            <button onClick={() => setSidebarOpen(false)} className="text-mil-sand-deep hover:text-white"><X className="w-4 h-4" /></button>
          </div>

          <div>
            <label className="text-[9px] uppercase tracking-widest text-mil-sand-deep font-bold block mb-1">Organizzazione</label>
            <select value={orgFilter} onChange={e => setOrgFilter(e.target.value)} className="w-full px-2 py-1.5 rounded bg-mil-sand-dark text-mil-black text-xs">
              <option value="">Tutte</option>
              {orgs.map(o => <option key={o} value={o}>{o}</option>)}
            </select>
          </div>

          <div>
            <label className="text-[9px] uppercase tracking-widest text-mil-sand-deep font-bold block mb-1">Regione</label>
            <select value={regionFilter} onChange={e => setRegionFilter(e.target.value)} className="w-full px-2 py-1.5 rounded bg-mil-sand-dark text-mil-black text-xs">
              <option value="">Tutte</option>
              {regions.map(r => <option key={r} value={r}>{r}</option>)}
            </select>
          </div>

          <label className="flex items-center gap-2 cursor-pointer">
            <input type="checkbox" checked={activeOnly} onChange={e => setActiveOnly(e.target.checked)} className="rounded accent-mil-olive" />
            <span className="text-xs text-mil-sand-dark">Solo in corso</span>
          </label>

          {activeFilters > 0 && (
            <button onClick={() => { setOrgFilter(''); setRegionFilter(''); setActiveOnly(false) }} className="w-full text-[10px] uppercase tracking-widest text-mil-sand-deep hover:text-white border border-mil-sand-deep/30 rounded py-1">
              Reset filtri
            </button>
          )}
        </div>
      </div>

      {/* Main content */}
      <div className="flex-1 max-w-7xl mx-auto px-4 py-6 space-y-5">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            {!sidebarOpen && (
              <button onClick={() => setSidebarOpen(true)} className="p-2 rounded bg-mil-olive text-white hover:bg-mil-olive-dark transition-colors">
                <Filter className="w-4 h-4" />
              </button>
            )}
            <div>
              <h1 className="text-2xl font-bold text-mil-navy uppercase tracking-wide">Analisi Operativa</h1>
              <p className="text-xs text-mil-steel">{filtered.length} missioni {activeFilters > 0 ? `(${activeFilters} filtri attivi)` : ''}</p>
            </div>
          </div>
        </div>

        {/* Charts 2x2 */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <OrgDonut data={byOrg} />
          <RegionBar data={byRegion} />
          <DecadeBar data={byDecade} />
          {/* Country Grid */}
          <div className="card-elevated">
            <h3 className="section-title">Teatri Operativi (Top 15)</h3>
            <div className="flex flex-wrap gap-1.5">
              {treemapData.map(d => {
                const maxSize = treemapData[0]?.size || 1
                const opacity = 0.5 + (d.size / maxSize) * 0.5
                return (
                  <div key={d.name} className="rounded px-2.5 py-2 text-white text-center" style={{ backgroundColor: `rgba(74,93,35,${opacity})`, minWidth: d.size > 5 ? 100 : 70, flex: `${d.size} 1 0` }}>
                    <p className="text-[10px] font-bold truncate">{d.name}</p>
                    <p className="text-sm font-mono font-bold">{d.size}</p>
                  </div>
                )
              })}
            </div>
          </div>
        </div>

        {/* Top 10 Personnel */}
        <div className="card-elevated">
          <h3 className="section-title">Top 10 — Personale Impiegato</h3>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={topPersonnel} layout="vertical" margin={{ left: 140 }}>
              <XAxis type="number" tick={{ fontSize: 10 }} tickLine={false} axisLine={{ stroke: '#D4CFC3' }} />
              <YAxis type="category" dataKey="name" width={140} tick={{ fontSize: 10 }} tickLine={false} axisLine={false} />
              <Tooltip formatter={(v: number) => [v.toLocaleString('it-IT'), 'Personale']} contentStyle={{ fontSize: 11, borderRadius: 4 }} />
              <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                {topPersonnel.map((entry) => (
                  <Cell key={entry.name} fill={ORG_COLORS[entry.org] || '#8B9298'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  )
}
