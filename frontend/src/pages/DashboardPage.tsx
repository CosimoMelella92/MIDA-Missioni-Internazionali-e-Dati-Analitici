import { useState, useMemo } from 'react'
import { useMissions } from '../hooks/useMissions'
import OrgDonut from '../components/charts/OrgDonut'
import RegionBar from '../components/charts/RegionBar'
import DecadeBar from '../components/charts/DecadeBar'
import { ORG_COLORS } from '../lib/constants'
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend, BarChart, Bar, XAxis, YAxis } from 'recharts'

export default function DashboardPage() {
  const { missions, loading } = useMissions()
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
    const counts: Record<string, number> = {}
    filtered.forEach(m => { counts[m.tipo_missione] = (counts[m.tipo_missione] || 0) + 1 })
    return counts
  }, [filtered])

  const byRegion = useMemo(() => {
    const counts: Record<string, number> = {}
    filtered.forEach(m => { counts[m.regione] = (counts[m.regione] || 0) + 1 })
    return counts
  }, [filtered])

  const byDecade = useMemo(() => {
    const counts: Record<string, number> = {}
    filtered.forEach(m => {
      if (!m.data_inizio) return
      const decade = Math.floor(new Date(m.data_inizio).getFullYear() / 10) * 10
      const key = `${decade}`
      counts[key] = (counts[key] || 0) + 1
    })
    return counts
  }, [filtered])

  const topPersonnel = useMemo(() => {
    return [...filtered]
      .filter(m => m.personale_totale > 0)
      .sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0))
      .slice(0, 10)
      .map(m => ({ name: m.nome.length > 25 ? m.nome.slice(0, 22) + '...' : m.nome, value: Math.round(m.personale_totale || 0), org: m.tipo_missione }))
  }, [filtered])

  const orgs = [...new Set(missions.map(m => m.tipo_missione))].sort()
  const regions = [...new Set(missions.map(m => m.regione))].sort()

  if (loading) return <div className="flex items-center justify-center h-96"><div className="animate-spin rounded-full h-12 w-12 border-b-2 border-mida-teal" /></div>

  return (
    <div className="max-w-7xl mx-auto px-4 py-8 space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <h1 className="text-3xl font-bold text-mida-navy dark:text-white">Dashboard ({filtered.length} missioni)</h1>
        <div className="flex flex-wrap gap-3">
          <select value={orgFilter} onChange={e => setOrgFilter(e.target.value)} className="px-3 py-2 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm">
            <option value="">Tutte le org.</option>
            {orgs.map(o => <option key={o} value={o}>{o}</option>)}
          </select>
          <select value={regionFilter} onChange={e => setRegionFilter(e.target.value)} className="px-3 py-2 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm">
            <option value="">Tutte le regioni</option>
            {regions.map(r => <option key={r} value={r}>{r}</option>)}
          </select>
          <label className="flex items-center gap-2 cursor-pointer text-sm">
            <input type="checkbox" checked={activeOnly} onChange={e => setActiveOnly(e.target.checked)} className="rounded text-mida-teal" />
            Solo attive
          </label>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <OrgDonut data={byOrg} />
        <RegionBar data={byRegion} />
        <DecadeBar data={byDecade} />
      </div>

      {/* Top 10 Personnel */}
      <div className="kpi-card">
        <h3 className="text-lg font-semibold mb-4">Top 10 — Personale</h3>
        <ResponsiveContainer width="100%" height={350}>
          <BarChart data={topPersonnel} layout="vertical" margin={{ left: 120 }}>
            <XAxis type="number" />
            <YAxis type="category" dataKey="name" width={120} tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => [v.toLocaleString('it-IT'), 'Personale']} />
            <Bar dataKey="value" radius={[0, 4, 4, 0]}>
              {topPersonnel.map((entry) => (
                <Cell key={entry.name} fill={ORG_COLORS[entry.org] || '#999'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
