import { useState, useMemo } from 'react'
import { motion } from 'framer-motion'
import { useMissions } from '../hooks/useMissions'
import OrgDonut from '../components/charts/OrgDonut'
import RegionBar from '../components/charts/RegionBar'
import DecadeBar from '../components/charts/DecadeBar'
import { ORG_COLORS } from '../lib/constants'
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, Cell } from 'recharts'

export default function DashboardPage() {
  const { missions, active, loading } = useMissions()
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
        name: m.nome.length > 25 ? m.nome.slice(0, 23) + '…' : m.nome,
        value: Math.round(m.personale_totale || 0),
        org: m.tipo_missione,
      }))
  }, [filtered])

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

  if (loading) return <div className="flex items-center justify-center h-96 bg-[#F5F3EE]"><p className="text-[11px] text-[#8B9298] uppercase tracking-[0.15em]">Caricamento dati...</p></div>

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.2 }} className="max-w-7xl mx-auto px-4 py-6 space-y-5">
      {/* Header + inline filters */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C]">Analisi Operativa</h1>
          <p className="text-[11px] text-[#8B9298]">{filtered.length} missioni</p>
        </div>
        <div className="flex items-center gap-2">
          <select value={orgFilter} onChange={e => setOrgFilter(e.target.value)} className="px-2 py-1 rounded border border-[#D4CFC3] bg-white text-[11px]">
            <option value="">Tutte le org.</option>
            {orgs.map(o => <option key={o} value={o}>{o}</option>)}
          </select>
          <select value={regionFilter} onChange={e => setRegionFilter(e.target.value)} className="px-2 py-1 rounded border border-[#D4CFC3] bg-white text-[11px]">
            <option value="">Tutte le regioni</option>
            {regions.map(r => <option key={r} value={r}>{r}</option>)}
          </select>
          <label className="flex items-center gap-1.5 cursor-pointer">
            <input type="checkbox" checked={activeOnly} onChange={e => setActiveOnly(e.target.checked)} className="accent-[#4A5D23]" />
            <span className="text-[11px] text-[#5A5F63]">Solo in corso</span>
          </label>
          {(orgFilter || regionFilter || activeOnly) && (
            <button onClick={() => { setOrgFilter(''); setRegionFilter(''); setActiveOnly(false) }} className="text-[10px] uppercase tracking-[0.1em] text-[#8B9298] hover:text-[#1B3A5C]">
              Reset
            </button>
          )}
        </div>
      </div>

      {/* Charts 2x2 */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <OrgDonut data={byOrg} />
        <RegionBar data={byRegion} />
        <DecadeBar data={byDecade} />
        {/* Top 10 Personnel */}
        <div className="bg-white border border-[#D4CFC3] rounded p-4">
          <h3 className="text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] pb-2 mb-3">Top 10 — Personale</h3>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={topPersonnel} layout="vertical" margin={{ left: 130 }}>
              <XAxis type="number" tick={{ fontSize: 9 }} tickLine={false} axisLine={{ stroke: '#D4CFC3' }} />
              <YAxis type="category" dataKey="name" width={130} tick={{ fontSize: 9 }} tickLine={false} axisLine={false} />
              <Tooltip formatter={(v: number) => [v.toLocaleString('it-IT'), 'Personale']} contentStyle={{ fontSize: 11, borderRadius: 2, border: '1px solid #D4CFC3' }} />
              <Bar dataKey="value" radius={[0, 2, 2, 0]}>
                {topPersonnel.map((entry) => (
                  <Cell key={entry.name} fill={ORG_COLORS[entry.org] || '#8B9298'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Org summary table */}
      <div className="bg-white border border-[#D4CFC3] rounded overflow-hidden">
        <h3 className="text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] p-4 pb-2">Riepilogo per Organizzazione</h3>
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
              <tr key={r.org} className={`border-b border-[#EAE6DC] ${i % 2 ? 'bg-[#F5F3EE]' : ''}`}>
                <td className="px-4 py-1.5 font-medium text-[#1B3A5C]">{r.org}</td>
                <td className="px-4 py-1.5 text-right font-mono text-[#5A5F63]">{r.total}</td>
                <td className="px-4 py-1.5 text-right font-mono text-[#4A5D23] font-bold">{r.active}</td>
                <td className="px-4 py-1.5 text-right font-mono text-[#1B3A5C]">{r.personnel.toLocaleString('it-IT')}</td>
                <td className="px-4 py-1.5 text-right font-mono text-[#8B9298]">{r.pct}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </motion.div>
  )
}
