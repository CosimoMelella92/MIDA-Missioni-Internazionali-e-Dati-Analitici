import { useState, useMemo } from 'react'
import { motion } from 'framer-motion'
import { Search, Download, ChevronUp, ChevronDown, X } from 'lucide-react'
import { useMissions } from '../hooks/useMissions'
import { ORG_COLORS } from '../lib/constants'
import type { Mission } from '../lib/types'

type SortKey = 'nome' | 'paese' | 'tipo_missione' | 'personale_totale' | 'data_inizio'
type SortDir = 'asc' | 'desc'

function durationYears(m: Mission): number {
  if (!m.data_inizio) return 0
  const start = new Date(m.data_inizio).getFullYear()
  const end = m.data_fine && m.data_fine !== 'NaT' ? new Date(m.data_fine).getFullYear() : 2026
  return Math.max(end - start, 0)
}

export default function MissionsPage() {
  const { missions, loading } = useMissions()
  const [search, setSearch] = useState('')
  const [orgFilter, setOrgFilter] = useState('')
  const [activeOnly, setActiveOnly] = useState(false)
  const [sortKey, setSortKey] = useState<SortKey>('nome')
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [selected, setSelected] = useState<Mission | null>(null)

  const maxDuration = useMemo(() => Math.max(...missions.map(durationYears), 1), [missions])

  const filtered = useMemo(() => {
    let result = missions
    if (search) result = result.filter(m => m.nome.toLowerCase().includes(search.toLowerCase()) || m.paese?.toLowerCase().includes(search.toLowerCase()))
    if (orgFilter) result = result.filter(m => m.tipo_missione === orgFilter)
    if (activeOnly) result = result.filter(m => m.is_active)
    result = [...result].sort((a, b) => {
      const av = a[sortKey] ?? ''
      const bv = b[sortKey] ?? ''
      const cmp = typeof av === 'number' ? av - (bv as number) : String(av).localeCompare(String(bv))
      return sortDir === 'asc' ? cmp : -cmp
    })
    return result
  }, [missions, search, orgFilter, activeOnly, sortKey, sortDir])

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir('asc') }
  }

  const SortIcon = ({ col }: { col: SortKey }) => {
    if (sortKey !== col) return null
    return sortDir === 'asc' ? <ChevronUp className="w-3 h-3 inline" /> : <ChevronDown className="w-3 h-3 inline" />
  }

  const exportCsv = () => {
    const headers = ['nome', 'paese', 'regione', 'tipo_missione', 'data_inizio', 'data_fine', 'personale_totale', 'is_active']
    const rows = filtered.map(m => headers.map(h => (m as unknown as Record<string, unknown>)[h] ?? '').join(','))
    const csv = [headers.join(','), ...rows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = 'mida_missioni.csv'; a.click()
    URL.revokeObjectURL(url)
  }

  const orgs = [...new Set(missions.map(m => m.tipo_missione))].sort()

  if (loading) return <div className="flex items-center justify-center h-96 bg-mil-sand"><div className="animate-spin rounded-full h-10 w-10 border-2 border-mil-olive border-t-transparent" /></div>

  return (
    <div className="flex">
      {/* Main table area */}
      <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.2 }} className={`flex-1 max-w-7xl mx-auto px-4 py-6 space-y-4 transition-all ${selected ? 'mr-80' : ''}`}>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h1 className="text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C]">Registro Operazioni</h1>
            <p className="text-[11px] text-[#8B9298]">Mostrando {filtered.length} di {missions.length} missioni</p>
          </div>
          <button onClick={exportCsv} className="flex items-center gap-2 px-3 py-1.5 bg-[#4A5D23] text-white rounded hover:bg-[#3D4F1E] transition-colors text-[10px] font-bold uppercase tracking-[0.1em]">
            <Download className="w-3.5 h-3.5" /> Export CSV
          </button>
        </div>

        {/* Filters */}
        <div className="flex flex-wrap gap-2 items-center">
          <div className="relative flex-1 min-w-[180px]">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-mil-steel-light" />
            <input type="text" placeholder="Cerca missione o paese..." value={search} onChange={e => setSearch(e.target.value)}
              className="w-full pl-8 pr-3 py-1.5 rounded border border-mil-sand-deep bg-white focus:ring-2 focus:ring-mil-olive focus:border-transparent outline-none text-xs" />
          </div>
          <select value={orgFilter} onChange={e => setOrgFilter(e.target.value)} className="px-2 py-1.5 rounded border border-mil-sand-deep bg-white text-xs">
            <option value="">Tutte le org.</option>
            {orgs.map(o => <option key={o} value={o}>{o}</option>)}
          </select>
          <label className="flex items-center gap-1.5 cursor-pointer">
            <input type="checkbox" checked={activeOnly} onChange={e => setActiveOnly(e.target.checked)} className="rounded accent-mil-olive" />
            <span className="text-xs text-mil-steel">Solo in corso</span>
          </label>
        </div>

        {/* Table */}
        <div className="overflow-x-auto card-elevated !p-0">
          <table className="w-full text-xs">
            <thead>
              <tr className="bg-mil-navy text-white">
                <th className="w-8 px-2 py-2.5"></th>
                {([['nome', 'Missione'], ['paese', 'Teatro'], ['tipo_missione', 'Org.'], ['data_inizio', 'Periodo'], ['personale_totale', 'Pers.']] as [SortKey, string][]).map(([key, label]) => (
                  <th key={key} onClick={() => toggleSort(key)} className="px-3 py-2.5 text-left cursor-pointer hover:bg-mil-navy-light select-none text-[10px] uppercase tracking-widest">
                    {label} <SortIcon col={key} />
                  </th>
                ))}
                <th className="px-3 py-2.5 text-left text-[10px] uppercase tracking-widest">Durata</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((m, i) => {
                const dur = durationYears(m)
                const startY = m.data_inizio?.slice(0, 4) || '—'
                const endY = m.is_active ? 'oggi' : (m.data_fine?.slice(0, 4) || '—')
                return (
                  <tr key={m.nome} onClick={() => setSelected(m)} className={`border-b border-mil-sand-dark hover:bg-mil-sand-dark/50 transition-colors cursor-pointer ${selected?.nome === m.nome ? 'bg-mil-olive/10 border-l-2 border-l-mil-olive' : i % 2 ? 'bg-mil-sand/30' : ''}`}>
                    <td className="px-2 py-2 text-center">
                      <div className={m.is_active ? 'led-active mx-auto' : 'led-inactive mx-auto'} />
                    </td>
                    <td className="px-3 py-2 font-medium text-[#1B3A5C]">{m.nome}</td>
                    <td className="px-3 py-2 text-mil-steel">{m.paese}</td>
                    <td className="px-3 py-2">
                      <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[9px] font-bold text-white" style={{ backgroundColor: ORG_COLORS[m.tipo_missione] || '#8B9298' }}>
                        {m.tipo_missione}
                      </span>
                    </td>
                    <td className="px-3 py-2 font-mono text-mil-steel">{startY}–{endY}</td>
                    <td className="px-3 py-2 font-mono font-bold text-mil-navy">{m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') : '—'}</td>
                    <td className="px-3 py-2 font-mono text-[#8B9298]">{dur} anni</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </motion.div>

      {/* Drawer */}
      {selected && (
        <div className="fixed top-12 right-0 w-80 h-[calc(100vh-48px)] bg-white border-l border-[#D4CFC3] shadow-lg z-50 overflow-y-auto">
          <div className="bg-[#1B3A5C] p-4 text-white flex items-start justify-between">
            <div>
              <p className="text-[9px] uppercase tracking-[0.15em] text-[#8B9298]">Scheda Missione</p>
              <h2 className="text-[13px] font-bold mt-1">{selected.nome}</h2>
            </div>
            <button onClick={() => setSelected(null)} className="text-[#8B9298] hover:text-white"><X className="w-4 h-4" /></button>
          </div>
          <div className="p-4 space-y-3">
            {[['Stato', selected.is_active ? 'In corso' : 'Conclusa'],
              ['Teatro', selected.paese],
              ['Regione', selected.regione],
              ['Organizzazione', selected.tipo_missione],
              ['Inizio', selected.data_inizio?.slice(0, 10) || '—'],
              ['Fine', selected.is_active ? 'In corso' : (selected.data_fine?.slice(0, 10) || '—')],
              ['Durata', `${durationYears(selected)} anni`],
              ['Personale totale', selected.personale_totale ? Math.round(selected.personale_totale).toLocaleString('it-IT') : '—'],
              ['Personale militare', selected.personale_militare ? Math.round(selected.personale_militare).toLocaleString('it-IT') : '—'],
              ['Personale civile', selected.personale_civile ? Math.round(selected.personale_civile).toLocaleString('it-IT') : '—'],
              ['Commitment', selected.commitment || '—'],
              ['Costo (quota ITA)', selected.costo_totale && selected.costo_totale > 0 ? `€ ${Math.round(selected.costo_totale).toLocaleString('it-IT')}` : '—'],
            ].map(([label, val]) => (
              <div key={label} className="flex justify-between items-baseline border-b border-[#EAE6DC] pb-1.5">
                <span className="text-[9px] uppercase tracking-[0.1em] text-[#8B9298] font-semibold">{label}</span>
                <span className="text-[13px] font-mono text-[#1B3A5C]">{val}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
