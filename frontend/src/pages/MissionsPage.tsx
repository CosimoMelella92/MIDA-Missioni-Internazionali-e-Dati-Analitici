import { useState, useMemo } from 'react'
import { Search, Download, ChevronUp, ChevronDown } from 'lucide-react'
import { useMissions } from '../hooks/useMissions'
import { ORG_COLORS } from '../lib/constants'

type SortKey = 'nome' | 'paese' | 'tipo_missione' | 'personale_totale' | 'data_inizio'
type SortDir = 'asc' | 'desc'

export default function MissionsPage() {
  const { missions, loading } = useMissions()
  const [search, setSearch] = useState('')
  const [orgFilter, setOrgFilter] = useState('')
  const [activeOnly, setActiveOnly] = useState(false)
  const [sortKey, setSortKey] = useState<SortKey>('nome')
  const [sortDir, setSortDir] = useState<SortDir>('asc')

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
    <div className="max-w-7xl mx-auto px-4 py-8 space-y-6">
      <h1 className="text-2xl font-bold text-mil-navy uppercase tracking-wide">Registro Missioni ({filtered.length})</h1>

      {/* Filters */}
      <div className="flex flex-wrap gap-3 items-center">
        <div className="relative flex-1 min-w-[200px]">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Cerca missione o paese..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="w-full pl-10 pr-4 py-2 rounded border border-mil-sand-deep bg-white focus:ring-2 focus:ring-mil-olive focus:border-transparent outline-none text-sm"
          />
        </div>
        <select
          value={orgFilter}
          onChange={e => setOrgFilter(e.target.value)}
          className="px-4 py-2 rounded border border-mil-sand-deep bg-white text-sm"
        >
          <option value="">Tutte le org.</option>
          {orgs.map(o => <option key={o} value={o}>{o}</option>)}
        </select>
        <label className="flex items-center gap-2 cursor-pointer">
          <input type="checkbox" checked={activeOnly} onChange={e => setActiveOnly(e.target.checked)} className="rounded text-mida-teal" />
          <span className="text-sm">Solo attive</span>
        </label>
        <button onClick={exportCsv} className="flex items-center gap-2 px-4 py-2 bg-mil-olive text-white rounded hover:bg-mil-olive-dark transition-colors text-sm font-semibold uppercase tracking-wider">
          <Download className="w-4 h-4" /> CSV
        </button>
      </div>

      {/* Table */}
      <div className="overflow-x-auto card-elevated !p-0">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-mil-navy text-white">
              {([['nome', 'Missione'], ['paese', 'Paese'], ['tipo_missione', 'Org.'], ['data_inizio', 'Inizio'], ['personale_totale', 'Personale']] as [SortKey, string][]).map(([key, label]) => (
                <th key={key} onClick={() => toggleSort(key)} className="px-4 py-3 text-left cursor-pointer hover:bg-mil-navy-light select-none text-xs uppercase tracking-wider">
                  {label} <SortIcon col={key} />
                </th>
              ))}
              <th className="px-4 py-3 text-left">Stato</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((m, i) => (
              <tr key={m.nome} className={`border-b border-mil-sand-dark hover:bg-mil-sand-dark/50 transition-colors ${i % 2 === 0 ? '' : 'bg-mil-sand/50'}`}>
                <td className="px-4 py-3 font-medium">{m.nome}</td>
                <td className="px-4 py-3">{m.paese}</td>
                <td className="px-4 py-3">
                  <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium text-white" style={{ backgroundColor: ORG_COLORS[m.tipo_missione] || '#999' }}>
                    {m.tipo_missione}
                  </span>
                </td>
                <td className="px-4 py-3 font-mono text-xs">{m.data_inizio?.slice(0, 4) || '—'}</td>
                <td className="px-4 py-3 font-mono">{m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') : '—'}</td>
                <td className="px-4 py-3">
                  {m.is_active
                    ? <span className="inline-flex items-center gap-1 text-xs font-bold text-mil-olive"><span className="w-2 h-2 rounded-full bg-mil-olive animate-pulse" />IN CORSO</span>
                    : <span className="text-xs text-mil-steel-light">CONCLUSA</span>
                  }
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
