import { useMemo, useState } from 'react'
import { useMissions } from '../hooks/useMissions'
import { ORG_COLORS } from '../lib/constants'

export default function TimelinePage() {
  const { missions, loading } = useMissions()
  const [orgFilter, setOrgFilter] = useState('')

  const data = useMemo(() => {
    let m = missions.filter(x => x.data_inizio)
    if (orgFilter) m = m.filter(x => x.tipo_missione === orgFilter)
    return m
      .map(x => ({
        ...x,
        startYear: new Date(x.data_inizio).getFullYear(),
        endYear: x.data_fine && x.data_fine !== 'NaT' ? new Date(x.data_fine).getFullYear() : 2026,
      }))
      .sort((a, b) => a.startYear - b.startYear)
  }, [missions, orgFilter])

  const minYear = 1948
  const maxYear = 2026
  const range = maxYear - minYear

  const orgs = [...new Set(missions.map(m => m.tipo_missione))].sort()

  if (loading) return <div className="flex items-center justify-center h-96"><div className="animate-spin rounded-full h-12 w-12 border-b-2 border-mida-teal" /></div>

  return (
    <div className="max-w-7xl mx-auto px-4 py-8 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-3xl font-bold text-mida-navy dark:text-white">Timeline ({data.length})</h1>
        <select value={orgFilter} onChange={e => setOrgFilter(e.target.value)} className="px-4 py-2 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800">
          <option value="">Tutte le org.</option>
          {orgs.map(o => <option key={o} value={o}>{o}</option>)}
        </select>
      </div>

      {/* Year axis */}
      <div className="relative">
        <div className="flex justify-between text-xs text-gray-400 mb-2 px-1">
          {[1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020].map(y => (
            <span key={y} style={{ position: 'absolute', left: `${((y - minYear) / range) * 100}%` }}>{y}</span>
          ))}
        </div>

        <div className="space-y-1 mt-6">
          {data.map(m => {
            const left = ((m.startYear - minYear) / range) * 100
            const width = Math.max(((m.endYear - m.startYear) / range) * 100, 0.5)
            return (
              <div key={m.nome} className="relative h-6 group">
                <div
                  className="absolute h-5 rounded-sm cursor-pointer transition-all hover:h-6 hover:shadow-md"
                  style={{
                    left: `${left}%`,
                    width: `${width}%`,
                    backgroundColor: ORG_COLORS[m.tipo_missione] || '#999',
                    opacity: m.is_active ? 1 : 0.6,
                  }}
                  title={`${m.nome} (${m.startYear}-${m.is_active ? 'attiva' : m.endYear}) · ${m.tipo_missione} · ${m.paese}`}
                />
                <div className="absolute hidden group-hover:block z-10 bg-white dark:bg-gray-800 shadow-lg rounded-lg p-3 text-xs -top-16 whitespace-nowrap border border-gray-200 dark:border-gray-600"
                  style={{ left: `${left}%` }}>
                  <p className="font-bold">{m.nome}</p>
                  <p>{m.paese} · {m.tipo_missione} · {m.startYear}-{m.is_active ? 'attiva' : m.endYear}</p>
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* Legend */}
      <div className="flex flex-wrap gap-3 mt-4">
        {Object.entries(ORG_COLORS).map(([org, color]) => (
          <div key={org} className="flex items-center gap-1.5 text-xs">
            <div className="w-3 h-3 rounded-sm" style={{ backgroundColor: color }} />
            <span>{org}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
