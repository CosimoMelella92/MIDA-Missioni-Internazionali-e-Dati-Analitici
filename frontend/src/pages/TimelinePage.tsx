import { useMemo, useState } from 'react'
import { useData } from '../context/DataProvider'
import { ORG_COLORS, HISTORICAL_EVENTS } from '../lib/constants'

export default function TimelinePage() {
  const { missions, loading } = useData()
  const [orgFilter, setOrgFilter] = useState('')
  const [rangeStart, setRangeStart] = useState(1948)
  const [rangeEnd, setRangeEnd] = useState(2026)

  const data = useMemo(() => {
    let m = missions.filter(x => x.data_inizio)
    if (orgFilter) m = m.filter(x => x.tipo_missione === orgFilter)
    return m
      .map(x => ({
        ...x,
        startYear: new Date(x.data_inizio).getFullYear(),
        endYear: x.data_fine && x.data_fine !== 'NaT' ? new Date(x.data_fine).getFullYear() : 2026,
      }))
      .filter(x => x.endYear >= rangeStart && x.startYear <= rangeEnd)
      .sort((a, b) => a.startYear - b.startYear || a.endYear - b.endYear)
  }, [missions, orgFilter, rangeStart, rangeEnd])

  // Active missions count per year for overlay line
  const activePerYear = useMemo(() => {
    const counts: Record<number, number> = {}
    for (let y = rangeStart; y <= rangeEnd; y++) counts[y] = 0
    data.forEach(m => {
      for (let y = Math.max(m.startYear, rangeStart); y <= Math.min(m.endYear, rangeEnd); y++) counts[y]++
    })
    return counts
  }, [data, rangeStart, rangeEnd])
  const maxActive = Math.max(...Object.values(activePerYear), 1)

  const range = rangeEnd - rangeStart || 1
  const orgs = [...new Set(missions.map(m => m.tipo_missione))].sort()
  const visibleEvents = HISTORICAL_EVENTS.filter(e => e.year >= rangeStart && e.year <= rangeEnd)

  // Decade ticks
  const ticks: number[] = []
  for (let y = Math.ceil(rangeStart / 10) * 10; y <= rangeEnd; y += 10) ticks.push(y)

  if (loading) return <div className="flex items-center justify-center h-96 bg-[#F5F3EE]"><div className="animate-spin rounded-full h-8 w-8 border-2 border-[#4A5D23] border-t-transparent" /></div>

  return (
    <div className="max-w-7xl mx-auto px-3 md:px-4 py-4 md:py-6 space-y-3 md:space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-2 md:gap-3">
        <div>
          <h1 className="text-base md:text-2xl font-bold text-[#1B3A5C] uppercase tracking-wide">Cronologia Operativa</h1>
          <p className="text-[10px] md:text-[11px] text-[#8B9298]">{data.length} missioni · {rangeStart}-{rangeEnd}</p>
        </div>
        <select value={orgFilter} onChange={e => setOrgFilter(e.target.value)} className="px-2 py-2 md:py-1.5 rounded border border-[#D4CFC3] bg-white text-[11px]">
          <option value="">Tutte le org.</option>
          {orgs.map(o => <option key={o} value={o}>{o}</option>)}
        </select>
      </div>

      {/* Zoom slider */}
      <div className="bg-white border border-[#D4CFC3] rounded p-3">
        <div className="flex items-center justify-between mb-2">
          <span className="text-[10px] font-bold uppercase tracking-widest text-[#5A5F63]">Periodo</span>
          <span className="text-xs font-mono font-bold text-[#1B3A5C]">{rangeStart} — {rangeEnd}</span>
        </div>
        <div className="flex items-center gap-2 md:gap-4">
          <span className="text-[10px] font-mono text-[#8B9298] w-8 text-center flex-shrink-0">{rangeStart}</span>
          <input type="range" min={1948} max={2020} value={rangeStart} onChange={e => setRangeStart(+e.target.value)} className="flex-1 accent-[#4A5D23] h-2" />
          <input type="range" min={rangeStart + 1} max={2026} value={rangeEnd} onChange={e => setRangeEnd(+e.target.value)} className="flex-1 accent-[#4A5D23] h-2" />
          <span className="text-[10px] font-mono text-[#8B9298] w-8 text-center flex-shrink-0">{rangeEnd}</span>
        </div>
      </div>

      {/* Gantt Chart */}
      <div className="bg-white border border-[#D4CFC3] rounded shadow-sm overflow-x-auto -mx-1 md:mx-0">
        <div className="relative min-w-[360px] md:min-w-[800px]" style={{ minHeight: data.length * 20 + 60 }}>
          {/* Year axis */}
          <div className="sticky top-0 z-10 h-6 border-b border-[#D4CFC3] bg-white">
            {ticks.map(y => (
              <span key={y} className="absolute text-[9px] font-mono text-[#8B9298]" style={{ left: `${((y - rangeStart) / range) * 100}%`, transform: 'translateX(-50%)' }}>{y}</span>
            ))}
          </div>

          {/* Historical event markers */}
          {visibleEvents.map(ev => {
            const left = ((ev.year - rangeStart) / range) * 100
            return (
              <div key={ev.year} className="absolute top-6 bottom-0 z-[5]" style={{ left: `${left}%` }}>
                <div className="w-px h-full bg-[#8B1A1A]/20" />
                <div className="absolute top-0 -translate-x-1/2 bg-[#8B1A1A]/90 text-white text-[7px] px-1 py-px rounded-b font-bold uppercase tracking-wider whitespace-nowrap">{ev.label}</div>
              </div>
            )
          })}

          {/* Active count overlay (mini area) */}
          <svg className="absolute top-6 left-0 w-full pointer-events-none" style={{ height: data.length * 18 }} viewBox={`0 0 1000 100`} preserveAspectRatio="none">
            <path
              d={`M ${Object.entries(activePerYear).map(([y, v]) => `${((+y - rangeStart) / range) * 1000},${100 - (v / maxActive) * 80}`).join(' L ')} L 1000,100 L 0,100 Z`}
              fill="rgba(74,93,35,0.06)" stroke="rgba(74,93,35,0.15)" strokeWidth="1"
            />
          </svg>

          {/* Mission bars */}
          <div className="relative" style={{ paddingTop: 28 }}>
            {data.map((m, i) => {
              const left = Math.max(((m.startYear - rangeStart) / range) * 100, 0)
              const right = Math.min(((m.endYear - rangeStart) / range) * 100, 100)
              const width = Math.max(right - left, 0.5)
              return (
                <div key={m.nome + i} className="relative group" style={{ height: 18, marginBottom: 2 }}>
                  <div
                    className="absolute top-0.5 h-4 md:h-3.5 rounded-sm cursor-pointer transition-all hover:h-5 hover:top-0 hover:shadow-md"
                    style={{
                      left: `${left}%`,
                      width: `${width}%`,
                      backgroundColor: ORG_COLORS[m.tipo_missione] || '#8B9298',
                      opacity: m.is_active ? 1 : 0.55,
                    }}
                  />
                  {/* Tooltip */}
                  <div className="absolute hidden group-hover:block z-20 bg-white shadow-lg rounded p-2.5 text-[10px] whitespace-nowrap border border-[#D4CFC3]"
                    style={{ left: `${Math.min(left, 60)}%`, top: -48 }}>
                    <p className="font-bold text-[#1B3A5C] text-xs">{m.nome}</p>
                    <p className="text-[#5A5F63]">{m.paese} · {m.tipo_missione} · {m.startYear}-{m.is_active ? <span className="text-[#4A5D23] font-bold">in corso</span> : m.endYear}</p>
                    {m.personale_totale ? <p className="font-mono font-bold text-[#1B3A5C]">{Math.round(m.personale_totale).toLocaleString('it-IT')} pers.</p> : null}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      </div>

      {/* Legend + stats */}
      <div className="flex flex-wrap items-center gap-2 md:gap-3">
        <div className="flex flex-wrap gap-2 md:gap-3">
          {Object.entries(ORG_COLORS).filter(([k]) => k !== 'Altro').map(([org, color]) => (
            <div key={org} className="flex items-center gap-1">
              <div className="w-2.5 h-2.5 md:w-3 md:h-3 rounded-sm" style={{ backgroundColor: color }} />
              <span className="text-[9px] md:text-[10px] font-bold uppercase tracking-wider text-[#5A5F63]">{org}</span>
            </div>
          ))}
        </div>
        <div className="flex items-center gap-2 ml-auto">
          <div className="w-3 h-px bg-[#8B1A1A]/40" />
          <span className="text-[8px] md:text-[9px] text-[#5A5F63] uppercase tracking-wider">Eventi storici</span>
        </div>
      </div>
    </div>
  )
}
