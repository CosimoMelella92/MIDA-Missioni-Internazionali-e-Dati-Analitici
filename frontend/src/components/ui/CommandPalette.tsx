import { useState, useEffect, useRef, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { useData } from '../../context/DataProvider'

export default function CommandPalette() {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const [idx, setIdx] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const navigate = useNavigate()
  const { missions } = useData()

  // Ctrl+K / Cmd+K to open
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault()
        setOpen(o => !o)
      }
      if (e.key === 'Escape') setOpen(false)
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  useEffect(() => {
    if (open) { inputRef.current?.focus(); setQuery(''); setIdx(0) }
  }, [open])

  const results = useMemo(() => {
    if (!query.trim()) return []
    const q = query.toLowerCase()
    const pages = [
      { type: 'Pagina', label: 'Situazione (Home)', path: '/' },
      { type: 'Pagina', label: 'Analisi (Dashboard)', path: '/dashboard' },
      { type: 'Pagina', label: 'Registro Operazioni', path: '/missions' },
      { type: 'Pagina', label: 'Cronologia Operativa', path: '/timeline' },
      { type: 'Pagina', label: 'Dispositivo (Mappa)', path: '/map' },
      { type: 'Pagina', label: 'Informazioni', path: '/about' },
    ].filter(p => p.label.toLowerCase().includes(q))

    const missionResults = missions
      .filter(m => m.nome.toLowerCase().includes(q) || m.paese?.toLowerCase().includes(q))
      .slice(0, 8)
      .map(m => ({ type: 'Missione', label: m.nome, sub: `${m.paese} · ${m.tipo_missione}`, path: '/missions' }))

    const countries = [...new Set(missions.map(m => m.paese))]
      .filter(c => c?.toLowerCase().includes(q))
      .slice(0, 5)
      .map(c => ({ type: 'Teatro', label: c, path: '/map' }))

    return [...pages, ...missionResults, ...countries] as { type: string; label: string; sub?: string; path: string }[]
  }, [query, missions])

  useEffect(() => { setIdx(0) }, [query])

  const handleSelect = (result: { path: string }) => {
    setOpen(false)
    navigate(result.path)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setIdx(i => Math.min(i + 1, results.length - 1)) }
    if (e.key === 'ArrowUp') { e.preventDefault(); setIdx(i => Math.max(i - 1, 0)) }
    if (e.key === 'Enter' && results[idx]) { e.preventDefault(); handleSelect(results[idx]) }
  }

  if (!open) return null

  return (
    <div className="fixed inset-0 z-[9999] flex items-start justify-center pt-[15vh]" onClick={() => setOpen(false)}>
      <div className="absolute inset-0 bg-black/40 backdrop-blur-sm" />
      <div className="relative w-full max-w-lg mx-4 bg-white rounded-lg shadow-2xl border border-[#D4CFC3] overflow-hidden" onClick={e => e.stopPropagation()}>
        {/* Input */}
        <div className="flex items-center gap-3 px-4 py-3 border-b border-[#D4CFC3]">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="#8B9298" strokeWidth="2" className="flex-shrink-0">
            <circle cx="7" cy="7" r="5" /><path d="M11 11l3.5 3.5" />
          </svg>
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={e => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Cerca missioni, paesi, pagine..."
            className="flex-1 text-[13px] outline-none bg-transparent text-[#1B3A5C] placeholder:text-[#8B9298]"
          />
          <kbd className="text-[9px] px-1.5 py-0.5 rounded border border-[#D4CFC3] text-[#8B9298] font-mono">ESC</kbd>
        </div>

        {/* Results */}
        {results.length > 0 && (
          <div className="max-h-[300px] overflow-y-auto py-1">
            {results.map((r, i) => (
              <button
                key={`${r.type}-${r.label}-${i}`}
                onClick={() => handleSelect(r)}
                className={`w-full text-left px-4 py-2 flex items-center gap-3 transition-colors ${i === idx ? 'bg-[#4A5D23]/10' : 'hover:bg-[#F5F3EE]'}`}
              >
                <span className="text-[8px] uppercase tracking-[0.1em] font-bold text-[#8B9298] w-14 flex-shrink-0">{r.type}</span>
                <div className="min-w-0 flex-1">
                  <p className="text-[12px] font-medium text-[#1B3A5C] truncate">{r.label}</p>
                  {r.sub && <p className="text-[10px] text-[#8B9298] truncate">{r.sub}</p>}
                </div>
              </button>
            ))}
          </div>
        )}

        {query && results.length === 0 && (
          <div className="px-4 py-6 text-center">
            <p className="text-[11px] text-[#8B9298]">Nessun risultato per "{query}"</p>
          </div>
        )}

        {!query && (
          <div className="px-4 py-4 text-center">
            <p className="text-[10px] text-[#8B9298] uppercase tracking-[0.1em]">Digita per cercare missioni, paesi o pagine</p>
          </div>
        )}
      </div>
    </div>
  )
}
