import { useState, useEffect } from 'react'
import { NavLink, useLocation } from 'react-router-dom'

const links = [
  { to: '/', label: 'Situazione', icon: '◉' },
  { to: '/dashboard', label: 'Analisi', icon: '◈' },
  { to: '/missions', label: 'Registro', icon: '▤' },
  { to: '/timeline', label: 'Timeline', icon: '▬' },
  { to: '/map', label: 'Dispositivo', icon: '◎' },
  { to: '/about', label: 'Info', icon: 'ⓘ' },
]

export default function Navbar() {
  const [open, setOpen] = useState(false)
  const location = useLocation()

  // Close mobile menu on route change
  useEffect(() => { setOpen(false) }, [location.pathname])

  // Prevent body scroll when menu is open
  useEffect(() => {
    if (open) document.body.style.overflow = 'hidden'
    else document.body.style.overflow = ''
    return () => { document.body.style.overflow = '' }
  }, [open])

  return (
    <nav className="sticky top-0 z-50 bg-[#1B3A5C] safe-area-top">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-12">
          <NavLink to="/" className="flex items-center gap-2">
            <img src="/emblema_repubblica.svg" alt="" className="w-6 h-6" />
            <span className="text-[14px] font-bold text-white tracking-[0.15em]">MIDA</span>
          </NavLink>

          {/* Desktop nav */}
          <div className="hidden md:flex items-center gap-0.5">
            {links.map(l => (
              <NavLink
                key={l.to}
                to={l.to}
                end={l.to === '/'}
                className={({ isActive }) =>
                  `px-3 py-1.5 rounded text-[11px] font-semibold uppercase tracking-[0.1em] transition-colors ${isActive ? 'bg-[#4A5D23] text-white' : 'text-[#D4CFC3] hover:text-white hover:bg-white/5'}`
                }
              >
                {l.label}
              </NavLink>
            ))}
          </div>

          <div className="hidden lg:flex items-center gap-3">
            <button onClick={() => window.dispatchEvent(new KeyboardEvent('keydown', { key: 'k', ctrlKey: true }))} className="flex items-center gap-1.5 px-2 py-1 rounded border border-[#2C5F8A]/40 text-[9px] text-[#8B9298] hover:text-white hover:border-[#8B9298]/60 transition-colors">
              <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="7" cy="7" r="5" /><path d="M11 11l3.5 3.5" /></svg>
              <span className="font-mono">Ctrl+K</span>
            </button>
            <span className="text-[9px] uppercase tracking-[0.2em] text-[#8B9298]">
              Stato Maggiore Difesa
            </span>
          </div>

          {/* Hamburger — 44px touch target */}
          <button onClick={() => setOpen(!open)} className="md:hidden text-white w-11 h-11 flex items-center justify-center -mr-2 active:bg-white/10 rounded-lg transition-colors" aria-label="Menu">
            <svg width="22" height="22" viewBox="0 0 22 22" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
              {open
                ? <><line x1="6" y1="6" x2="16" y2="16" /><line x1="16" y1="6" x2="6" y2="16" /></>
                : <><line x1="4" y1="6" x2="18" y2="6" /><line x1="4" y1="11" x2="18" y2="11" /><line x1="4" y1="16" x2="18" y2="16" /></>}
            </svg>
          </button>
        </div>
      </div>

      {/* Mobile menu — full-screen overlay */}
      {open && (
        <>
          <div className="md:hidden fixed inset-0 top-12 bg-black/40 z-40" onClick={() => setOpen(false)} />
          <div className="md:hidden fixed left-0 right-0 top-12 z-50 bg-[#15304D] border-t border-[#2C5F8A]/30 shadow-2xl animate-slideDown max-h-[calc(100vh-48px)] overflow-y-auto">
            {links.map(l => (
              <NavLink
                key={l.to}
                to={l.to}
                end={l.to === '/'}
                onClick={() => setOpen(false)}
                className={({ isActive }) =>
                  `flex items-center gap-3 px-5 py-4 text-[13px] font-semibold uppercase tracking-[0.12em] border-b border-[#1B3A5C]/60 transition-colors active:bg-white/5 ${isActive ? 'bg-[#4A5D23] text-white' : 'text-[#D4CFC3]'}`
                }
              >
                <span className="text-[16px] opacity-60 w-6 text-center">{l.icon}</span>
                {l.label}
              </NavLink>
            ))}
            <div className="px-5 py-3 text-[8px] uppercase tracking-[0.2em] text-[#8B9298]/60 text-center">
              Stato Maggiore della Difesa
            </div>
          </div>
        </>
      )}
    </nav>
  )
}
