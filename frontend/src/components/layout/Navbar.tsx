import { useState } from 'react'
import { NavLink } from 'react-router-dom'

const links = [
  { to: '/', label: 'Situazione' },
  { to: '/dashboard', label: 'Analisi' },
  { to: '/missions', label: 'Registro' },
  { to: '/timeline', label: 'Timeline' },
  { to: '/map', label: 'Dispositivo' },
]

export default function Navbar() {
  const [open, setOpen] = useState(false)

  return (
    <nav className="sticky top-0 z-50 bg-[#1B3A5C]">
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
                  `px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.1em] transition-colors ${isActive ? 'bg-[#4A5D23] text-white' : 'text-[#D4CFC3] hover:text-white'}`
                }
              >
                {l.label}
              </NavLink>
            ))}
          </div>

          <span className="text-[9px] uppercase tracking-[0.2em] text-[#8B9298] hidden lg:block">
            Stato Maggiore Difesa
          </span>

          {/* Hamburger */}
          <button onClick={() => setOpen(!open)} className="md:hidden text-white p-1" aria-label="Menu">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="2">
              {open
                ? <path d="M5 5l10 10M15 5L5 15" />
                : <path d="M3 5h14M3 10h14M3 15h14" />}
            </svg>
          </button>
        </div>
      </div>

      {/* Mobile menu */}
      {open && (
        <div className="md:hidden bg-[#15304D] border-t border-[#2C5F8A]/30">
          {links.map(l => (
            <NavLink
              key={l.to}
              to={l.to}
              end={l.to === '/'}
              onClick={() => setOpen(false)}
              className={({ isActive }) =>
                `block px-6 py-3 text-[12px] font-semibold uppercase tracking-[0.12em] border-b border-[#1B3A5C] transition-colors ${isActive ? 'bg-[#4A5D23] text-white' : 'text-[#D4CFC3]'}`
              }
            >
              {l.label}
            </NavLink>
          ))}
        </div>
      )}
    </nav>
  )
}
