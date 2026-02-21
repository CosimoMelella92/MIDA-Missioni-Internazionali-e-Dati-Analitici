import { NavLink } from 'react-router-dom'

const links = [
  { to: '/', label: 'Situazione' },
  { to: '/dashboard', label: 'Analisi' },
  { to: '/missions', label: 'Registro' },
  { to: '/timeline', label: 'Timeline' },
  { to: '/map', label: 'Dispositivo' },
]

export default function Navbar() {
  return (
    <nav className="sticky top-0 z-50 bg-[#1B3A5C]">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-12">
          <NavLink to="/" className="flex items-center gap-2">
            <img src="/emblema_repubblica.svg" alt="" className="w-6 h-6" />
            <span className="text-[14px] font-bold text-white tracking-[0.15em]">MIDA</span>
          </NavLink>

          <div className="flex items-center gap-0.5">
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

          <span className="text-[9px] uppercase tracking-[0.2em] text-[#8B9298] hidden md:block">
            Stato Maggiore Difesa
          </span>
        </div>
      </div>
    </nav>
  )
}
