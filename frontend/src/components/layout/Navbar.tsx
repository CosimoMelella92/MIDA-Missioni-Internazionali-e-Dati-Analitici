import { NavLink } from 'react-router-dom'
import { Shield, Map, BarChart3, List, Clock, Globe } from 'lucide-react'

const links = [
  { to: '/', label: 'Situazione', icon: Shield },
  { to: '/dashboard', label: 'Analisi', icon: BarChart3 },
  { to: '/missions', label: 'Missioni', icon: List },
  { to: '/timeline', label: 'Cronologia', icon: Clock },
  { to: '/map', label: 'Teatro Operativo', icon: Globe },
]

export default function Navbar() {
  return (
    <nav className="sticky top-0 z-50 bg-mil-olive-dark border-b-2 border-mil-olive-light">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-14">
          <NavLink to="/" className="flex items-center gap-2.5">
            <div className="w-8 h-8 bg-mil-olive-light rounded flex items-center justify-center">
              <Map className="w-5 h-5 text-white" />
            </div>
            <div className="leading-tight">
              <span className="text-sm font-bold text-white tracking-widest">MIDA</span>
              <span className="hidden sm:block text-[10px] text-mil-sand-deep tracking-wide uppercase">Missioni Internazionali</span>
            </div>
          </NavLink>

          <div className="flex items-center gap-0.5">
            {links.map(l => (
              <NavLink
                key={l.to}
                to={l.to}
                end={l.to === '/'}
                className={({ isActive }) =>
                  `nav-link flex items-center gap-1.5 ${isActive ? 'nav-link-active' : 'text-mil-sand-dark hover:text-white hover:bg-mil-olive/50'}`
                }
              >
                <l.icon className="w-3.5 h-3.5" />
                <span className="hidden lg:inline text-xs">{l.label}</span>
              </NavLink>
            ))}
          </div>

          <div className="text-[10px] text-mil-sand-deep uppercase tracking-wide hidden md:block">
            Stato Maggiore Difesa
          </div>
        </div>
      </div>
    </nav>
  )
}
