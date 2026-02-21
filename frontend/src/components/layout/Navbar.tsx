import { NavLink } from 'react-router-dom'
import { Shield, Moon, Sun } from 'lucide-react'
import { useState } from 'react'

export default function Navbar() {
  const [dark, setDark] = useState(false)

  const toggle = () => {
    setDark(!dark)
    document.documentElement.classList.toggle('dark')
  }

  const links = [
    { to: '/', label: 'Home' },
    { to: '/dashboard', label: 'Dashboard' },
    { to: '/missions', label: 'Missioni' },
    { to: '/timeline', label: 'Timeline' },
    { to: '/map', label: 'Mappa' },
  ]

  return (
    <nav className="sticky top-0 z-50 bg-white/80 dark:bg-mida-dark/80 backdrop-blur-md border-b border-gray-200 dark:border-gray-700">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          <NavLink to="/" className="flex items-center gap-2">
            <Shield className="w-8 h-8 text-mida-navy dark:text-mida-teal" />
            <span className="text-xl font-bold text-mida-navy dark:text-white">MIDA</span>
          </NavLink>

          <div className="hidden md:flex items-center gap-1">
            {links.map(l => (
              <NavLink
                key={l.to}
                to={l.to}
                end={l.to === '/'}
                className={({ isActive }) =>
                  `nav-link ${isActive ? 'nav-link-active' : 'text-gray-600 dark:text-gray-300'}`
                }
              >
                {l.label}
              </NavLink>
            ))}
          </div>

          <button onClick={toggle} className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors">
            {dark ? <Sun className="w-5 h-5 text-mida-gold" /> : <Moon className="w-5 h-5 text-mida-navy" />}
          </button>
        </div>
      </div>
    </nav>
  )
}
