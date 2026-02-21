import { motion } from 'framer-motion'
import { Shield, Users, Globe, Crosshair, Building2 } from 'lucide-react'
import KpiCard from '../components/cards/KpiCard'
import OrgDonut from '../components/charts/OrgDonut'
import RegionBar from '../components/charts/RegionBar'
import DecadeBar from '../components/charts/DecadeBar'
import { useMissions } from '../hooks/useMissions'

export default function HomePage() {
  const { missions, active, stats, loading } = useMissions()

  if (loading || !stats) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-mida-teal" />
      </div>
    )
  }

  return (
    <div className="max-w-7xl mx-auto px-4 py-8 space-y-8">
      {/* Hero */}
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="text-center py-12"
      >
        <h1 className="text-4xl md:text-5xl font-bold text-mida-navy dark:text-white mb-4">
          Missioni Internazionali Italiane
        </h1>
        <p className="text-lg text-gray-600 dark:text-gray-300 max-w-2xl mx-auto">
          {stats.total} missioni dal 1948 · {stats.active} attive nel 2026 · Dati verificati vs{' '}
          <a href="https://www.difesa.it/operazionimilitari/" className="text-mida-teal hover:underline" target="_blank" rel="noopener">
            difesa.it
          </a>
        </p>
      </motion.div>

      {/* KPI Grid */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <KpiCard label="Totali" value={stats.total} icon={Shield} color="#264653" />
        <KpiCard label="Attive" value={stats.active} icon={Crosshair} color="#2A9D8F" />
        <KpiCard label="Personale" value={stats.personnel} icon={Users} color="#E76F51" />
        <KpiCard label="Paesi" value={stats.countries} icon={Globe} color="#E9C46A" />
        <KpiCard label="Organizzazioni" value={stats.organizations} icon={Building2} color="#9467BD" />
      </div>

      {/* Active missions list */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.3 }}
        className="kpi-card"
      >
        <h3 className="text-lg font-semibold mb-4">Missioni Attive ({active.length})</h3>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {active
            .sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0))
            .map(m => (
              <div
                key={m.nome}
                className="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-gray-700/50 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
              >
                <div
                  className="w-2 h-8 rounded-full flex-shrink-0"
                  style={{ backgroundColor: orgColor(m.tipo_missione) }}
                />
                <div className="min-w-0">
                  <p className="font-medium text-sm truncate">{m.nome}</p>
                  <p className="text-xs text-gray-500 dark:text-gray-400">
                    {m.paese} · {m.tipo_missione} · {m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') : '—'} pers.
                  </p>
                </div>
              </div>
            ))}
        </div>
      </motion.div>

      {/* Charts Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <OrgDonut data={stats.by_org} />
        <RegionBar data={stats.by_region} />
        <DecadeBar data={stats.by_decade} />
      </div>
    </div>
  )
}

function orgColor(org: string): string {
  const colors: Record<string, string> = {
    ONU: '#1F77B4', NATO: '#2CA02C', UE: '#FF7F0E', ITA: '#D62728',
    Bilateral: '#9467BD', Multinational: '#8C564B', Coalizione: '#E377C2',
  }
  return colors[org] || '#999'
}
