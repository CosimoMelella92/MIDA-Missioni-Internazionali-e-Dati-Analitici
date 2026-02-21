import { motion } from 'framer-motion'
import { Shield, Users, Globe, Crosshair, Building2, MapPin, Calendar } from 'lucide-react'
import KpiCard from '../components/cards/KpiCard'
import OrgDonut from '../components/charts/OrgDonut'
import RegionBar from '../components/charts/RegionBar'
import DecadeBar from '../components/charts/DecadeBar'
import { useMissions } from '../hooks/useMissions'
import { ORG_COLORS, MILITARY } from '../lib/constants'

export default function HomePage() {
  const { missions, active, stats, loading } = useMissions()

  if (loading || !stats) {
    return (
      <div className="flex items-center justify-center h-96 bg-mil-sand">
        <div className="text-center">
          <div className="animate-spin rounded-full h-10 w-10 border-2 border-mil-olive border-t-transparent mx-auto" />
          <p className="mt-3 text-sm text-mil-steel uppercase tracking-widest">Caricamento dati operativi...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="max-w-7xl mx-auto px-4 py-6 space-y-6">
      {/* BRIEFING HEADER */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="bg-mil-navy rounded-lg p-6 text-white"
      >
        <div className="flex items-start justify-between">
          <div>
            <p className="text-[10px] uppercase tracking-[0.3em] text-mil-sand-deep mb-1">Rapporto Situazione — Febbraio 2026</p>
            <h1 className="text-2xl md:text-3xl font-bold tracking-tight">
              Missioni Internazionali Italiane
            </h1>
            <p className="text-sm text-mil-sand-dark mt-2 max-w-xl">
              {stats.total} missioni dal 1948 ad oggi · {stats.active} operazioni in corso ·{' '}
              {stats.personnel.toLocaleString('it-IT')} unità di personale impiegato
            </p>
          </div>
          <div className="hidden md:flex flex-col items-end text-right">
            <p className="text-[10px] uppercase tracking-widest text-mil-sand-deep">Fonte</p>
            <a href="https://www.difesa.it/operazionimilitari/" className="text-xs text-mil-sand-dark hover:text-white underline" target="_blank" rel="noopener">
              Ministero della Difesa
            </a>
            <a href="https://www.analisidifesa.it/" className="text-xs text-mil-sand-dark hover:text-white underline" target="_blank" rel="noopener">
              Analisi Difesa
            </a>
          </div>
        </div>
      </motion.div>

      {/* KPI STRIP */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <KpiCard label="Missioni Totali" value={stats.total} icon={Shield} color={MILITARY.navy} delay={0} />
        <KpiCard label="In Corso" value={stats.active} icon={Crosshair} color={MILITARY.olive} delay={0.05} />
        <KpiCard label="Personale" value={stats.personnel} icon={Users} color={MILITARY.red} delay={0.1} />
        <KpiCard label="Teatri Operativi" value={stats.countries} icon={Globe} color={MILITARY.khaki} delay={0.15} />
        <KpiCard label="Organizzazioni" value={stats.organizations} icon={Building2} color={MILITARY.steel} delay={0.2} />
      </div>

      {/* DISPOSITIVO OPERATIVO — Active missions */}
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.25 }}
        className="card-elevated"
      >
        <h2 className="section-title">Dispositivo Operativo — {active.length} Missioni in Corso</h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-2">
          {[...active]
            .sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0))
            .map((m, i) => (
              <div
                key={m.nome}
                className="flex items-center gap-2.5 p-2.5 rounded border border-mil-sand-dark hover:border-mil-olive hover:bg-mil-sand-dark/50 transition-all group"
              >
                <div className="w-1 h-10 rounded-full flex-shrink-0" style={{ backgroundColor: ORG_COLORS[m.tipo_missione] || '#999' }} />
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-1">
                    <p className="font-semibold text-xs truncate text-mil-navy">{m.nome}</p>
                  </div>
                  <div className="flex items-center gap-2 text-[10px] text-mil-steel mt-0.5">
                    <span className="flex items-center gap-0.5"><MapPin className="w-2.5 h-2.5" />{m.paese}</span>
                    <span className="badge text-white" style={{ backgroundColor: ORG_COLORS[m.tipo_missione] || '#999', fontSize: '8px', padding: '1px 4px' }}>{m.tipo_missione}</span>
                    <span className="font-mono font-bold">{m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') : '—'}</span>
                  </div>
                </div>
              </div>
            ))}
        </div>
      </motion.div>

      {/* ANALISI — Charts */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <OrgDonut data={stats.by_org} />
        <RegionBar data={stats.by_region} />
        <DecadeBar data={stats.by_decade} />
      </div>
    </div>
  )
}
