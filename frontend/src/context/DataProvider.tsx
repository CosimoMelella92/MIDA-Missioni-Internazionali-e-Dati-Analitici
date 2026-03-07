import { createContext, useContext, useState, useEffect, type ReactNode } from 'react'
import type { Mission, Stats } from '../lib/types'

interface DataCtx {
  missions: Mission[]
  active: Mission[]
  stats: Stats | null
  loading: boolean
}

const DataContext = createContext<DataCtx>({ missions: [], active: [], stats: null, loading: true })

export function DataProvider({ children }: { children: ReactNode }) {
  const [missions, setMissions] = useState<Mission[]>([])
  const [stats, setStats] = useState<Stats | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      fetch('/data/missions.json').then(r => r.json()),
      fetch('/data/stats.json').then(r => r.json()),
    ])
      .then(([m, s]) => { setMissions(m); setStats(s) })
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [])

  const active = missions.filter(m => m.is_active)

  return (
    <DataContext.Provider value={{ missions, active, stats, loading }}>
      {children}
    </DataContext.Provider>
  )
}

export function useData() {
  return useContext(DataContext)
}
