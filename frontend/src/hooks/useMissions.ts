import { useState, useEffect } from 'react'
import type { Mission, Stats } from '../lib/types'

export function useMissions() {
  const [missions, setMissions] = useState<Mission[]>([])
  const [stats, setStats] = useState<Stats | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      fetch('/data/missions.json').then(r => r.json()),
      fetch('/data/stats.json').then(r => r.json()),
    ])
      .then(([m, s]) => {
        setMissions(m)
        setStats(s)
      })
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [])

  const active = missions.filter(m => m.is_active)

  return { missions, active, stats, loading }
}
