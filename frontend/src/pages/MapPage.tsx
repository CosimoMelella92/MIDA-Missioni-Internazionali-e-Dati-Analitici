import { useEffect, useRef } from 'react'
import { useMissions } from '../hooks/useMissions'
import { GEOCODING, ROMA, ORG_COLORS } from '../lib/constants'
import L from 'leaflet'

export default function MapPage() {
  const { active, loading } = useMissions()
  const mapRef = useRef<HTMLDivElement>(null)
  const mapInstance = useRef<L.Map | null>(null)

  useEffect(() => {
    if (loading || !mapRef.current || mapInstance.current) return

    const map = L.map(mapRef.current).setView([30, 20], 3)
    mapInstance.current = map

    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; OpenStreetMap &copy; CARTO',
    }).addTo(map)

    // Roma marker
    L.circleMarker(ROMA, { radius: 8, color: '#D62728', fillColor: '#D62728', fillOpacity: 1, weight: 2 })
      .bindPopup('<b>Roma</b><br/>Comando operativo')
      .addTo(map)

    // Mission markers + lines
    active.forEach(m => {
      const coords = GEOCODING[m.paese]
      if (!coords) return
      const color = ORG_COLORS[m.tipo_missione] || '#999'

      // Line Roma → mission
      L.polyline([ROMA, coords], { color, weight: 1.5, opacity: 0.4, dashArray: '4 6' }).addTo(map)

      // Mission marker
      L.circleMarker(coords, { radius: 6, color, fillColor: color, fillOpacity: 0.8, weight: 1 })
        .bindPopup(`<b>${m.nome}</b><br/>${m.paese} · ${m.tipo_missione}<br/>Personale: ${m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') : '—'}`)
        .addTo(map)
    })

    return () => { map.remove(); mapInstance.current = null }
  }, [active, loading])

  if (loading) return <div className="flex items-center justify-center h-96"><div className="animate-spin rounded-full h-12 w-12 border-b-2 border-mida-teal" /></div>

  return (
    <div className="max-w-7xl mx-auto px-4 py-8 space-y-4">
      <h1 className="text-3xl font-bold text-mida-navy dark:text-white">Mappa Missioni Attive ({active.length})</h1>
      <div className="flex flex-wrap gap-3 mb-2">
        {Object.entries(ORG_COLORS).map(([org, color]) => (
          <div key={org} className="flex items-center gap-1.5 text-xs">
            <div className="w-3 h-3 rounded-full" style={{ backgroundColor: color }} />
            <span>{org}</span>
          </div>
        ))}
      </div>
      <div ref={mapRef} className="w-full h-[600px] rounded-xl shadow-lg border border-gray-200 dark:border-gray-700" />
    </div>
  )
}
