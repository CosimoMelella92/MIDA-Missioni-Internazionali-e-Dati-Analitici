import { useEffect, useRef } from 'react'
import { useMissions } from '../hooks/useMissions'
import { GEOCODING, ROMA, ORG_COLORS, MILITARY } from '../lib/constants'
import L from 'leaflet'

export default function MapPage() {
  const { active, loading } = useMissions()
  const mapRef = useRef<HTMLDivElement>(null)
  const mapInstance = useRef<L.Map | null>(null)

  useEffect(() => {
    if (loading || !mapRef.current || mapInstance.current) return

    const map = L.map(mapRef.current, { zoomControl: false }).setView([32, 25], 3)
    mapInstance.current = map

    L.control.zoom({ position: 'topright' }).addTo(map)

    L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; OpenStreetMap &copy; CARTO',
      maxZoom: 18,
    }).addTo(map)

    // Roma — Comando Operativo di Vertice Interforze (COI)
    const romaIcon = L.divIcon({
      html: `<div style="width:16px;height:16px;background:${MILITARY.red};border:2px solid white;border-radius:2px;box-shadow:0 1px 4px rgba(0,0,0,0.4)"></div>`,
      iconSize: [16, 16],
      iconAnchor: [8, 8],
      className: '',
    })
    L.marker(ROMA, { icon: romaIcon })
      .bindPopup('<div style="font-family:Inter,sans-serif"><b style="color:#8B1A1A">ROMA</b><br/><span style="font-size:11px">Comando Operativo di Vertice Interforze</span></div>')
      .addTo(map)

    // Group missions by country to avoid overlapping markers
    const byCountry: Record<string, typeof active> = {}
    active.forEach(m => {
      const key = m.paese || 'Unknown'
      if (!byCountry[key]) byCountry[key] = []
      byCountry[key].push(m)
    })

    Object.entries(byCountry).forEach(([paese, missions]) => {
      const coords = GEOCODING[paese]
      if (!coords) return

      // Size based on total personnel in this country
      const totalPers = missions.reduce((s, m) => s + (m.personale_totale || 0), 0)
      const radius = Math.max(5, Math.min(18, Math.sqrt(totalPers) / 2))
      const mainOrg = missions.sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0))[0].tipo_missione
      const color = ORG_COLORS[mainOrg] || '#5A5F63'

      // Line Roma → theatre
      L.polyline([ROMA, coords], {
        color: MILITARY.steel,
        weight: 1,
        opacity: 0.3,
        dashArray: '6 4',
      }).addTo(map)

      // Mission marker — size proportional to personnel
      L.circleMarker(coords, {
        radius,
        color: '#fff',
        fillColor: color,
        fillOpacity: 0.85,
        weight: 2,
      })
        .bindPopup(`
          <div style="font-family:Inter,sans-serif;min-width:180px">
            <div style="font-size:10px;color:#5A5F63;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:4px">${paese}</div>
            ${missions.map(m => `
              <div style="margin-bottom:4px;padding:3px 0;border-bottom:1px solid #EAE6DC">
                <b style="font-size:12px;color:#1B3A5C">${m.nome}</b><br/>
                <span style="font-size:10px;color:#5A5F63">${m.tipo_missione} · ${m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') + ' pers.' : ''}</span>
              </div>
            `).join('')}
            <div style="font-size:11px;font-weight:700;color:#4A5D23;margin-top:2px">Totale: ${Math.round(totalPers).toLocaleString('it-IT')} unità</div>
          </div>
        `)
        .addTo(map)
    })

    return () => { map.remove(); mapInstance.current = null }
  }, [active, loading])

  if (loading) return (
    <div className="flex items-center justify-center h-96 bg-mil-sand">
      <div className="text-center">
        <div className="animate-spin rounded-full h-10 w-10 border-2 border-mil-olive border-t-transparent mx-auto" />
        <p className="mt-3 text-sm text-mil-steel uppercase tracking-widest">Caricamento teatro operativo...</p>
      </div>
    </div>
  )

  // Count missions without geocoding
  const unmapped = active.filter(m => m.paese && !GEOCODING[m.paese])

  return (
    <div className="max-w-7xl mx-auto px-4 py-6 space-y-4">
      <div className="bg-mil-navy rounded-lg p-4 text-white flex items-center justify-between">
        <div>
          <p className="text-[10px] uppercase tracking-[0.3em] text-mil-sand-deep">Teatro Operativo</p>
          <h1 className="text-xl font-bold">{active.length} Missioni in {Object.keys(GEOCODING).length - 1} Teatri</h1>
        </div>
        <div className="flex flex-wrap gap-3">
          {Object.entries(ORG_COLORS).filter(([k]) => k !== 'Altro').map(([org, color]) => (
            <div key={org} className="flex items-center gap-1.5">
              <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: color }} />
              <span className="text-[10px] uppercase tracking-wider font-semibold">{org}</span>
            </div>
          ))}
        </div>
      </div>
      {unmapped.length > 0 && (
        <p className="text-xs text-mil-steel">Nota: {unmapped.length} missioni senza coordinate ({unmapped.map(m => m.paese).join(', ')})</p>
      )}
      <div ref={mapRef} className="w-full h-[600px] rounded-lg shadow-md border-2 border-mil-sand-deep" />
    </div>
  )
}
