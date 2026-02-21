import { useEffect, useRef, useMemo } from 'react'
import { useMissions } from '../hooks/useMissions'
import { GEOCODING, ROMA, ORG_COLORS, MILITARY, COUNTRY_FLAGS } from '../lib/constants'
import L from 'leaflet'

export default function MapPage() {
  const { active, loading } = useMissions()
  const mapRef = useRef<HTMLDivElement>(null)
  const mapInstance = useRef<L.Map | null>(null)

  const byRegion = useMemo(() => {
    const groups: Record<string, typeof active> = {}
    active.forEach(m => {
      const r = m.regione || 'Altro'
      if (!groups[r]) groups[r] = []
      groups[r].push(m)
    })
    return Object.entries(groups).sort((a, b) => b[1].length - a[1].length)
  }, [active])

  const totalPers = useMemo(() => active.reduce((s, m) => s + (m.personale_totale || 0), 0), [active])

  useEffect(() => {
    if (loading || !mapRef.current || mapInstance.current) return

    const map = L.map(mapRef.current, { zoomControl: false }).setView([30, 22], 3)
    mapInstance.current = map
    L.control.zoom({ position: 'topright' }).addTo(map)

    // Dark tiles for war-room effect
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; OSM &copy; CARTO',
      maxZoom: 18,
    }).addTo(map)

    // Roma COI
    const romaIcon = L.divIcon({
      html: `<div style="width:14px;height:14px;background:#8B1A1A;border:2px solid #fff;border-radius:2px;box-shadow:0 0 8px rgba(139,26,26,0.6)"></div>`,
      iconSize: [14, 14], iconAnchor: [7, 7], className: '',
    })
    L.marker(ROMA, { icon: romaIcon })
      .bindPopup('<div style="font-family:Inter,sans-serif"><b style="color:#8B1A1A;font-size:13px">ROMA</b><br/><span style="font-size:10px;color:#5A5F63">Comando Operativo di Vertice Interforze (COI)</span></div>')
      .addTo(map)

    // Group by country
    const byCountry: Record<string, typeof active> = {}
    active.forEach(m => {
      const key = m.paese || 'Unknown'
      if (!byCountry[key]) byCountry[key] = []
      byCountry[key].push(m)
    })

    Object.entries(byCountry).forEach(([paese, missions]) => {
      const coords = GEOCODING[paese]
      if (!coords) return
      const pers = missions.reduce((s, m) => s + (m.personale_totale || 0), 0)
      const radius = Math.max(4, Math.min(20, Math.sqrt(pers) / 1.8))
      const mainOrg = [...missions].sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0))[0].tipo_missione
      const color = ORG_COLORS[mainOrg] || '#5A5F63'

      // Dashed line Roma → theatre
      L.polyline([ROMA, coords], { color: '#6B8C2A', weight: 1, opacity: 0.25, dashArray: '4 6' }).addTo(map)

      // Proportional marker with glow
      L.circleMarker(coords, {
        radius, color, fillColor: color, fillOpacity: 0.8, weight: 2,
        // @ts-ignore
        className: 'transition-all',
      })
        .bindPopup(`
          <div style="font-family:Inter,sans-serif;min-width:200px;max-width:280px">
            <div style="font-size:9px;color:#8B9298;text-transform:uppercase;letter-spacing:0.15em;border-bottom:1px solid #3D4F1E;padding-bottom:3px;margin-bottom:6px">${paese}</div>
            ${missions.map(m => `
              <div style="display:flex;align-items:center;gap:6px;margin-bottom:5px">
                <div style="width:3px;height:24px;border-radius:2px;background:${ORG_COLORS[m.tipo_missione] || '#999'}"></div>
                <div>
                  <div style="font-size:11px;font-weight:700;color:#F5F3EE">${m.nome}</div>
                  <div style="font-size:9px;color:#8B9298">${m.tipo_missione} · ${m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') + ' pers.' : ''}</div>
                </div>
              </div>
            `).join('')}
            <div style="font-size:10px;font-weight:700;color:#6B8C2A;border-top:1px solid #3D4F1E;padding-top:3px;margin-top:2px">Totale: ${Math.round(pers).toLocaleString('it-IT')} unità</div>
          </div>
        `, { className: 'dark-popup' })
        .addTo(map)
    })

    return () => { map.remove(); mapInstance.current = null }
  }, [active, loading])

  if (loading) return (
    <div className="flex items-center justify-center h-screen bg-mil-black">
      <div className="text-center">
        <div className="animate-spin rounded-full h-10 w-10 border-2 border-mil-olive border-t-transparent mx-auto" />
        <p className="mt-3 text-xs text-mil-steel uppercase tracking-[0.2em]">Caricamento teatro operativo...</p>
      </div>
    </div>
  )

  return (
    <div className="relative" style={{ height: 'calc(100vh - 56px)' }}>
      {/* Full-bleed map */}
      <div ref={mapRef} className="absolute inset-0 z-0" />

      {/* Overlay panel — left side */}
      <div className="absolute top-3 left-3 z-[1000] w-72 max-h-[calc(100vh-80px)] overflow-y-auto bg-mil-black/85 backdrop-blur-sm rounded-lg border border-mil-olive-dark/50 text-white">
        <div className="p-3 border-b border-mil-olive-dark/50">
          <p className="text-[9px] uppercase tracking-[0.2em] text-mil-sand-deep">Teatro Operativo Globale</p>
          <p className="text-lg font-bold">{active.length} Missioni</p>
          <p className="text-[10px] text-mil-sand-deep">{totalPers.toLocaleString('it-IT')} unità · {Object.keys(GEOCODING).length - 1} teatri</p>
        </div>

        {/* By region */}
        {byRegion.map(([region, missions]) => (
          <div key={region} className="border-b border-mil-olive-dark/30">
            <div className="px-3 py-1.5 bg-mil-olive-dark/30">
              <span className="text-[9px] font-bold uppercase tracking-widest text-mil-olive-light">{region}</span>
              <span className="text-[9px] text-mil-sand-deep ml-2">({missions.length})</span>
            </div>
            {[...missions].sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0)).map(m => (
              <div key={m.nome} className="px-3 py-1.5 flex items-center gap-2 hover:bg-mil-olive-dark/20 transition-colors cursor-default">
                <div className="led-active flex-shrink-0" style={{ width: 5, height: 5 }} />
                <div className="min-w-0 flex-1">
                  <p className="text-[10px] font-semibold truncate">{COUNTRY_FLAGS[m.paese] || ''} {m.nome}</p>
                  <p className="text-[8px] text-mil-sand-deep">{m.paese} · <span className="font-mono">{m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') : '—'}</span></p>
                </div>
                <div className="w-1.5 h-4 rounded-full flex-shrink-0" style={{ backgroundColor: ORG_COLORS[m.tipo_missione] || '#999' }} />
              </div>
            ))}
          </div>
        ))}

        {/* Legend */}
        <div className="p-3">
          <p className="text-[8px] uppercase tracking-widest text-mil-sand-deep mb-2">Organizzazioni</p>
          <div className="flex flex-wrap gap-2">
            {Object.entries(ORG_COLORS).filter(([k]) => k !== 'Altro').map(([org, color]) => (
              <div key={org} className="flex items-center gap-1">
                <div className="w-2 h-2 rounded-full" style={{ backgroundColor: color }} />
                <span className="text-[8px] font-bold uppercase tracking-wider">{org}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
