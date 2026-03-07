import { useEffect, useRef, useMemo } from 'react'
import { useData } from '../context/DataProvider'
import { GEOCODING, ROMA, ORG_COLORS } from '../lib/constants'
import L from 'leaflet'

export default function MapPage() {
  const { active, loading } = useData()
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
    <div className="flex items-center justify-center h-screen bg-[#1A1A1A]">
      <div className="text-center">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-[#4A5D23] border-t-transparent mx-auto" />
        <p className="mt-3 text-[10px] text-[#8B9298] uppercase tracking-[0.2em]">Caricamento teatro operativo...</p>
      </div>
    </div>
  )

  return (
    <div className="relative" style={{ height: 'calc(100vh - 56px)' }}>
      {/* Full-bleed map */}
      <div ref={mapRef} className="absolute inset-0 z-0" />

      {/* Overlay panel — side on desktop, bottom sheet on mobile */}
      <div className="absolute bottom-0 left-0 right-0 md:bottom-auto md:right-auto md:top-3 md:left-3 z-[1000] md:w-72 max-h-[40vh] md:max-h-[calc(100vh-80px)] overflow-y-auto bg-[#1A1A1A]/90 backdrop-blur-sm md:rounded-lg border-t md:border border-[#3D4F1E]/50 text-white">
        <div className="p-3 border-b border-[#3D4F1E]/50">
          <p className="text-[9px] uppercase tracking-[0.2em] text-[#D4CFC3]">Teatro Operativo Globale</p>
          <p className="text-lg font-bold">{active.length} Missioni</p>
          <p className="text-[10px] text-[#D4CFC3]">{totalPers.toLocaleString('it-IT')} unità · {Object.keys(GEOCODING).length - 1} teatri</p>
        </div>

        {/* By region */}
        {byRegion.map(([region, missions]) => (
          <div key={region} className="border-b border-[#3D4F1E]/30">
            <div className="px-3 py-1.5 bg-[#3D4F1E]/30">
              <span className="text-[9px] font-bold uppercase tracking-widest text-[#6B8C2A]">{region}</span>
              <span className="text-[9px] text-[#D4CFC3] ml-2">({missions.length})</span>
            </div>
            {[...missions].sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0)).map(m => (
              <div key={m.nome} className="px-3 py-1.5 flex items-center gap-2 hover:bg-[#3D4F1E]/20 transition-colors cursor-default">
                <div className="w-1.5 h-1.5 rounded-full bg-[#4A5D23] flex-shrink-0" />
                <div className="min-w-0 flex-1">
                  <p className="text-[10px] font-semibold truncate">{m.nome}</p>
                  <p className="text-[8px] text-[#D4CFC3]">{m.paese} · <span className="font-mono">{m.personale_totale ? Math.round(m.personale_totale).toLocaleString('it-IT') : '—'}</span></p>
                </div>
                <div className="w-1.5 h-4 rounded-full flex-shrink-0" style={{ backgroundColor: ORG_COLORS[m.tipo_missione] || '#999' }} />
              </div>
            ))}
          </div>
        ))}

        {/* Legend */}
        <div className="p-3">
          <p className="text-[8px] uppercase tracking-widest text-[#D4CFC3] mb-2">Organizzazioni</p>
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
