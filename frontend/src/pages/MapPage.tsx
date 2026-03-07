import { useEffect, useRef, useMemo, useState } from 'react'
import { useData } from '../context/DataProvider'
import { GEOCODING, ROMA, ORG_COLORS } from '../lib/constants'
import L from 'leaflet'

function fmtNum(n: number) { return Math.round(n).toLocaleString('it-IT') }
function forceBar(mil: number, civ: number) {
  const total = mil + civ
  if (!total) return ''
  const milPct = Math.round((mil / total) * 100)
  return `<div style="display:flex;height:4px;border-radius:2px;overflow:hidden;margin-top:3px">
    <div style="width:${milPct}%;background:#4A5D23" title="Militari ${fmtNum(mil)}"></div>
    <div style="width:${100 - milPct}%;background:#2C5F8A" title="Civili ${fmtNum(civ)}"></div>
  </div>
  <div style="display:flex;justify-content:space-between;font-size:7px;color:#8B9298;margin-top:1px">
    <span>MIL ${fmtNum(mil)}</span><span>CIV ${fmtNum(civ)}</span>
  </div>`
}

export default function MapPage() {
  const { active, loading } = useData()
  const mapRef = useRef<HTMLDivElement>(null)
  const mapInstance = useRef<L.Map | null>(null)
  const [panelOpen, setPanelOpen] = useState(true)
  const [orgFilter, setOrgFilter] = useState('')

  const filteredActive = useMemo(() => {
    if (!orgFilter) return active
    return active.filter(m => m.tipo_missione === orgFilter)
  }, [active, orgFilter])

  const byRegion = useMemo(() => {
    const groups: Record<string, typeof active> = {}
    filteredActive.forEach(m => {
      const r = m.regione || 'Altro'
      if (!groups[r]) groups[r] = []
      groups[r].push(m)
    })
    return Object.entries(groups).sort((a, b) => b[1].length - a[1].length)
  }, [filteredActive])

  const totalPers = useMemo(() => filteredActive.reduce((s, m) => s + (m.personale_totale || 0), 0), [filteredActive])
  const totalMil = useMemo(() => filteredActive.reduce((s, m) => s + (m.personale_militare || 0), 0), [filteredActive])
  const totalCiv = useMemo(() => filteredActive.reduce((s, m) => s + (m.personale_civile || 0), 0), [filteredActive])
  const orgs = [...new Set(active.map(m => m.tipo_missione))].sort()

  useEffect(() => {
    if (loading || !mapRef.current) return
    if (mapInstance.current) { mapInstance.current.remove(); mapInstance.current = null }

    const map = L.map(mapRef.current, { zoomControl: false }).setView([30, 22], 3)
    mapInstance.current = map
    L.control.zoom({ position: 'topright' }).addTo(map)

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; OSM &copy; CARTO', maxZoom: 18,
    }).addTo(map)

    // Roma COI — command HQ
    const romaIcon = L.divIcon({
      html: `<div style="width:16px;height:16px;background:#8B1A1A;border:2px solid #fff;border-radius:2px;box-shadow:0 0 10px rgba(139,26,26,0.7)"></div>`,
      iconSize: [16, 16], iconAnchor: [8, 8], className: '',
    })
    L.marker(ROMA, { icon: romaIcon })
      .bindPopup(`<div style="font-family:Inter,sans-serif;padding:2px 0">
        <div style="font-size:11px;font-weight:800;color:#8B1A1A;letter-spacing:0.1em">ROMA — COI</div>
        <div style="font-size:9px;color:#5A5F63;margin-top:2px">Comando Operativo di Vertice Interforze</div>
        <div style="font-size:9px;color:#5A5F63">Stato Maggiore della Difesa</div>
      </div>`)
      .addTo(map)

    // Group by country
    const byCountry: Record<string, typeof active> = {}
    filteredActive.forEach(m => {
      const key = m.paese || 'Unknown'
      if (!byCountry[key]) byCountry[key] = []
      byCountry[key].push(m)
    })

    Object.entries(byCountry).forEach(([paese, missions]) => {
      const coords = GEOCODING[paese]
      if (!coords) return
      const pers = missions.reduce((s, m) => s + (m.personale_totale || 0), 0)
      const mil = missions.reduce((s, m) => s + (m.personale_militare || 0), 0)
      const civ = missions.reduce((s, m) => s + (m.personale_civile || 0), 0)
      const radius = Math.max(5, Math.min(22, Math.sqrt(pers) / 1.6))
      const mainOrg = [...missions].sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0))[0].tipo_missione
      const color = ORG_COLORS[mainOrg] || '#5A5F63'

      // Strategic line Roma → theatre
      L.polyline([ROMA, coords], { color: '#6B8C2A', weight: 1, opacity: 0.2, dashArray: '4 6' }).addTo(map)

      // Proportional marker
      L.circleMarker(coords, {
        radius, color, fillColor: color, fillOpacity: 0.75, weight: 2,
      })
        .bindPopup(`
          <div style="font-family:Inter,sans-serif;min-width:220px;max-width:300px">
            <div style="background:#1B3A5C;margin:-16px -16px 8px;padding:8px 12px;border-radius:4px 4px 0 0">
              <div style="font-size:8px;color:#8B9298;text-transform:uppercase;letter-spacing:0.2em">Teatro Operativo</div>
              <div style="font-size:14px;font-weight:800;color:#fff;letter-spacing:0.05em">${paese}</div>
              <div style="font-size:9px;color:#D4CFC3;margin-top:2px">${missions.length} missione/i · ${fmtNum(pers)} unità</div>
            </div>
            ${missions.map(m => {
              const milP = m.personale_militare || 0
              const civP = m.personale_civile || 0
              return `
              <div style="padding:4px 0;border-bottom:1px solid rgba(74,93,35,0.2);margin-bottom:4px">
                <div style="display:flex;align-items:center;gap:6px">
                  <div style="width:3px;height:28px;border-radius:2px;background:${ORG_COLORS[m.tipo_missione] || '#999'}"></div>
                  <div style="flex:1;min-width:0">
                    <div style="font-size:11px;font-weight:700;color:#F5F3EE">${m.nome}</div>
                    <div style="font-size:8px;color:#8B9298;text-transform:uppercase;letter-spacing:0.08em">${m.tipo_missione} · ${m.commitment || 'N/D'}</div>
                    <div style="display:flex;gap:8px;margin-top:2px;font-size:9px">
                      <span style="color:#6B8C2A;font-weight:700">${m.personale_totale ? fmtNum(m.personale_totale) + ' pers.' : ''}</span>
                      ${milP ? `<span style="color:#4A5D23;font-size:8px">MIL ${fmtNum(milP)}</span>` : ''}
                      ${civP ? `<span style="color:#2C5F8A;font-size:8px">CIV ${fmtNum(civP)}</span>` : ''}
                    </div>
                  </div>
                </div>
              </div>`
            }).join('')}
            <div style="margin-top:4px">
              <div style="display:flex;justify-content:space-between;font-size:10px;font-weight:700;color:#6B8C2A">
                <span>FORZA TOTALE</span><span>${fmtNum(pers)}</span>
              </div>
              ${forceBar(mil, civ)}
            </div>
          </div>
        `, { className: 'dark-popup', maxWidth: 320 })
        .addTo(map)
    })

    return () => { map.remove(); mapInstance.current = null }
  }, [filteredActive, loading])

  if (loading) return (
    <div className="flex items-center justify-center bg-[#1A1A1A]" style={{ height: 'calc(100vh - 48px)' }}>
      <div className="text-center">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-[#4A5D23] border-t-transparent mx-auto" />
        <p className="mt-3 text-[10px] text-[#8B9298] uppercase tracking-[0.2em]">Inizializzazione C2...</p>
      </div>
    </div>
  )

  return (
    <div className="relative" style={{ height: 'calc(100vh - 48px)' }}>
      <div ref={mapRef} className="absolute inset-0 z-0" />

      {/* C2 top bar */}
      <div className="absolute top-0 left-0 right-0 z-[1000] bg-[#0F1419]/90 backdrop-blur-sm border-b border-[#3D4F1E]/40 px-3 py-2 flex items-center justify-between gap-2">
        <div className="flex items-center gap-3 min-w-0">
          <div className="w-2 h-2 rounded-full bg-[#4A5D23] animate-pulse flex-shrink-0" />
          <div className="min-w-0">
            <p className="text-[10px] font-bold uppercase tracking-[0.15em] text-white truncate">
              Dispositivo — {filteredActive.length} Missioni Attive
            </p>
            <p className="text-[8px] text-[#8B9298] font-mono">
              {fmtNum(totalPers)} UNITA · MIL {fmtNum(totalMil)} · CIV {fmtNum(totalCiv)}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          <select
            value={orgFilter}
            onChange={e => setOrgFilter(e.target.value)}
            className="bg-[#1A2332] border border-[#3D4F1E]/50 text-[9px] text-white rounded px-2 py-1 outline-none"
          >
            <option value="">TUTTE ORG.</option>
            {orgs.map(o => <option key={o} value={o}>{o}</option>)}
          </select>
          <button
            onClick={() => setPanelOpen(!panelOpen)}
            className="text-[8px] text-[#8B9298] hover:text-white border border-[#3D4F1E]/50 rounded px-2 py-1 uppercase tracking-wider"
          >
            {panelOpen ? 'NASCONDI' : 'PANNELLO'}
          </button>
        </div>
      </div>

      {/* Intelligence panel */}
      {panelOpen && (
        <div className="absolute bottom-0 left-0 right-0 md:bottom-auto md:right-auto md:top-14 md:left-3 z-[1000] md:w-80 max-h-[45vh] md:max-h-[calc(100vh-100px)] overflow-y-auto bg-[#0F1419]/92 backdrop-blur-sm md:rounded-lg border-t md:border border-[#3D4F1E]/40 text-white">
          {/* Mobile drag handle */}
          <div className="md:hidden flex justify-center py-2">
            <div className="w-10 h-1 rounded-full bg-[#3D4F1E]" />
          </div>

          {/* Force composition summary */}
          <div className="px-3 pb-2 border-b border-[#3D4F1E]/40">
            <div className="flex items-center justify-between">
              <p className="text-[8px] uppercase tracking-[0.2em] text-[#6B8C2A] font-bold">Composizione Forze</p>
              <p className="text-[8px] text-[#8B9298] font-mono">{Object.keys(GEOCODING).length - 1} TEATRI</p>
            </div>
            <div className="flex gap-3 mt-2">
              <div className="flex-1 bg-[#1A2332] rounded p-2">
                <p className="text-[16px] font-mono font-bold text-white leading-none">{fmtNum(totalMil)}</p>
                <p className="text-[7px] uppercase tracking-[0.1em] text-[#4A5D23] mt-0.5">Militari</p>
              </div>
              <div className="flex-1 bg-[#1A2332] rounded p-2">
                <p className="text-[16px] font-mono font-bold text-white leading-none">{fmtNum(totalCiv)}</p>
                <p className="text-[7px] uppercase tracking-[0.1em] text-[#2C5F8A] mt-0.5">Civili</p>
              </div>
              <div className="flex-1 bg-[#1A2332] rounded p-2">
                <p className="text-[16px] font-mono font-bold text-[#6B8C2A] leading-none">{fmtNum(totalPers)}</p>
                <p className="text-[7px] uppercase tracking-[0.1em] text-[#8B9298] mt-0.5">Totale</p>
              </div>
            </div>
            {/* Force bar */}
            <div className="flex h-1.5 rounded-full overflow-hidden mt-2">
              <div className="bg-[#4A5D23]" style={{ width: totalPers ? `${(totalMil / totalPers) * 100}%` : '0' }} />
              <div className="bg-[#2C5F8A]" style={{ width: totalPers ? `${(totalCiv / totalPers) * 100}%` : '0' }} />
            </div>
          </div>

          {/* By region */}
          {byRegion.map(([region, missions]) => {
            const regMil = missions.reduce((s, m) => s + (m.personale_militare || 0), 0)
            const regCiv = missions.reduce((s, m) => s + (m.personale_civile || 0), 0)
            const regTot = missions.reduce((s, m) => s + (m.personale_totale || 0), 0)
            return (
              <div key={region} className="border-b border-[#3D4F1E]/25">
                <div className="px-3 py-1.5 bg-[#1A2332]/60 flex items-center justify-between">
                  <div className="flex items-center gap-1.5">
                    <span className="text-[9px] font-bold uppercase tracking-widest text-[#6B8C2A]">{region}</span>
                    <span className="text-[8px] text-[#8B9298]">({missions.length})</span>
                  </div>
                  <span className="text-[8px] font-mono text-[#8B9298]">{fmtNum(regTot)} · M{fmtNum(regMil)} C{fmtNum(regCiv)}</span>
                </div>
                {[...missions].sort((a, b) => (b.personale_totale || 0) - (a.personale_totale || 0)).map(m => (
                  <div key={m.nome} className="px-3 py-1.5 flex items-center gap-2 hover:bg-[#3D4F1E]/15 transition-colors">
                    <div className="w-1.5 h-1.5 rounded-full bg-[#4A5D23] flex-shrink-0 animate-pulse" />
                    <div className="min-w-0 flex-1">
                      <p className="text-[10px] font-semibold truncate">{m.nome}</p>
                      <div className="flex items-center gap-2 text-[8px] text-[#8B9298]">
                        <span>{m.paese}</span>
                        <span className="font-mono">{m.personale_totale ? fmtNum(m.personale_totale) : '—'}</span>
                        {m.personale_militare ? <span className="text-[#4A5D23]">M{fmtNum(m.personale_militare)}</span> : null}
                        {m.personale_civile ? <span className="text-[#2C5F8A]">C{fmtNum(m.personale_civile)}</span> : null}
                      </div>
                    </div>
                    <div className="w-1.5 h-5 rounded-full flex-shrink-0" style={{ backgroundColor: ORG_COLORS[m.tipo_missione] || '#999' }} />
                  </div>
                ))}
              </div>
            )
          })}

          {/* Legend */}
          <div className="p-3">
            <p className="text-[7px] uppercase tracking-widest text-[#8B9298] mb-1.5">Organizzazioni</p>
            <div className="flex flex-wrap gap-x-3 gap-y-1">
              {Object.entries(ORG_COLORS).filter(([k]) => k !== 'Altro').map(([org, color]) => (
                <div key={org} className="flex items-center gap-1">
                  <div className="w-2 h-2 rounded-full" style={{ backgroundColor: color }} />
                  <span className="text-[8px] font-bold uppercase tracking-wider">{org}</span>
                </div>
              ))}
            </div>
            <div className="flex gap-3 mt-2 text-[7px] text-[#8B9298] uppercase tracking-wider">
              <span><span className="inline-block w-3 h-1 bg-[#4A5D23] rounded mr-1" />Militari</span>
              <span><span className="inline-block w-3 h-1 bg-[#2C5F8A] rounded mr-1" />Civili</span>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
