export function SkeletonBar({ className = '', style }: { className?: string; style?: React.CSSProperties }) {
  return <div className={`animate-pulse bg-[#EAE6DC] rounded ${className}`} style={style} />
}

export function SkeletonKpiStrip() {
  return (
    <div className="bg-white border border-[#D4CFC3] rounded grid grid-cols-2 md:grid-cols-5 divide-y md:divide-y-0 md:divide-x divide-[#D4CFC3] -mt-6 md:-mt-8 relative z-10">
      {[...Array(5)].map((_, i) => (
        <div key={i} className={`flex-1 text-center py-4 ${i === 4 ? 'col-span-2 md:col-span-1' : ''}`}>
          <SkeletonBar className="h-7 w-16 mx-auto mb-2" />
          <SkeletonBar className="h-2.5 w-20 mx-auto" />
        </div>
      ))}
    </div>
  )
}

export function SkeletonTable({ rows = 6 }: { rows?: number }) {
  return (
    <div className="bg-white border border-[#D4CFC3] rounded overflow-hidden">
      <div className="bg-[#1B3A5C] h-9" />
      {[...Array(rows)].map((_, i) => (
        <div key={i} className={`flex items-center gap-3 px-3 py-2.5 border-b border-[#EAE6DC] ${i % 2 ? 'bg-[#F5F3EE]' : ''}`}>
          <SkeletonBar className="w-1.5 h-1.5 rounded-full" />
          <SkeletonBar className="h-3 flex-[3]" />
          <SkeletonBar className="h-3 flex-[2]" />
          <SkeletonBar className="h-3 flex-1" />
          <SkeletonBar className="h-3 w-10" />
        </div>
      ))}
    </div>
  )
}

export function SkeletonChart() {
  return (
    <div className="bg-white border border-[#D4CFC3] rounded p-4">
      <SkeletonBar className="h-4 w-40 mb-4" />
      <div className="flex items-end gap-2 h-[200px] pt-4">
        {[40, 65, 80, 55, 90, 70, 45, 85].map((h, i) => (
          <SkeletonBar key={i} className="flex-1 rounded-t" style={{ height: `${h}%` }} />
        ))}
      </div>
    </div>
  )
}

export function SkeletonMap() {
  return (
    <div className="w-full h-[280px] md:h-[400px] border border-[#D4CFC3] rounded bg-[#1A1A1A] animate-pulse flex items-center justify-center">
      <div className="text-center">
        <div className="w-8 h-8 rounded-full border-2 border-[#4A5D23] border-t-transparent animate-spin mx-auto" />
        <p className="mt-2 text-[9px] text-[#8B9298] uppercase tracking-[0.15em]">Caricamento mappa...</p>
      </div>
    </div>
  )
}
