interface Props {
  message?: string
  onReset?: () => void
}

export default function EmptyState({ message = 'Nessun risultato trovato', onReset }: Props) {
  return (
    <div className="bg-white border border-[#D4CFC3] rounded p-8 text-center">
      <svg width="40" height="40" viewBox="0 0 40 40" fill="none" className="mx-auto mb-3">
        <circle cx="20" cy="20" r="18" stroke="#D4CFC3" strokeWidth="2" />
        <path d="M14 20h12M20 14v12" stroke="#8B9298" strokeWidth="2" strokeLinecap="round" transform="rotate(45 20 20)" />
      </svg>
      <p className="text-[12px] text-[#5A5F63]">{message}</p>
      {onReset && (
        <button onClick={onReset} className="mt-3 px-4 py-1.5 bg-[#4A5D23] text-white rounded text-[10px] font-bold uppercase tracking-[0.1em] hover:bg-[#3D4F1E] transition-colors">
          Reset filtri
        </button>
      )}
    </div>
  )
}
