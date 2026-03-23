import { useAnimatedCounter } from '../../hooks/useAnimatedCounter'

interface Props {
  label: string
  value: number
  suffix?: string
}

export default function KpiCard({ label, value, suffix = '' }: Props) {
  const count = useAnimatedCounter(value)

  return (
    <div className="flex-1 text-center py-2.5 md:py-3 px-1">
      <p className="text-[20px] md:text-[28px] font-bold font-mono leading-none text-[#1B3A5C]">
        {count.toLocaleString('it-IT')}{suffix}
      </p>
      <p className="text-[8px] md:text-[10px] text-[#8B9298] uppercase tracking-[0.12em] md:tracking-[0.15em] font-semibold mt-1 truncate">{label}</p>
    </div>
  )
}
