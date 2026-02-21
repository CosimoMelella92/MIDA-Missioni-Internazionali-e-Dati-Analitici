import { useAnimatedCounter } from '../../hooks/useAnimatedCounter'

interface Props {
  label: string
  value: number
  suffix?: string
}

export default function KpiCard({ label, value, suffix = '' }: Props) {
  const count = useAnimatedCounter(value)

  return (
    <div className="flex-1 text-center py-3">
      <p className="text-[28px] font-bold font-mono leading-none text-[#1B3A5C]">
        {count.toLocaleString('it-IT')}{suffix}
      </p>
      <p className="text-[10px] text-[#8B9298] uppercase tracking-[0.15em] font-semibold mt-1">{label}</p>
    </div>
  )
}
