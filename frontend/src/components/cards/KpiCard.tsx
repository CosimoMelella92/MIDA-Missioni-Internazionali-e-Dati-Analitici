import { motion } from 'framer-motion'
import { useAnimatedCounter } from '../../hooks/useAnimatedCounter'
import type { LucideIcon } from 'lucide-react'

interface Props {
  label: string
  value: number
  icon: LucideIcon
  color: string
  suffix?: string
  delay?: number
}

export default function KpiCard({ label, value, icon: Icon, color, suffix = '', delay = 0 }: Props) {
  const count = useAnimatedCounter(value)

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay, duration: 0.4 }}
      className="bg-gradient-to-br from-white to-mil-sand-dark border border-mil-sand-deep rounded-lg p-4 flex items-center gap-3"
    >
      <div className="w-10 h-10 rounded flex items-center justify-center flex-shrink-0" style={{ backgroundColor: color }}>
        <Icon className="w-5 h-5 text-white" />
      </div>
      <div className="min-w-0">
        <p className="text-2xl font-bold font-mono leading-none" style={{ color }}>
          {count.toLocaleString('it-IT')}{suffix}
        </p>
        <p className="text-[11px] text-mil-steel uppercase tracking-widest font-semibold mt-0.5">{label}</p>
      </div>
    </motion.div>
  )
}
