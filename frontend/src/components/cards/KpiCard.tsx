import { motion } from 'framer-motion'
import { useAnimatedCounter } from '../../hooks/useAnimatedCounter'
import type { LucideIcon } from 'lucide-react'

interface Props {
  label: string
  value: number
  icon: LucideIcon
  color: string
  suffix?: string
}

export default function KpiCard({ label, value, icon: Icon, color, suffix = '' }: Props) {
  const count = useAnimatedCounter(value)

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className="kpi-card flex items-center gap-4"
    >
      <div className="p-3 rounded-xl" style={{ backgroundColor: `${color}15` }}>
        <Icon className="w-6 h-6" style={{ color }} />
      </div>
      <div>
        <p className="text-2xl font-bold font-mono" style={{ color }}>
          {count.toLocaleString('it-IT')}{suffix}
        </p>
        <p className="text-sm text-gray-500 dark:text-gray-400 uppercase tracking-wide">{label}</p>
      </div>
    </motion.div>
  )
}
