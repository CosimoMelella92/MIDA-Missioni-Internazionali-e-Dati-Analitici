import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts'
import { ORG_COLORS } from '../../lib/constants'

interface Props {
  data: Record<string, number>
}

const RADIAN = Math.PI / 180
function renderLabel({ cx, cy, midAngle, innerRadius, outerRadius, percent }: { cx: number; cy: number; midAngle: number; innerRadius: number; outerRadius: number; percent: number }) {
  if (percent < 0.05) return null
  const radius = innerRadius + (outerRadius - innerRadius) * 0.55
  const x = cx + radius * Math.cos(-midAngle * RADIAN)
  const y = cy + radius * Math.sin(-midAngle * RADIAN)
  return <text x={x} y={y} fill="#fff" textAnchor="middle" dominantBaseline="central" fontSize={9} fontWeight={700}>{`${(percent * 100).toFixed(0)}%`}</text>
}

export default function OrgDonut({ data }: Props) {
  const chartData = Object.entries(data).map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value)
  const total = chartData.reduce((s, d) => s + d.value, 0)

  return (
    <div className="bg-white border border-[#D4CFC3] rounded p-4">
      <h3 className="text-[13px] md:text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] pb-2 mb-3">Per Organizzazione</h3>
      <ResponsiveContainer width="100%" height={280}>
        <PieChart>
          <Pie data={chartData} cx="50%" cy="50%" innerRadius={50} outerRadius={90} paddingAngle={2} dataKey="value" stroke="none" label={renderLabel} labelLine={false}>
            {chartData.map((entry) => (
              <Cell key={entry.name} fill={ORG_COLORS[entry.name] || '#8B9298'} />
            ))}
          </Pie>
          {/* Center total */}
          <text x="50%" y="48%" textAnchor="middle" dominantBaseline="central" fill="#1B3A5C" fontSize={22} fontWeight={700} fontFamily="JetBrains Mono, monospace">{total}</text>
          <text x="50%" y="57%" textAnchor="middle" dominantBaseline="central" fill="#8B9298" fontSize={8} fontWeight={600} style={{ textTransform: 'uppercase' }}>MISSIONI</text>
          <Tooltip formatter={(v: number) => [v, 'Missioni']} contentStyle={{ fontSize: 11, borderRadius: 2, border: '1px solid #D4CFC3' }} />
          <Legend iconType="square" iconSize={8} wrapperStyle={{ fontSize: 10, fontWeight: 600 }} />
        </PieChart>
      </ResponsiveContainer>
    </div>
  )
}
