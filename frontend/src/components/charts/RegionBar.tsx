import { BarChart, Bar, XAxis, YAxis, ResponsiveContainer, Tooltip, Cell } from 'recharts'
import { REGION_COLORS } from '../../lib/constants'

interface Props {
  data: Record<string, number>
}

export default function RegionBar({ data }: Props) {
  const chartData = Object.entries(data)
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => b.value - a.value)

  return (
    <div className="card-elevated">
      <h3 className="section-title">Per Teatro Operativo</h3>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData} layout="vertical" margin={{ left: 80 }}>
          <XAxis type="number" />
          <YAxis type="category" dataKey="name" width={80} tick={{ fontSize: 12 }} />
          <Tooltip formatter={(v: number) => [v, 'Missioni']} />
          <Bar dataKey="value" radius={[0, 4, 4, 0]}>
            {chartData.map((entry) => (
              <Cell key={entry.name} fill={REGION_COLORS[entry.name] || '#999'} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
