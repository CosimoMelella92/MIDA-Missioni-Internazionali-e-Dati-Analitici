import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts'
import { ORG_COLORS } from '../../lib/constants'

interface Props {
  data: Record<string, number>
}

export default function OrgDonut({ data }: Props) {
  const chartData = Object.entries(data).map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value)

  return (
    <div className="card-elevated">
      <h3 className="section-title">Per Organizzazione</h3>
      <ResponsiveContainer width="100%" height={280}>
        <PieChart>
          <Pie data={chartData} cx="50%" cy="50%" innerRadius={55} outerRadius={95} paddingAngle={2} dataKey="value" stroke="none">
            {chartData.map((entry) => (
              <Cell key={entry.name} fill={ORG_COLORS[entry.name] || '#8B9298'} />
            ))}
          </Pie>
          <Tooltip formatter={(v: number) => [v, 'Missioni']} contentStyle={{ fontSize: 12, borderRadius: 4 }} />
          <Legend iconType="square" iconSize={10} wrapperStyle={{ fontSize: 11, fontWeight: 600 }} />
        </PieChart>
      </ResponsiveContainer>
    </div>
  )
}
