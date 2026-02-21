import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts'
import { ORG_COLORS } from '../../lib/constants'

interface Props {
  data: Record<string, number>
}

export default function OrgDonut({ data }: Props) {
  const chartData = Object.entries(data).map(([name, value]) => ({ name, value }))

  return (
    <div className="kpi-card">
      <h3 className="text-lg font-semibold mb-4">Per Organizzazione</h3>
      <ResponsiveContainer width="100%" height={300}>
        <PieChart>
          <Pie data={chartData} cx="50%" cy="50%" innerRadius={60} outerRadius={100} paddingAngle={3} dataKey="value">
            {chartData.map((entry) => (
              <Cell key={entry.name} fill={ORG_COLORS[entry.name] || '#999'} />
            ))}
          </Pie>
          <Tooltip formatter={(v: number) => [v, 'Missioni']} />
          <Legend />
        </PieChart>
      </ResponsiveContainer>
    </div>
  )
}
