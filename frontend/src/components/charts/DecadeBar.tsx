import { BarChart, Bar, XAxis, YAxis, ResponsiveContainer, Tooltip } from 'recharts'

interface Props {
  data: Record<string, number>
}

export default function DecadeBar({ data }: Props) {
  const chartData = Object.entries(data)
    .map(([name, value]) => ({ name: name + 's', value }))
    .sort((a, b) => a.name.localeCompare(b.name))

  return (
    <div className="kpi-card">
      <h3 className="text-lg font-semibold mb-4">Per Decennio</h3>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData}>
          <XAxis dataKey="name" tick={{ fontSize: 11 }} />
          <YAxis />
          <Tooltip formatter={(v: number) => [v, 'Missioni']} />
          <Bar dataKey="value" fill="#264653" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
