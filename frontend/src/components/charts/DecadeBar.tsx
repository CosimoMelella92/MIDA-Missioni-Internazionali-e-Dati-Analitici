import { BarChart, Bar, XAxis, YAxis, ResponsiveContainer, Tooltip } from 'recharts'

interface Props {
  data: Record<string, number>
}

export default function DecadeBar({ data }: Props) {
  const chartData = Object.entries(data)
    .map(([name, value]) => ({ name: name + 's', value }))
    .sort((a, b) => a.name.localeCompare(b.name))

  return (
    <div className="card-elevated">
      <h3 className="section-title">Evoluzione Storica</h3>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData}>
          <XAxis dataKey="name" tick={{ fontSize: 11 }} />
          <YAxis />
          <Tooltip formatter={(v: number) => [v, 'Missioni']} />
          <Bar dataKey="value" fill="#4A5D23" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
