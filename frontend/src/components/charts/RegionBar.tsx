import { BarChart, Bar, XAxis, YAxis, ResponsiveContainer, Tooltip, Cell, LabelList } from 'recharts'
import { REGION_COLORS } from '../../lib/constants'

interface Props {
  data: Record<string, number>
}

export default function RegionBar({ data }: Props) {
  const chartData = Object.entries(data)
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => b.value - a.value)

  return (
    <div className="bg-white border border-[#D4CFC3] rounded p-4">
      <h3 className="text-[13px] md:text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] pb-2 mb-3">Per Teatro Operativo</h3>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData} layout="vertical" margin={{ left: 90, right: 30 }}>
          <XAxis type="number" tick={{ fontSize: 9 }} tickLine={false} axisLine={{ stroke: '#D4CFC3' }} />
          <YAxis type="category" dataKey="name" width={90} tick={{ fontSize: 10 }} tickLine={false} axisLine={false} />
          <Tooltip formatter={(v: number) => [v, 'Missioni']} contentStyle={{ fontSize: 11, borderRadius: 2, border: '1px solid #D4CFC3' }} />
          <Bar dataKey="value" radius={[0, 3, 3, 0]}>
            {chartData.map((entry) => (
              <Cell key={entry.name} fill={REGION_COLORS[entry.name] || '#8B9298'} />
            ))}
            <LabelList dataKey="value" position="right" fontSize={9} fontWeight={700} fill="#1B3A5C" />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
