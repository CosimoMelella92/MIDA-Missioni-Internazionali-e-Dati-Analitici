import { BarChart, Bar, XAxis, YAxis, ResponsiveContainer, Tooltip, LabelList } from 'recharts'

interface Props {
  data: Record<string, number>
}

export default function DecadeBar({ data }: Props) {
  const chartData = Object.entries(data)
    .map(([name, value]) => ({ name: name + 's', value }))
    .sort((a, b) => a.name.localeCompare(b.name))

  return (
    <div className="bg-white border border-[#D4CFC3] rounded p-4">
      <h3 className="text-[13px] md:text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] pb-2 mb-3">Evoluzione Storica</h3>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData}>
          <XAxis dataKey="name" tick={{ fontSize: 10 }} tickLine={false} axisLine={{ stroke: '#D4CFC3' }} />
          <YAxis tick={{ fontSize: 9 }} tickLine={false} axisLine={false} />
          <Tooltip formatter={(v: number) => [v, 'Missioni avviate']} contentStyle={{ fontSize: 11, borderRadius: 2, border: '1px solid #D4CFC3' }} />
          <Bar dataKey="value" fill="#4A5D23" radius={[3, 3, 0, 0]}>
            <LabelList dataKey="value" position="top" fontSize={9} fontWeight={700} fill="#1B3A5C" />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
