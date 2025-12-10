import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ReferenceLine } from 'recharts';
import { THEME } from '../../config/theme';

export default function TopCountriesChart({ data, referenceLine }) {
    return (
        <div className="w-full h-full relative">
            {referenceLine && (
                <div className="absolute top-0 right-0 p-2 bg-white/10 rounded-lg backdrop-blur-md border border-white/10 z-10 pointer-events-none">
                    <p className="text-xs text-blue-200 tracking-widest font-bold">{referenceLine.label}</p>
                    <p className="text-2xl font-bold text-white">
                        {referenceLine.value}
                    </p>
                </div>
            )}
            <ResponsiveContainer width="100%" height="100%">
                <BarChart
                    data={data}
                    margin={{
                        top: 20,
                        right: 30,
                        left: 20,
                        bottom: 30,
                    }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke={THEME.colors.grid} />
                    <XAxis
                        dataKey="country"
                        stroke={THEME.colors.text}
                        tick={{ fill: THEME.colors.text, fontSize: 12 }}
                        interval={0}
                        angle={-45}
                        textAnchor="end"
                        height={60}
                    />
                    <YAxis
                        stroke={THEME.colors.text}
                        tick={{ fill: THEME.colors.text }}
                    />
                    <Tooltip
                        contentStyle={{ backgroundColor: THEME.colors.tooltipBg, border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                        itemStyle={{ color: '#e2e8f0' }}
                        cursor={{ fill: 'rgba(255, 255, 255, 0.05)' }}
                    />
                    <Legend formatter={(value) => <span style={{ color: THEME.colors.textStrong }}>{value}</span>} />
                    <Bar dataKey="crime" name="Crime Rate" fill={THEME.colors.crime} radius={[4, 4, 0, 0]} animationDuration={500} />
                    <Bar dataKey="immigration" name="Immigration Rate" fill={THEME.colors.immigration} radius={[4, 4, 0, 0]} animationDuration={500} />

                    {referenceLine && (
                        <ReferenceLine
                            y={referenceLine.value}
                            stroke={THEME.colors.textStrong}
                            strokeDasharray="5 5"
                            strokeWidth={1}
                        />
                    )}
                </BarChart>
            </ResponsiveContainer>
        </div >
    );
}
