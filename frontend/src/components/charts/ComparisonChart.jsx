import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { THEME } from '../../config/theme';

export default function ComparisonChart({ data }) {
    return (
        <div className="w-full h-full">
            <ResponsiveContainer width="100%" height="100%">
                <LineChart
                    data={data}
                    margin={{
                        top: 20,
                        right: 30,
                        left: 20,
                        bottom: 20,
                    }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke={THEME.colors.grid} />
                    <XAxis
                        dataKey="year"
                        stroke={THEME.colors.text}
                        tick={{ fill: THEME.colors.text }}
                        type="number"
                        domain={[2018, 2022]}
                        tickCount={5}
                    />

                    <YAxis
                        stroke={THEME.colors.text}
                        tick={{ fill: THEME.colors.text }}
                    />

                    <Tooltip
                        contentStyle={{ backgroundColor: THEME.colors.tooltipBg, border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                        itemStyle={{ color: '#e2e8f0' }}
                        cursor={{ stroke: 'rgba(255, 255, 255, 0.1)' }}
                        labelFormatter={(year) => `Year: ${year}`}
                    />
                    <Legend formatter={(value) => <span style={{ color: '#ffffff' }}>{value}</span>} />

                    <Line
                        type="monotone"
                        dataKey="crime"
                        name="Crime Rate"
                        stroke={THEME.colors.crime}
                        strokeWidth={4}
                        dot={{ r: 6, fill: THEME.colors.crime, stroke: '#fff', strokeWidth: 3 }}
                        activeDot={{ r: 8 }}
                        animationDuration={500}
                    />
                    <Line
                        type="monotone"
                        dataKey="immigration"
                        name="Immigration Rate"
                        stroke={THEME.colors.immigration}
                        strokeWidth={4}
                        dot={{ r: 6, fill: THEME.colors.immigration, stroke: '#fff', strokeWidth: 3 }}
                        activeDot={{ r: 8 }}
                        animationDuration={500}
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}
