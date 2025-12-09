import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';

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
                    <CartesianGrid strokeDasharray="3 3" stroke="#ffffff20" />
                    <XAxis
                        dataKey="year"
                        stroke="#94a3b8"
                        tick={{ fill: '#94a3b8' }}
                        type="number"
                        domain={[2018, 2022]}
                        tickCount={5}
                    />

                    <YAxis
                        stroke="#94a3b8"
                        tick={{ fill: '#94a3b8' }}
                    />

                    <Tooltip
                        contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                        itemStyle={{ color: '#e2e8f0' }}
                        cursor={{ stroke: 'rgba(255, 255, 255, 0.1)' }}
                        labelFormatter={(year) => `Year: ${year}`}
                    />
                    <Legend wrapperStyle={{ color: '#94a3b8' }} />

                    <Line
                        type="monotone"
                        dataKey="crime"
                        name="Crime Rate"
                        stroke="#ef4444"
                        strokeWidth={4}
                        dot={{ r: 6, fill: '#ef4444', stroke: '#fff', strokeWidth: 3 }}
                        activeDot={{ r: 8 }}
                        animationDuration={500}
                    />
                    <Line
                        type="monotone"
                        dataKey="immigration"
                        name="Immigration Rate"
                        stroke="#10b981"
                        strokeWidth={4}
                        dot={{ r: 6, fill: '#10b981', stroke: '#fff', strokeWidth: 3 }}
                        activeDot={{ r: 8 }}
                        animationDuration={500}
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}
