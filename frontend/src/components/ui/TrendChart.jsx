import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export default function TrendChart({ data, color, domainMax }) {
    // Extract Tailwind color hex/class or use a default
    // We expect color to be a hex or we map it.
    // For simplicity, let's use a nice dynamic color or the one passed prop if it's a hex. 
    // If it's a tailwind class like 'bg-blue-500', we might need to compute it, 
    // but Recharts needs hex strings usually.
    // Let's assume we pass a hex color or use a default.
    const strokeColor = color || '#60a5fa'; // Fallback blue

    return (
        <div className="w-full h-full">
            <ResponsiveContainer width="100%" height="100%">
                <LineChart
                    data={data}
                    margin={{
                        top: 20,
                        right: 30,
                        left: 20,
                        bottom: 5,
                    }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke="#ffffff20" />
                    <XAxis
                        dataKey="year"
                        type="number"
                        domain={[2018, 2022]}
                        tickCount={5}
                        stroke="#94a3b8"
                        tick={{ fill: '#94a3b8' }}
                    />
                    <YAxis
                        stroke="#94a3b8"
                        tick={{ fill: '#94a3b8' }}
                        domain={[0, domainMax || 'auto']}
                        label={{ value: 'per 100k people', angle: -90, position: 'insideLeft', fill: '#94a3b8' }}
                    />
                    <Tooltip
                        contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                        itemStyle={{ color: '#e2e8f0' }}
                        cursor={{ stroke: 'rgba(255, 255, 255, 0.1)' }}
                        labelFormatter={(year) => `Year: ${year}`}
                    />
                    <Line
                        type="monotone"
                        dataKey="value"
                        name="Rate per 100k"
                        stroke={strokeColor}
                        strokeWidth={4}
                        dot={{ r: 6, fill: strokeColor, strokeWidth: 3, stroke: '#fff' }}
                        activeDot={{ r: 8 }}
                        animationDuration={500}
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}
