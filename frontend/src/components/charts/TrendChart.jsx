import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { THEME } from '../../config/theme';

export default function TrendChart({ data, color, domainMax }) {
    const strokeColor = color || THEME.colors.primary;

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
                        type="number"
                        domain={[2018, 2022]}
                        tickCount={5}
                        stroke={THEME.colors.text}
                        tick={{ fill: THEME.colors.text }}
                    />
                    <YAxis
                        stroke={THEME.colors.text}
                        tick={{ fill: THEME.colors.text }}
                        domain={[0, domainMax || 'auto']}
                    />
                    <Tooltip
                        contentStyle={{ backgroundColor: THEME.colors.tooltipBg, border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                        itemStyle={{ color: '#e2e8f0' }}
                        cursor={{ stroke: 'rgba(255, 255, 255, 0.1)' }}
                        labelFormatter={(year) => `Year: ${year}`}
                    />
                    <Line
                        type="monotone"
                        dataKey="value"
                        name="per 100k"
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
