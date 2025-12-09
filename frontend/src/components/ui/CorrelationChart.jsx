import React from 'react';
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine, Label } from 'recharts';

export default function CorrelationChart({ data, correlation }) {
    // data format: [{ country: 'Name', crime: 100, immigration: 200 }, ...]

    return (
        <div className="w-full h-full relative">
            <div className="absolute top-0 right-0 p-2 bg-white/10 rounded-lg backdrop-blur-md border border-white/10 z-10">
                <p className="text-xs text-blue-200 uppercase tracking-widest font-bold">Correlation Coefficient</p>
                <p className={`text-2xl font-mono font-bold ${Math.abs(correlation) > 0.5 ? 'text-yellow-400' : 'text-white'}`}>
                    r = {correlation?.toFixed(3)}
                </p>
            </div>

            <ResponsiveContainer width="100%" height="100%">
                <ScatterChart
                    margin={{
                        top: 20,
                        right: 20,
                        bottom: 40,
                        left: 20,
                    }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke="#ffffff20" />
                    <XAxis
                        type="number"
                        dataKey="immigration"
                        name="Immigration"
                        stroke="#94a3b8"
                        tick={{ fill: '#94a3b8' }}
                        label={{ value: 'Immigration Rate', position: 'bottom', offset: 15, fill: '#94a3b8' }}
                    />
                    <YAxis
                        type="number"
                        dataKey="crime"
                        name="Crime"
                        stroke="#94a3b8"
                        tick={{ fill: '#94a3b8' }}
                        label={{ value: 'Crime Rate', angle: -90, position: 'insideLeft', fill: '#94a3b8', dy: 50 }}
                    />
                    <Tooltip
                        cursor={{ strokeDasharray: '3 3' }}
                        content={({ active, payload }) => {
                            if (active && payload && payload.length) {
                                const data = payload[0].payload;
                                return (
                                    <div className="bg-slate-900 border border-slate-700 p-3 rounded-lg shadow-xl text-white">
                                        <p className="font-bold border-b border-slate-700 pb-1 mb-2">{data.country}</p>
                                        <p className="text-emerald-400 text-sm">Immigration: {data.immigration}</p>
                                        <p className="text-red-400 text-sm">Crime: {data.crime}</p>
                                    </div>
                                );
                            }
                            return null;
                        }}
                    />
                    <Scatter name="Countries" data={data} fill="#60a5fa" shape="circle" />
                </ScatterChart>
            </ResponsiveContainer>
        </div>
    );
}
