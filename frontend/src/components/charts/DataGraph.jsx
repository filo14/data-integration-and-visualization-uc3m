import { useState } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { THEME } from '../../config/theme';

export default function DataGraph({ data, availableCountries, metric, setMetric, loading }) {
    const [selectedCountries, setSelectedCountries] = useState(['ESP', 'AUT', 'DEU']); // Default selection

    const handleCountryToggle = (iso3) => {
        setSelectedCountries(prev =>
            prev.includes(iso3)
                ? prev.filter(c => c !== iso3)
                : [...prev, iso3]
        );
    };

    // High-contrast palette for dark mode
    const colors = [
        THEME.colors.accent,    // Yellow
        '#22d3ee',             // Cyan
        '#e879f9',             // Fuchsia
        '#a3e635',             // Lime
        '#fb923c',             // Orange
        THEME.colors.immigration, // Emerald
        THEME.colors.crime      // Red
    ];

    return (
        <div className="h-full flex flex-col">
            <div className="flex flex-col md:flex-row justify-between items-start mb-6 gap-4">
                <div className="flex gap-2 bg-white/10 p-1 rounded-lg inline-flex">
                    <button
                        onClick={() => setMetric('convicts_per_100000')}
                        className={`px-3 py-1 rounded-md text-sm transition-colors ${metric === 'convicts_per_100000' ? 'bg-blue-500 text-white' : 'text-blue-200 hover:text-white'}`}
                    >
                        Crime
                    </button>
                    <button
                        onClick={() => setMetric('immigration_per_100000')}
                        className={`px-3 py-1 rounded-md text-sm transition-colors ${metric === 'immigration_per_100000' ? 'bg-blue-500 text-white' : 'text-blue-200 hover:text-white'}`}
                    >
                        Immigration
                    </button>
                </div>

                <div className="md:w-64 max-h-32 overflow-y-auto bg-black/20 p-2 rounded border border-white/10">
                    <div className="text-xs text-blue-200 mb-2 sticky top-0 bg-transparent">Select Countries:</div>
                    <div className="grid grid-cols-2 gap-2">
                        {availableCountries.map(([iso3, name]) => (
                            <label key={iso3} className="flex items-center space-x-2 text-xs text-white cursor-pointer hover:bg-white/5 p-1 rounded">
                                <input
                                    type="checkbox"
                                    checked={selectedCountries.includes(iso3)}
                                    onChange={() => handleCountryToggle(iso3)}
                                    className="rounded border-white/30 bg-transparent text-blue-500 focus:ring-blue-500"
                                />
                                <span className="truncate" title={name}>{name}</span>
                            </label>
                        ))}
                    </div>
                </div>
            </div>

            <div className="flex-1 w-full min-h-0">
                <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={data} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={THEME.colors.grid} />
                        <XAxis dataKey="year" stroke={THEME.colors.text} />
                        <YAxis stroke={THEME.colors.text} label={{ value: 'per 100k people', angle: -90, position: 'insideLeft', fill: THEME.colors.textStrong }} />
                        <Tooltip
                            contentStyle={{ backgroundColor: THEME.colors.tooltipBg, border: 'none', borderRadius: '8px', color: '#f8fafc' }}
                            itemStyle={{ color: '#e2e8f0' }}
                            cursor={{ stroke: 'rgba(255, 255, 255, 0.1)' }}
                        />
                        <Legend wrapperStyle={{ color: THEME.colors.text }} dy={10} />
                        {selectedCountries.map((iso3, index) => (
                            <Line
                                key={iso3}
                                type="monotone"
                                dataKey={iso3}
                                name={availableCountries.find(c => c[0] === iso3)?.[1] || iso3}
                                stroke={colors[index % colors.length]}
                                strokeWidth={2}
                                dot={{ r: 4 }}
                                activeDot={{ r: 8 }}
                            />
                        ))}
                    </LineChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
}
