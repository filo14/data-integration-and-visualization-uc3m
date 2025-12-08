import React, { useState, useEffect, useMemo } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

export default function DataGraph() {
    const [rawData, setRawData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [selectedCountries, setSelectedCountries] = useState(['ESP', 'AUT', 'DEU']); // Default selection
    const [metric, setMetric] = useState('convicts_per_100000'); // 'convicts_per_100000' or 'immigration_per_100000'

    useEffect(() => {
        const fetchData = async () => {
            try {
                const response = await fetch('http://localhost:8000/api/full-data');
                const data = await response.json();
                setRawData(data);
                setLoading(false);
            } catch (error) {
                console.error('Error fetching data:', error);
                setLoading(false);
            }
        };

        fetchData();
    }, []);

    // Process data for Recharts
    // Structure needed: [{ year: 2018, ESP: 12.5, FRA: 10.2, ... }, { year: 2019, ... }]
    const chartData = useMemo(() => {
        if (!rawData.length) return [];

        const years = [...new Set(rawData.map(d => d.year))].sort();
        const processed = years.map(year => {
            const entry = { year };
            rawData.filter(d => d.year === year).forEach(d => {
                entry[d.country_iso3_id] = d[metric];
            });
            return entry;
        });
        return processed;
    }, [rawData, metric]);

    const availableCountries = useMemo(() => {
        // First, check which countries have data for all years 2018-2022
        const countryYears = new Map();
        rawData.forEach(d => {
            // Check if there is a valid value for the current metric
            if (d[metric] !== null && d[metric] !== undefined) {
                if (!countryYears.has(d.country_iso3_id)) {
                    countryYears.set(d.country_iso3_id, new Set());
                }
                countryYears.get(d.country_iso3_id).add(d.year);
            }
        });

        const validCountries = new Set();
        countryYears.forEach((years, iso3) => {
            // Ensure we have data for all 5 years (2018-2022)
            if (years.size >= 5) {
                validCountries.add(iso3);
            }
        });

        const countries = new Map();
        rawData.forEach(d => {
            if (validCountries.has(d.country_iso3_id)) {
                countries.set(d.country_iso3_id, d.country_name);
            }
        });
        return Array.from(countries.entries()).sort((a, b) => a[1].localeCompare(b[1]));
    }, [rawData, metric]);

    const handleCountryToggle = (iso3) => {
        setSelectedCountries(prev =>
            prev.includes(iso3)
                ? prev.filter(c => c !== iso3)
                : [...prev, iso3]
        );
    };

    const colors = ['#8884d8', '#82ca9d', '#ffc658', '#ff7300', '#0088FE', '#00C49F'];

    if (loading) return <div className="text-white text-center">Loading data...</div>;

    return (
        <div className="bg-white/5 p-6 rounded-xl backdrop-blur-sm border border-white/10">
            <div className="flex flex-col md:flex-row justify-between items-start mb-6 gap-4">
                <div>
                    <h3 className="text-xl font-bold text-white mb-2">Collected statistics</h3>
                    <div className="flex gap-2 bg-white/10 p-1 rounded-lg inline-flex">
                        <button
                            onClick={() => setMetric('convicts_per_100000')}
                            className={`px-3 py-1 rounded-md text-sm transition-colors ${metric === 'convicts_per_100000' ? 'bg-blue-500 text-white' : 'text-blue-200 hover:text-white'}`}
                        >
                            Convicts
                        </button>
                        <button
                            onClick={() => setMetric('immigration_per_100000')}
                            className={`px-3 py-1 rounded-md text-sm transition-colors ${metric === 'immigration_per_100000' ? 'bg-blue-500 text-white' : 'text-blue-200 hover:text-white'}`}
                        >
                            Immigration
                        </button>
                    </div>
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

            <div className="h-[400px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#ffffff20" />
                        <XAxis dataKey="year" stroke="#94a3b8" />
                        <YAxis stroke="#94a3b8" label={{ value: 'Per 100k Population', angle: -90, position: 'insideLeft', fill: '#94a3b8' }} />
                        <Tooltip
                            contentStyle={{ backgroundColor: '#1e293b', devder: 'none', borderRadius: '8px', color: '#f8fafc' }}
                            itemStyle={{ color: '#e2e8f0' }}
                        />
                        <Legend wrapperStyle={{ color: '#94a3b8' }} />
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
            <p className="text-xs text-blue-300/60 mt-4 text-center">
                Data source: World Bank, EU & UN (2018-2022)
            </p>
        </div>
    );
}
