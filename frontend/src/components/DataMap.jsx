import React, { useState, useMemo } from 'react';
import Plot from 'react-plotly.js';

export default function DataMap({ rawData, loading }) {
    const [year, setYear] = useState(2018);

    const mapData = useMemo(() => {
        if (!rawData || rawData.length === 0) return null;

        const currentYearData = rawData.filter(d => d.year === year);

        // Find max crime for color scaling (global max to keep scale consistent across years)
        const maxCrime = Math.max(...rawData.map(d => d.convicts_per_100000 || 0));

        return {
            locations: currentYearData.map(d => d.country_iso3_id),
            z: currentYearData.map(d => d.convicts_per_100000),
            text: currentYearData.map(d => `<b style="font-size: 14px; color: #60a5fa">${d.country_name}</b><br><span style="color: #94a3b8">Immigration:</span> <b style="color: white">${d.immigration_per_100000}</b> per 100k<br><span style="color: #94a3b8">Crime:</span> <b style="color: white">${d.convicts_per_100000}</b> per 100k`),
            // Marker sizing based on immigration
            marker: {
                size: currentYearData.map(d => (d.immigration_per_100000 || 0) / 15), // Scale factor adjusted
                color: currentYearData.map(d => d.convicts_per_100000),
                colorscale: [[0, '#ffffffff'], [1, '#ff0000ff']], // Red 400 to Red 900
                cmin: 0,
                cmax: maxCrime,
                opacity: 0.9,
                line: { color: 'rgba(255,255,255,0.8)', width: 1 },
                sizemode: 'area'
            }
        };

    }, [rawData, year]);

    if (loading) return <div className="text-white text-center">Loading map...</div>;

    return (
        <div className="bg-white/5 p-6 rounded-xl backdrop-blur-sm border border-white/10 shadow-2xl h-full flex flex-col">

            <h3 className="text-xl font-bold text-white mb-6 tracking-wide">Interactive Crime & Immigration Map</h3>

            <div className="flex-1 w-full min-h-[400px] rounded-lg overflow-hidden border border-white/10 relative">
                <Plot
                    data={[
                        {
                            type: 'scattergeo',
                            mode: 'markers',
                            locations: mapData?.locations,
                            locationmode: 'ISO-3',
                            text: mapData?.text,
                            marker: mapData?.marker,
                            hoverinfo: 'text'
                        }
                    ]}
                    layout={{
                        geo: {
                            bgcolor: 'rgba(0,0,0,0)',
                            showland: true,
                            landcolor: '#1e293b', // Slate 800
                            countrycolor: '#334155', // Slate 700
                            showlakes: false,
                            fitbounds: "locations", // Auto-zoom to data
                            projection: { type: 'mercator' }
                        },
                        paper_bgcolor: 'rgba(0,0,0,0)',
                        plot_bgcolor: 'rgba(0,0,0,0)',
                        margin: { l: 0, r: 0, t: 0, b: 0 },
                        showlegend: false,
                        hoverlabel: {
                            bgcolor: 'rgba(15, 23, 42, 0.85)', // translucent slate-900
                            bordercolor: 'rgba(255,255,255,0.1)',
                            font: { color: '#f8fafc', family: 'sans-serif' },
                            align: 'left'
                        },
                        font: { color: 'white' }
                    }}
                    style={{ width: '100%', height: '100%' }}
                    config={{ responsive: true, displayModeBar: false }}
                />
            </div>

            <div className="mt-6 flex items-center gap-4">
                <span className="text-white font-bold">Year: {year}</span>
                <input
                    type="range"
                    min="2018"
                    max="2022"
                    step="1"
                    value={year}
                    onChange={(e) => setYear(parseInt(e.target.value))}
                    className="w-full h-2 bg-blue-200 rounded-lg appearance-none cursor-pointer"
                />
            </div>
            <p className="text-xs text-blue-300/60 mt-2 text-center">
                Bubble size: Immigration Rate | Color: Crime Rate (White to Red)
            </p>
        </div>
    );
}
