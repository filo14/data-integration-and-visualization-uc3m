import { useMemo } from 'react';
import Plot from 'react-plotly.js';
import { THEME } from '../../config/theme';

export default function DataMap({ mapData, year, setYear, loading }) {

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
                            landcolor: THEME.colors.mapLand,
                            countrycolor: THEME.colors.mapCountry,
                            showlakes: false,
                            fitbounds: "locations", // Auto-zoom to data
                            projection: { type: 'mercator' }
                        },
                        paper_bgcolor: 'rgba(0,0,0,0)',
                        plot_bgcolor: 'rgba(0,0,0,0)',
                        margin: { l: 0, r: 0, t: 0, b: 0 },
                        showlegend: false,
                        hoverlabel: {
                            bgcolor: THEME.colors.mapHover,
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
            <p className="text-m mt-2 text-center">
                Bubble size: Immigration Rate | Color: Crime Rate (White to Red)
            </p>
        </div>
    );
}
