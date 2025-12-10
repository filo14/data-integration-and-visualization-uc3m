import { useState, useEffect } from 'react';
import { formatCountryName, calculateAnnualTrends, getCountryAverages, calculateCorrelation } from '../utils/statistics';
import { THEME } from '../config/theme';

export default function useEuData() {
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [crimeVisuals, setCrimeVisuals] = useState([]);
    const [immigrationVisuals, setImmigrationVisuals] = useState([]);
    const [storyData, setStoryData] = useState({
        comparison: [],
        topCrime: [],
        topImmigration: [],
        correlationData: [],
        correlation: 0,
        globalStats: null
    });

    useEffect(() => {
        const loadData = async () => {
            try {
                const response = await fetch('http://localhost:8000/api/eu-data');
                const rawData = await response.json();

                const processed = rawData
                    .map(d => ({ ...d, country_name: formatCountryName(d.country_name) }))
                    .filter(d =>
                        d.convicts_per_100000 !== null &&
                        d.immigration_per_100000 !== null &&
                        d.year >= 2018
                    );

                setData(processed);


                const crimeTrend = calculateAnnualTrends(processed, 'convicts_per_100000');
                const immTrend = calculateAnnualTrends(processed, 'immigration_per_100000');

                setCrimeVisuals(createTrendVisuals(crimeTrend, 'Average EU Crime Rate per 100,000', THEME.colors.crime, 1000, 'Crime Rate'));
                setImmigrationVisuals(createTrendVisuals(immTrend, 'Average EU Immigration Rate per 100,000', THEME.colors.immigration, 2000, 'Immigration Rate'));


                const comparison = crimeTrend.map(c => ({
                    year: c.year,
                    crime: c.value,
                    immigration: immTrend.find(i => i.year === c.year)?.value || 0
                }));

                const countryStats = getCountryAverages(processed);
                const topCrime = [...countryStats].sort((a, b) => b.crime - a.crime).slice(0, 5);
                const topImmigration = [...countryStats].sort((a, b) => b.immigration - a.immigration).slice(0, 5);

                const r = calculateCorrelation(countryStats);

                const globalStats = {
                    crime: Math.round(countryStats.reduce((acc, c) => acc + c.crime, 0) / countryStats.length),
                    immigration: Math.round(countryStats.reduce((acc, c) => acc + c.immigration, 0) / countryStats.length)
                };

                setStoryData({
                    comparison,
                    topCrime,
                    topImmigration,
                    correlationData: countryStats,
                    correlation: r,
                    globalStats
                });

                setLoading(false);
            } catch (err) {
                console.error("Failed to load crime data:", err);
                setLoading(false);
            }
        };

        loadData();
    }, []);

    return { data, loading, crimeVisuals, immigrationVisuals, storyData };
}

const createTrendVisuals = (trendData, label, color, maxDomain, metricName) => {
    return trendData.map((_, i) => ({
        id: `trend-${trendData[i].year}`,
        type: 'trend',
        label,
        metricName,
        data: trendData.slice(0, i + 1),
        chartColor: color,
        domainMax: maxDomain
    }));
};
