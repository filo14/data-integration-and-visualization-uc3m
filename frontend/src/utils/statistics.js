
import { THEME } from '../config/theme';

// Helper to format country names
export const formatCountryName = (name) => {
    if (!name) return name;
    return name.toLowerCase().split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
};

// Calculate trend data for a specific metric
export const calculateAnnualTrends = (data, metricKey) => {
    const years = [...new Set(data.map(d => d.year))].sort();
    return years.map(year => {
        const yearData = data.filter(d => d.year === year);
        const validValues = yearData
            .map(d => d[metricKey])
            .filter(v => v !== null && v !== undefined);

        const avg = validValues.length
            ? validValues.reduce((a, b) => a + b, 0) / validValues.length
            : 0;

        return { year, value: Math.round(avg * 100) / 100 };
    });
};

// Get average stats per country
export const getCountryAverages = (data) => {
    const countries = [...new Set(data.map(d => d.country_iso3_id))];

    return countries.map(iso3 => {
        const countryData = data.filter(d => d.country_iso3_id === iso3);
        const name = countryData[0]?.country_name || iso3;

        const avgCrime = countryData.reduce((acc, d) => acc + (d.convicts_per_100000 || 0), 0) / countryData.length;
        const avgImm = countryData.reduce((acc, d) => acc + (d.immigration_per_100000 || 0), 0) / countryData.length;

        return {
            name: name,
            country: name,
            iso3,
            crime: Math.round(avgCrime * 100) / 100,
            immigration: Math.round(avgImm * 100) / 100
        };
    });
};

// Calculate Pearson correlation coefficient
export const calculateCorrelation = (stats) => {
    const n = stats.length;
    if (n === 0) return 0;

    const sumX = stats.reduce((acc, s) => acc + s.immigration, 0);
    const sumY = stats.reduce((acc, s) => acc + s.crime, 0);
    const sumXY = stats.reduce((acc, s) => acc + s.immigration * s.crime, 0);
    const sumX2 = stats.reduce((acc, s) => acc + s.immigration * s.immigration, 0);
    const sumY2 = stats.reduce((acc, s) => acc + s.crime * s.crime, 0);

    const numerator = (n * sumXY) - (sumX * sumY);
    const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

    if (denominator === 0) return 0;
    return numerator / denominator;
};

// Transforms raw data for Recharts LineChart
export const transformDataForChart = (rawData, metric) => {
    if (!rawData.length) return [];

    const years = [...new Set(rawData.map(d => d.year))].sort();
    return years.map(year => {
        const entry = { year };
        rawData.filter(d => d.year === year).forEach(d => {
            entry[d.country_iso3_id] = d[metric];
        });
        return entry;
    });
};

// Identifies countries with valid data
export const getAvailableCountries = (rawData, metric, minYears = 5) => {
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
        if (years.size >= minYears) {
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
};

// Transforms raw data for Map Plot
export const transformDataForMap = (rawData, year) => {
    if (!rawData || rawData.length === 0) return null;

    const currentYearData = rawData.filter(d => d.year === year);
    if (!currentYearData.length) return null;

    const maxCrime = Math.max(...rawData.map(d => d.convicts_per_100000 || 0));

    return {
        locations: currentYearData.map(d => d.country_iso3_id),
        z: currentYearData.map(d => d.convicts_per_100000),
        text: currentYearData.map(d => `<b style="font-size: 14px; color: white">${d.country_name}</b><br><span style="color: ${THEME.colors.text}">Immigration:</span> <b style="color: white">${d.immigration_per_100000}</b> per 100k<br><span style="color: ${THEME.colors.text}">Crime:</span> <b style="color: white">${d.convicts_per_100000}</b> per 100k`),
        marker: {
            size: currentYearData.map(d => (d.immigration_per_100000 || 0) / 15),
            color: currentYearData.map(d => d.convicts_per_100000),
            colorscale: [[0, '#ffffffff'], [1, '#ff0000ff']],
            cmin: 0,
            cmax: maxCrime,
            opacity: 0.9,
            line: { color: 'rgba(255,255,255,0.8)', width: 1 },
            sizemode: 'area'
        }
    };
};
