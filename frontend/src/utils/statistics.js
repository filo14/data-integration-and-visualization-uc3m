/**
 * Formats a country name to Title Case (e.g., "UNITED KINGDOM" -> "United Kingdom").
 */
export const formatCountryName = (name) => {
    return name.split(' ')
        .map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
        .join(' ');
};

/**
 * Calculates annual averages for a given metric across the dataset.
 */
export const calculateAnnualTrends = (data, metric, years = [2018, 2019, 2020, 2021, 2022]) => {
    return years.map(year => {
        const yearRecords = data.filter(d => d.year === year);
        if (!yearRecords.length) return { year, value: 0 };

        const sum = yearRecords.reduce((acc, r) => acc + (r[metric] || 0), 0);
        return {
            year,
            value: Math.round(sum / yearRecords.length)
        };
    });
};

/**
 * Aggregates data by country to find average Crime and Immigration rates over the full period.
 */
export const getCountryAverages = (data) => {
    const countryStats = {};

    data.forEach(d => {
        if (!countryStats[d.country_name]) {
            countryStats[d.country_name] = {
                country: d.country_name,
                totalCrime: 0,
                totalImm: 0,
                count: 0
            };
        }
        countryStats[d.country_name].totalCrime += (d.convicts_per_100000 || 0);
        countryStats[d.country_name].totalImm += (d.immigration_per_100000 || 0);
        countryStats[d.country_name].count += 1;
    });

    return Object.values(countryStats).map(c => ({
        country: c.country,
        crime: Math.round(c.totalCrime / c.count),
        immigration: Math.round(c.totalImm / c.count)
    }));
};

/**
 * Calculates the Pearson Correlation Coefficient (r) between Crime and Immigration.
 */
export const calculateCorrelation = (data) => {
    const validData = data.filter(c => c.crime > 0 && c.immigration > 0);
    const n = validData.length;

    if (n === 0) return 0;

    const sumX = validData.reduce((acc, c) => acc + c.immigration, 0);
    const sumY = validData.reduce((acc, c) => acc + c.crime, 0);
    const sumXY = validData.reduce((acc, c) => acc + (c.immigration * c.crime), 0);
    const sumX2 = validData.reduce((acc, c) => acc + (c.immigration * c.immigration), 0);
    const sumY2 = validData.reduce((acc, c) => acc + (c.crime * c.crime), 0);

    const numerator = (n * sumXY) - (sumX * sumY);
    const denominator = Math.sqrt(((n * sumX2) - (sumX * sumX)) * ((n * sumY2) - (sumY * sumY)));

    return denominator === 0 ? 0 : numerator / denominator;
};
