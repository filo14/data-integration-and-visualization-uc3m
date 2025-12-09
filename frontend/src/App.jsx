import React from 'react';
import EuropeanStars from './EuropeanStars';
import Hero from './components/sections/Hero';
import StickySection from './components/sections/StickySection';
import InfoSection from './components/sections/InfoSection';
import StorySection from './components/sections/StorySection';
import DataGraph from './components/DataGraph';
import DataMap from './components/DataMap';
import { useState, useEffect } from 'react';

// --- DATA CONFIGURATION ---

const originalSteps = [
  {
    id: 0,
    title: "The Initial Question",
    content: "Our initial question was whether there is a correlation between immigration flows and crime rates in the European Union. The first step was to look at the raw data, which at first glance suggested no correlation.",
    visualType: "question",
    visualColor: "bg-blue-800"
  },
  {
    id: 1,
    title: "Statistical Reality",
    content: "When we look at the raw data across 27 member states, the picture becomes complex. While some specific areas show fluctuations, the overall trend lines often diverge from the popular political rhetoric.",
    visualType: "chart",
    visualColor: "bg-indigo-800"
  },
  {
    id: 2,
    title: "Socio-Economic Factors",
    content: "Crime is rarely driven by nationality alone. Studies consistently show that poverty, lack of integration opportunities, and unemployment are far stronger predictors of criminal behavior than country of origin.",
    visualType: "people",
    visualColor: "bg-slate-800"
  },
];

export default function App() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [crimeVisuals, setCrimeVisuals] = useState([]);
  const [immigrationVisuals, setImmigrationVisuals] = useState([]);
  const [storySteps, setStorySteps] = useState(originalSteps);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('http://localhost:8000/api/full-data');
        const jsonData = await response.json();

        // 1. Capitalize names
        const capitalized = jsonData.map(d => ({
          ...d,
          country_name: d.country_name.split(' ')
            .map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
            .join(' ')
        }));

        // 2. Strict Filtering: Must have data for year 2018 with valid metrics
        const grouped = {};
        capitalized.forEach(d => {
          if (!grouped[d.country_iso3_id]) grouped[d.country_iso3_id] = [];
          grouped[d.country_iso3_id].push(d);
        });

        const filteredData = [];
        Object.values(grouped).forEach(countryRecords => {
          const data2018 = countryRecords.find(r => r.year === 2018);
          if (data2018 && data2018.convicts_per_100000 !== null && data2018.immigration_per_100000 !== null) {
            filteredData.push(...countryRecords);
          }
        });

        setData(filteredData);

        // --- Calculate Trends for Sticky Sections ---
        const years = [2018, 2019, 2020, 2021, 2022];

        // Helper to get annual average
        const getAnnualAvg = (dataSet, metric) => {
          return years.map(year => {
            const yearRecords = dataSet.filter(d => d.year === year);
            if (yearRecords.length === 0) return { year, value: 0 };
            const sum = yearRecords.reduce((acc, r) => acc + (r[metric] || 0), 0);
            return { year, value: Math.round(sum / yearRecords.length) };
          });
        };

        const crimeTrend = getAnnualAvg(filteredData, 'convicts_per_100000');
        const immTrend = getAnnualAvg(filteredData, 'immigration_per_100000');

        const maxCrime = 1000;
        const maxImm = 2000;

        // Generate Progressive Steps
        const cVisuals = years.map((year, i) => ({
          id: `crime-${year}`,
          type: 'trend',
          label: 'Average EU Crime Rate',
          data: crimeTrend.slice(0, i + 1),
          chartColor: '#ef4444', // Red
          domainMax: maxCrime
        }));

        const iVisuals = years.map((year, i) => ({
          id: `imm-${year}`,
          type: 'trend',
          label: 'Average EU Immigration Rate',
          data: immTrend.slice(0, i + 1),
          chartColor: '#10b981', // Emerald
          domainMax: maxImm
        }));

        setCrimeVisuals(cVisuals);
        setImmigrationVisuals(iVisuals);

        // --- Comparison Data for Story Section ---
        const comparisonData = years.map(year => {
          const crimeVal = crimeTrend.find(c => c.year === year)?.value || 0;
          const immVal = immTrend.find(i => i.year === year)?.value || 0;
          return { year, crime: crimeVal, immigration: immVal };
        });

        // Update steps with dynamic data
        const newSteps = [...originalSteps];
        newSteps[0] = {
          ...newSteps[0],
          visualType: 'comparison',
          title: 'Crime vs Immigration', // Override title if needed or keep "The Initial Question"
          data: comparisonData
        };

        setStorySteps(newSteps);
        setLoading(false);

      } catch (error) {
        console.error('Error fetching data:', error);
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  return (
    <div className="min-h-screen bg-[#003399] text-white font-sans selection:bg-yellow-400 selection:text-blue-900 relative">
      <div className="hidden md:block">
        <EuropeanStars />
      </div>

      <Hero />
      <StickySection
        title="Crime in the EU"
        subtitle="2018 to 2022"
        text="Despite popular beliefs, crime in the European Union has been decreasing steadily over the years. The average EU crime rate has dropped by about 40% since 2018."
        followText="Is less Immigration the reason for this positive development?"
        visuals={crimeVisuals.length > 0 ? crimeVisuals : [{ id: 'loading', type: 'default', label: 'Loading Data...' }]}
      />

      <StickySection
        title="Immigration in the EU"
        subtitle="2018 to 2022"
        text="The average EU immigration rate has neither increased nor decreased, while 2022 saw a strong increase that may or may not be representative of the long-term trend."
        followText="Does this mean Crime and Immigration are unrelated?"
        visuals={immigrationVisuals.length > 0 ? immigrationVisuals : [{ id: 'loading', type: 'default', label: 'Loading Data...' }]}
      />

      <StorySection steps={storySteps} />

      <div className="max-w-[1600px] mx-auto px-4 py-12 grid grid-cols-1 lg:grid-cols-2 gap-8">
        <div className="w-full">
          <DataMap rawData={data} loading={loading} />
        </div>
        <div className="w-full">
          <DataGraph rawData={data} loading={loading} />
        </div>
      </div>

      <footer className="bg-blue-950 text-blue-300 py-12 text-center relative z-10">
        <p>2025. Data Visualization by Filip, Ivan, Siro, Anastasija</p>
      </footer>

    </div>
  );
}