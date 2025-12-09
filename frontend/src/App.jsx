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

const section1Visuals = [
  { id: 101, type: 'flag', color: 'bg-blue-800', label: 'Demographic A' },
  { id: 102, type: 'chart', color: 'bg-indigo-800', label: 'Demographic B' },
  { id: 103, type: 'people', color: 'bg-slate-800', label: 'Demographic C' },
  { id: 104, type: 'alert', color: 'bg-red-900', label: 'Demographic D' },
  { id: 105, type: 'globe', color: 'bg-teal-800', label: 'Demographic E' },
];

const section2Visuals = [
  { id: 201, type: 'chart', color: 'bg-emerald-800', label: 'Trend 2015' },
  { id: 202, type: 'scale', color: 'bg-cyan-800', label: 'Trend 2017' },
  { id: 203, type: 'people', color: 'bg-sky-800', label: 'Trend 2019' },
  { id: 204, type: 'activity', color: 'bg-violet-800', label: 'Trend 2021' },
  { id: 205, type: 'flag', color: 'bg-fuchsia-800', label: 'Trend 2023' },
];

const originalSteps = [
  {
    id: 0,
    title: "The Initial Question",
    content: "Is there a direct correlation between immigration flows and crime rates in the European Union? Or is this narrative merely a political tool used to polarize voters?",
    visualType: "flag",
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
        // If 2018 exists and is valid, we keep the country (and all its available years)
        const grouped = {};

        capitalized.forEach(d => {
          if (!grouped[d.country_iso3_id]) grouped[d.country_iso3_id] = [];
          grouped[d.country_iso3_id].push(d);
        });

        const filteredData = [];
        Object.values(grouped).forEach(countryRecords => {
          const data2018 = countryRecords.find(r => r.year === 2018);

          // Check if 2018 exists and has valid values
          const hasValid2018 = data2018 &&
            data2018.convicts_per_100000 !== null &&
            data2018.immigration_per_100000 !== null;

          if (hasValid2018) {
            filteredData.push(...countryRecords);
          }
        });

        setData(filteredData);
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

      <Hero rawData={data} loading={loading} />

      <StickySection
        title="Crime in the EU - 2018 to 2022"
        text="As we delve into the data, we must first understand the landscape. This section illustrates five key demographics across the EU. Keep scrolling to see how the visual representation evolves while this text remains your constant guide through the initial statistics."
        visuals={section1Visuals}
      />

      <StickySection
        title="Immigration in the EU - 2018 to 2022"
        text="Moving deeper into the analysis, we observe specific trends over the last decade. Notice how the visuals shift to represent different time periods and intensity of migration flows, while we maintain our focus on the overarching narrative of stability versus volatility."
        visuals={section2Visuals}
      />

      <StorySection steps={originalSteps} />

      <div className="max-w-7xl mx-auto px-4 py-12">
        <DataGraph rawData={data} loading={loading} />
      </div>

      <footer className="bg-blue-950 text-blue-300 py-12 text-center relative z-10">
        <p>2025. Data Visualization by Filip, Ivan, Siro, Anastasija</p>
      </footer>

    </div>
  );
}