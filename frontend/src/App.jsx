import React from 'react';
import EuropeanStars from './EuropeanStars';
import Hero from './components/sections/Hero';
import StickySection from './components/sections/StickySection';
import InfoSection from './components/sections/InfoSection';
import LegacySection from './components/sections/LegacySection';

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
  return (
    <div className="min-h-screen bg-[#003399] text-white font-sans selection:bg-yellow-400 selection:text-blue-900 relative">
      <EuropeanStars />

      <Hero />

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

      <InfoSection />

      <LegacySection steps={originalSteps} />

      <footer className="bg-blue-950 text-blue-300 py-12 text-center relative z-10">
        <p>2025. Data Visualization by Filip, Ivan, Siro, Anastasija</p>
      </footer>

    </div>
  );
}