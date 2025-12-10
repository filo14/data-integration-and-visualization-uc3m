import React, { useState, useEffect, useMemo } from 'react';
import Hero from './components/sections/Hero';
import StorySection from './components/sections/StorySection';
import StickySection from './components/sections/StickySection';
import DataMap from './components/charts/DataMap';
import DataGraph from './components/charts/DataGraph';
import ChartCard from './components/common/ChartCard';
import Conclusion from './components/sections/Conclusion';
import EuropeanStars from './EuropeanStars';
import useEuData from './hooks/useEuData';
import { THEME } from './config/theme';


export default function App() {
  const {
    data,
    loading,
    crimeVisuals,
    immigrationVisuals,
    storyData,
    filterMapData,
    filterGraphData,
    filterAvailableCountries
  } = useEuData();
  const [storySteps, setStorySteps] = useState([]);


  const [mapYear, setMapYear] = useState(2018);
  const [graphMetric, setGraphMetric] = useState('convicts_per_100000');


  const mapDisplayData = useMemo(() => {
    return filterMapData(mapYear);
  }, [filterMapData, mapYear]);

  const graphData = useMemo(() => {
    return filterGraphData(graphMetric);
  }, [filterGraphData, graphMetric]);

  const graphAvailableCountries = useMemo(() => {
    return filterAvailableCountries(graphMetric);
  }, [filterAvailableCountries, graphMetric]);


  useEffect(() => {
    if (loading) return;


    const newSteps = [
      {
        id: 0,
        visualType: 'comparison',
        title: 'The initial question',
        content: 'Our initial question was whether there is a correlation between immigration flows and crime rates in the European Union. The first step was to look at the raw data, which at first glance suggested no correlation.',
        label: 'Immigration vs. Crime',
        data: storyData.comparison
      },
      {
        id: 1,
        visualType: 'global-stats',
        title: 'Global Averages',
        content: 'We can establish a baseline for the entire European Union. These numbers represent the average annual rates per 100,000 people across all 27 member states from 2018 to 2022.',
        label: 'EU Average Rates',
        data: storyData.globalStats,
      },
      {
        id: 2,
        visualType: 'top-countries',
        title: 'Top 5 Countries by Crime',
        content: 'Taking a look at the countries with the highest crime rates, we can see that immigration rates are not necessarily high in these countries compared to the EU average. But what about the countries with the highest immigration rates?',
        label: 'Top 5 by Crime Rate',
        data: storyData.topCrime,
        referenceLine: {
          value: storyData.globalStats?.immigration,
          label: 'Average EU Immigration Rate',
          color: THEME.colors.textStrong
        }
      },
      {
        id: 3,
        visualType: 'top-countries',
        title: 'Top 5 Countries by Immigration',
        content: 'The countries with the highest immigration rates do not consistently feature the highest crime rates, further challenging the direct correlation hypothesis. The small country Luxembourg is the only outlier. Do the maths confirm or reject the direct correlation hypothesis?',
        label: 'Top 5 by Immigration Rate',
        data: storyData.topImmigration,
        referenceLine: {
          value: storyData.globalStats?.crime,
          label: 'Average EU Crime Rate',
          color: THEME.colors.textStrong
        }
      },
      {
        id: 4,
        title: "The Correlation Verdict",
        content: `Statistical analysis reveals a Pearson correlation coefficient (r) of ${(storyData.correlation || 0).toFixed(3)}. The positive value indicates a slight alignment: generally, higher immigration is faintly linked to higher crime. However, a value of ${(storyData.correlation || 0).toFixed(3)} is statistically weak. It implies that while there is a slight overlap, immigration is not a strong or reliable predictor of crime rates across the EU, suggesting other factors are more significant.`,
        visualType: 'correlation',
        data: storyData.correlationData,
        label: 'Immigration vs Crime Correlation',
        correlation: storyData.correlation
      }
    ];

    setStorySteps(newSteps);
  }, [loading, storyData]);

  return (
    <div className="min-h-screen bg-primary text-white font-sans selection:bg-accent selection:text-primary-dark relative">
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

      <Conclusion />


      <div className="max-w-7xl mx-auto px-4 pt-20 pb-8 text-center mt-50">
        <h2 className="text-4xl font-bold text-white mb-4">Explore the Full Dataset</h2>
        <p className="text-blue-200 max-w-2xl mx-auto">
          Dive into the interactive data tools below to analyze trends, compare countries, and draw your own conclusions based on the official statistics.
        </p>
      </div>

      <div className="max-w-[1600px] mx-auto px-4 pb-20 grid grid-cols-1 lg:grid-cols-2 gap-8">
        <div className="w-full">
          <DataMap
            mapData={mapDisplayData}
            year={mapYear}
            setYear={setMapYear}
            loading={loading}
          />
        </div>
        <div className="w-full h-[600px]">
          <ChartCard title="Crime & Immigration per Country" className="h-full">
            <DataGraph
              data={graphData}
              availableCountries={graphAvailableCountries}
              metric={graphMetric}
              setMetric={setGraphMetric}
              loading={loading}
            />
          </ChartCard>
        </div>
      </div>

      <footer className="bg-primary-dark text-blue-300 py-12 text-center relative z-10">
        <p>2025. Data Visualization by Filip, Ivan, Siro, Anastasija</p>
        <p className="text-xs text-blue-300/60 mt-4 text-center">
          Data sources: World Bank, EU & UN (2018-2022)
        </p>
      </footer>

    </div>
  );
}