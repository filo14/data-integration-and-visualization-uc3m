import React, { useState, useEffect, useRef } from 'react';
import { Star, BarChart3, Image, Users, ShieldAlert, Activity, Globe, Scale } from 'lucide-react';
import EuropeanStars from './EuropeanStars';

export default function App() {
  return (
    <div className="min-h-screen bg-[#003399] text-white font-sans selection:bg-yellow-400 selection:text-blue-900 relative">
      <EuropeanStars />

      {/* --- HERO SECTION --- */}
      <section className="relative z-10 max-w-6xl mx-auto px-6 pt-24 pb-32">
        <div className="grid md:grid-cols-2 gap-12 items-center">
          <div className="space-y-8">
            <h1 className="text-5xl md:text-7xl font-bold text-yellow-400 leading-tight">
              Crime & Immigration in the EU <br /> <span className="text-white font-normal">is it a Problem?</span>
            </h1>
            <p className="text-lg md:text-xl text-blue-100 leading-relaxed max-w-xl">
              Are crime and immigration related in the EU, or is it just a political ploy to gain voters? We analyze the data behind the catchphrases.
            </p>
            <div className="animate-bounce m-20">
              <div className="text-center text-xs font-mono uppercase tracking-widest text-blue-300">Scroll to Begin</div>
              <div className="w-[1px] h-16 bg-yellow-400 mx-auto mt-4"></div>
            </div>
          </div>
          <div className="hidden md:flex justify-center items-center">
            {/* Abstract Map Placeholder */}
            <div className="relative w-full max-w-md aspect-square bg-blue-800/50 rounded-2xl border-2 border-blue-400/30 p-8 flex items-center justify-center backdrop-blur-sm">
              <span className="text-blue-300 font-mono text-sm tracking-widest text-center">
                [ PLACEHOLDER: EU MAP VISUALIZATION ]
                <br />
                Interactive elements disabled
              </span>
              <div className="absolute top-1/4 left-1/4 w-3 h-3 bg-yellow-400 rounded-full animate-pulse"></div>
              <div className="absolute bottom-1/3 right-1/4 w-3 h-3 bg-yellow-400 rounded-full animate-pulse delay-75"></div>
              <div className="absolute top-1/2 left-1/2 w-3 h-3 bg-yellow-400 rounded-full animate-pulse delay-150"></div>
            </div>
          </div>
        </div>
      </section>

      {/* --- SECTION 1: STICKY TEXT (RIGHT), CHANGING IMAGES (LEFT) --- */}
      {/* 5 pictures that change when scrolling, text stays the same */}
      <StickyTextChangingImageSection
        title="Crime in the EU - 2018 to 2022"
        text="As we delve into the data, we must first understand the landscape. This section illustrates five key demographics across the EU. Keep scrolling to see how the visual representation evolves while this text remains your constant guide through the initial statistics."
        visuals={section1Visuals}
      />

      {/* --- SECTION 2: STICKY TEXT (RIGHT), CHANGING IMAGES (LEFT) --- */}
      {/* One more with pictures with text like this */}
      <StickyTextChangingImageSection
        title="Immigration in the EU - 2018 to 2022"
        text="Moving deeper into the analysis, we observe specific trends over the last decade. Notice how the visuals shift to represent different time periods and intensity of migration flows, while we maintain our focus on the overarching narrative of stability versus volatility."
        visuals={section2Visuals}
        theme="darker" // Optional prop to vary styling slightly if needed
      />

      {/* --- SECTION 3: NORMAL SCROLL --- */}
      {/* Normal text and normal picture that scroll normally */}
      <NormalScrollSection />

      {/* --- SECTION 4: ORIGINAL IMPLEMENTATION --- */}
      {/* Current scrolling implementation for 3 pictures */}
      <OriginalScrollSection steps={originalSteps} />

      {/* Footer */}
      <footer className="bg-blue-950 text-blue-300 py-12 text-center relative z-10">
        <p>2025. Data Visualization by Filip, Ivan, Siro, Anastasija</p>
      </footer>

    </div>
  );
}

// --- DATA ---

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

// --- COMPONENTS ---

// 1. STICKY TEXT (RIGHT) + CHANGING IMAGES (LEFT)
// Logic: The container is tall (e.g. 500vh for 5 images). 
// The image container is sticky top-0. The text container is sticky top-0.
// We trigger image changes based on scroll progress within the parent container.
function StickyTextChangingImageSection({ title, text, visuals }) {
  const containerRef = useRef(null);
  const [activeVisualIndex, setActiveVisualIndex] = useState(0);

  useEffect(() => {
    const handleScroll = () => {
      if (!containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      const sectionHeight = rect.height;
      const viewportHeight = window.innerHeight;

      // Calculate how far we've scrolled into this section
      // Start counting when top of section hits top of viewport (or close to it)
      // negative top means we are scrolling down
      const scrolled = -rect.top;

      if (scrolled < 0) {
        setActiveVisualIndex(0);
        return;
      }

      // Total track length roughly
      const scrollableDistance = sectionHeight - viewportHeight;
      if (scrollableDistance <= 0) return;

      const progress = scrolled / scrollableDistance;
      const index = Math.min(
        Math.max(Math.floor(progress * visuals.length), 0),
        visuals.length - 1
      );

      setActiveVisualIndex(index);
    };

    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, [visuals.length]);

  return (
    <div ref={containerRef} className="relative" style={{ height: `${visuals.length * 100}vh` }}>
      <div className="sticky top-0 h-screen w-full flex flex-col md:flex-row overflow-hidden bg-blue-950">

        {/* LEFT: VISUALS (Changing) */}
        <div className="md:w-1/2 h-full flex items-center justify-center p-8 bg-black/20">
          <VisualDisplaySimple visual={visuals[activeVisualIndex]} />
        </div>

        {/* RIGHT: TEXT (Static) */}
        <div className="md:w-1/2 h-full flex items-center justify-center p-8 md:p-16">
          <div className="max-w-xl">
            <h2 className="text-4xl md:text-5xl font-bold mb-8">{title}</h2>
            <p className="text-xl text-blue-100 leading-relaxed mb-8">
              {text}
            </p>
            <div className="text-sm font-mono text-blue-400 border border-blue-400/30 inline-block px-3 py-1 rounded-full">
              {activeVisualIndex + 1}/{visuals.length}
            </div>
          </div>
        </div>

      </div>
    </div>
  );
}

// 2. NORMAL SCROLL SECTION
function NormalScrollSection() {
  return (
    <div className="bg-blue-900 py-32 px-6">
      <div className="max-w-4xl mx-auto space-y-24">

        <div className="flex flex-col md:flex-row gap-12 items-center">
          <div className="md:w-1/2">
            <div className="aspect-video bg-blue-800 rounded-xl flex items-center justify-center border border-blue-400/30">
              <Globe className="w-16 h-16 text-blue-300" />
            </div>
          </div>
          <div className="md:w-1/2">
            <h3 className="text-3xl font-bold text-white mb-4">A Global Perspective</h3>
            <p className="text-blue-100 text-lg leading-relaxed">
              This section behaves normally. As you scroll, the content moves up. No sticky tricks here.
              We step back to look at the broader European context, examining how cross-border cooperation
              has effectively managed data sharing despite political rhetoric.
            </p>
          </div>
        </div>

        <div className="flex flex-col md:flex-row-reverse gap-12 items-center">
          <div className="md:w-1/2">
            <div className="aspect-video bg-indigo-900 rounded-xl flex items-center justify-center border border-indigo-400/30">
              <Activity className="w-16 h-16 text-indigo-300" />
            </div>
          </div>
          <div className="md:w-1/2">
            <h3 className="text-3xl font-bold text-white mb-4">Methodology</h3>
            <p className="text-blue-100 text-lg leading-relaxed">
              Our analysis relies on Eurostat figures combined with local police reports.
              By normalizing for population density and economic factors, a clearer pattern emerges—one
              that defies simple causality.
            </p>
          </div>
        </div>

      </div>
    </div>
  );
}

// 3. ORIGINAL SCROLL SECTION (Sticky Image Left, Scrolling Text Right)
function OriginalScrollSection({ steps }) {
  const [activeStep, setActiveStep] = useState(0);

  return (
    <div className="relative max-w-7xl mx-auto px-4 py-32">
      <div className="flex flex-col md:flex-row">

        {/* LEFT COLUMN: Sticky Visuals */}
        <div className="md:w-1/2 sticky top-0 h-screen flex items-center justify-center p-8">
          {/* Reusing existing VisualDisplay logic essentially */}
          <VisualDisplaySimple visual={steps[activeStep] ? {
            type: steps[activeStep].visualType,
            color: steps[activeStep].visualColor,
            label: steps[activeStep].title + " Data",
            subLabel: `FIG ${steps[activeStep].id + 1}.0`
          } : { type: 'flag', color: 'bg-blue-800' }} />
        </div>

        {/* RIGHT COLUMN: Scrolling Text */}
        <div className="md:w-1/2">
          {steps.map((step, index) => (
            <TextBlock
              key={step.id}
              step={step}
              index={index}
              setActiveStep={setActiveStep}
            />
          ))}
          <div className="h-[50vh]"></div>
        </div>

      </div>
    </div>
  );
}

// --- SHARED SUB-COMPONENTS ---

function VisualDisplaySimple({ visual }) {
  // Safe default
  const v = visual || { type: 'flag', color: 'bg-blue-800', label: 'Loading...' };

  return (
    <div className="w-full max-w-lg aspect-[4/3] relative rounded-xl overflow-hidden shadow-2xl transition-all duration-500 ease-in-out border-4 border-blue-400/20 bg-blue-900">
      <div className={`absolute inset-0 transition-colors duration-700 ${v.color} opacity-50`}></div>
      <div className="absolute inset-0 flex flex-col items-center justify-center p-8 text-center">
        <div className="mb-6 transform transition-all duration-500 scale-100 p-6 bg-white/10 rounded-full backdrop-blur-md">
          {/* Rendering Icon based on type string */}
          <IconRenderer type={v.type} />
        </div>
        <h3 className="text-2xl font-bold text-white mb-2 tracking-wide">
          {v.label}
        </h3>
        {v.subLabel && (
          <p className="text-blue-200 text-sm font-mono border border-blue-400/30 px-3 py-1 rounded-full">{v.subLabel}</p>
        )}
      </div>
    </div>
  );
}

function IconRenderer({ type }) {
  switch (type) {
    case 'flag': return <FlagIcon />;
    case 'chart': return <BarChart3 className="w-20 h-20 text-yellow-400" />;
    case 'people': return <Users className="w-20 h-20 text-yellow-400" />;
    case 'alert': return <ShieldAlert className="w-20 h-20 text-red-400" />;
    case 'activity': return <Activity className="w-20 h-20 text-green-400" />;
    case 'globe': return <Globe className="w-20 h-20 text-cyan-400" />;
    case 'scale': return <Scale className="w-20 h-20 text-orange-400" />;
    default: return <Star className="w-20 h-20 text-yellow-400" />;
  }
}

function TextBlock({ step, index, setActiveStep }) {
  const ref = useRef(null);
  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) setActiveStep(index);
      },
      { root: null, rootMargin: "-40% 0px -40% 0px", threshold: 0.2 }
    );
    if (ref.current) observer.observe(ref.current);
    return () => ref.current && observer.unobserve(ref.current);
  }, [index, setActiveStep]);

  return (
    <div ref={ref} className="min-h-screen flex items-center justify-center p-8 md:p-16 border-l border-blue-800/50">
      <div className="transition-opacity duration-500">
        <span className="font-bold text-6xl opacity-40 block mb-4">0{index + 1}</span>
        <h2 className="text-3xl md:text-4xl font-bold text-white mb-6">{step.title}</h2>
        <p className="text-lg md:text-xl text-blue-100 leading-relaxed">{step.content}</p>
      </div>
    </div>
  );
}

function FlagIcon() {
  return (
    <div className="w-24 h-16 bg-blue-700 relative flex items-center justify-center overflow-hidden border border-yellow-400/30 shadow-inner">
      <div className="absolute inset-0 flex items-center justify-center">
        <div className="w-12 h-12 border-2 border-dashed border-yellow-400 rounded-full animate-spin-slow opacity-80"></div>
      </div>
      <Star className="w-6 h-6 text-yellow-400 fill-yellow-400 relative z-10" />
    </div>
  );
}