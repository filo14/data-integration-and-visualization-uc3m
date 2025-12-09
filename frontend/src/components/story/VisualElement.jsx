import React from 'react';
import { Star, BarChart3, Users, ShieldAlert, Activity, Globe, Scale, FileQuestion } from 'lucide-react';
import TrendChart from '../charts/TrendChart';
import ComparisonChart from '../charts/ComparisonChart';
import TopCountriesChart from '../charts/TopCountriesChart';
import CorrelationChart from '../charts/CorrelationChart';
import ChartCard from '../common/ChartCard';

const icons = {
    flag: FlagIcon,
    chart: (props) => <BarChart3 {...props} className="w-20 h-20 text-yellow-400" />,
    people: (props) => <Users {...props} className="w-20 h-20 text-yellow-400" />,
    alert: (props) => <ShieldAlert {...props} className="w-20 h-20 text-red-400" />,
    activity: (props) => <Activity {...props} className="w-20 h-20 text-green-400" />,
    globe: (props) => <Globe {...props} className="w-20 h-20 text-cyan-400" />,
    scale: (props) => <Scale {...props} className="w-20 h-20 text-orange-400" />,
    default: (props) => <Star {...props} className="w-20 h-20 text-yellow-400" />,
    question: (props) => <FileQuestion {...props} className="w-20 h-20 text-yellow-400" />,
};

export default function VisualElement({ visual }) {
    const { type = 'default', color = 'bg-white/5', label = 'Loading...', subLabel, data, chartColor, domainMax, correlation } = visual || {};
    const IconComponent = icons[type] || icons.default;

    // Charts that share the same glassmorphism card style
    if (['comparison', 'top-countries', 'correlation', 'global-stats'].includes(type)) {
        return (
            <div className="w-full aspect-video">
                <ChartCard title={label} className="h-full">
                    {type === 'comparison' && <ComparisonChart data={data} />}
                    {type === 'top-countries' && <TopCountriesChart data={data} referenceLine={visual.referenceLine} />}
                    {type === 'correlation' && <CorrelationChart data={data} correlation={correlation} />}
                    {type === 'global-stats' && (
                        <div className="h-full flex flex-col justify-center gap-8 px-8">
                            {/* Immigration Stat */}
                            <div className="flex items-center justify-between border-b border-white/10 pb-6">
                                <div>
                                    <h4 className="text-xl text-blue-200 mb-1">Average Immigration</h4>
                                    <p className="text-sm text-blue-300/60">Per 100k inhabitants</p>
                                </div>
                                <div className="text-right">
                                    <span className="text-5xl font-black text-immigration block">{data?.immigration}</span>
                                </div>
                            </div>
                            {/* Crime Stat */}
                            <div className="flex items-center justify-between border-b border-white/10 pb-6">
                                <div>
                                    <h4 className="text-xl text-blue-200 mb-1">Average Crime Rate</h4>
                                    <p className="text-sm text-blue-300/60">Per 100k inhabitants</p>
                                </div>
                                <div className="text-right">
                                    <span className="text-5xl font-black text-crime block">{data?.crime}</span>
                                </div>
                            </div>
                        </div>
                    )}
                </ChartCard>
            </div>
        );

    }

    // Special case for the "Trend" sticky card which has complex internal layout
    if (type === 'trend') {
        return (
            <div className="w-full aspect-[4/3] relative rounded-xl overflow-hidden shadow-2xl transition-all duration-500 ease-in-out border-4 border-blue-400/20 bg-primary-dark/50">
                <div className="absolute inset-0 p-6 flex flex-col bg-blue-1000/80 backdrop-blur-sm">
                    <h3 className="text-2xl font-bold text-white mb-2 text-center tracking-wide">{label}</h3>
                    <div className="flex-1 min-h-0 w-full">
                        <TrendChart data={data} color={chartColor} domainMax={domainMax} />
                    </div>
                    {subLabel && (
                        <p className="mt-2 text-center text-blue-200 text-sm font-mono border border-blue-400/30 px-3 py-1 rounded-full self-center">
                            {subLabel}
                        </p>
                    )}
                </div>
            </div>
        );
    }

    // Default "Card" view for questions/icons
    return (
        <div className="w-full aspect-[4/3] relative rounded-xl overflow-hidden shadow-2xl transition-all duration-500 ease-in-out border-4 border-blue-400/20 bg-primary-dark/50">
            <div className={`absolute inset-0 transition-colors duration-700 ${color} opacity-50`} />
            <div className="absolute inset-0 flex flex-col items-center justify-center p-8 text-center">
                <div className="mb-6 transform transition-all duration-500 scale-100 p-6 bg-white/10 rounded-full backdrop-blur-md">
                    <IconComponent />
                </div>
                <h3 className="text-2xl font-bold text-white mb-2 tracking-wide">
                    {label}
                </h3>
            </div>
        </div>
    );
}

function FlagIcon() {
    return (
        <div className="w-24 h-16 bg-blue-700 relative flex items-center justify-center overflow-hidden border border-yellow-400/30 shadow-inner">
            <div className="absolute inset-0 flex items-center justify-center">
                <div className="w-12 h-12 border-2 border-dashed border-yellow-400 rounded-full animate-spin-slow opacity-80" />
            </div>
            <Star className="w-6 h-6 text-yellow-400 fill-yellow-400 relative z-10" />
        </div>
    );
}
