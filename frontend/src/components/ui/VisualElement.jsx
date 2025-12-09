import React from 'react';
import { Star, BarChart3, Users, ShieldAlert, Activity, Globe, Scale, FileQuestion } from 'lucide-react';

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

import TrendChart from './TrendChart';
import ComparisonChart from './ComparisonChart';
import TopCountriesChart from './TopCountriesChart';
import CorrelationChart from './CorrelationChart';

export default function VisualElement({ visual }) {
    const { type = 'default', color = 'bg-white/5', label = 'Loading...', subLabel, data, chartColor, domainMax, correlation } = visual || {};
    const IconComponent = icons[type] || icons.default;

    if (type === 'comparison') {
        return (
            <div className="w-full bg-white/5 p-6 rounded-xl backdrop-blur-sm border border-white/10 shadow-2xl">
                <h3 className="text-2xl font-bold text-white mb-6 text-center tracking-wide">{label}</h3>
                <div className="w-full h-[400px]">
                    <ComparisonChart data={data} />
                </div>
            </div>
        );
    }

    if (type === 'top-countries') {
        return (
            <div className="w-full bg-white/5 p-6 rounded-xl backdrop-blur-sm border border-white/10 shadow-2xl">
                <h3 className="text-2xl font-bold text-white mb-6 text-center tracking-wide">{label}</h3>
                <div className="w-full h-[400px]">
                    <TopCountriesChart data={data} />
                </div>
            </div>
        );
    }

    if (type === 'correlation') {
        return (
            <div className="w-full bg-white/5 p-6 rounded-xl backdrop-blur-sm border border-white/10 shadow-2xl">
                <h3 className="text-2xl font-bold text-white mb-6 text-center tracking-wide">{label}</h3>
                <div className="w-full h-[400px]">
                    <CorrelationChart data={data} correlation={correlation} />
                </div>
            </div>
        );
    }

    if (type === 'trend') {
        return (
            <div className="w-full max-w-lg aspect-[4/3] relative rounded-xl overflow-hidden shadow-2xl transition-all duration-500 ease-in-out border-4 border-blue-400/20 bg-blue-900">
                <div className="absolute inset-0 p-6 flex flex-col bg-slate-900/80 backdrop-blur-sm">
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

    return (
        <div className="w-full max-w-lg aspect-[4/3] relative rounded-xl overflow-hidden shadow-2xl transition-all duration-500 ease-in-out border-4 border-blue-400/20 bg-blue-900">
            <div className={`absolute inset-0 transition-colors duration-700 ${color} opacity-50`} />
            <div className="absolute inset-0 flex flex-col items-center justify-center p-8 text-center">
                <div className="mb-6 transform transition-all duration-500 scale-100 p-6 bg-white/10 rounded-full backdrop-blur-md">
                    <IconComponent />
                </div>
                <h3 className="text-2xl font-bold text-white mb-2 tracking-wide">
                    {label}
                </h3>
                {subLabel && (
                    <p className="text-blue-200 text-sm font-mono border border-blue-400/30 px-3 py-1 rounded-full">
                        {subLabel}
                    </p>
                )}
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
