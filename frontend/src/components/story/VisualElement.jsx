import TrendChart from '../charts/TrendChart';
import ComparisonChart from '../charts/ComparisonChart';
import TopCountriesChart from '../charts/TopCountriesChart';
import CorrelationChart from '../charts/CorrelationChart';
import ChartCard from '../common/ChartCard';

export default function VisualElement({ visual }) {
    const { type = 'default', label = 'Loading...', subLabel, data, chartColor, domainMax, correlation, metricName } = visual || {};

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
                                    <h4 className="text-xl text-blue-200 mb-1">Average Immigration Rate</h4>
                                    <p className="text-sm text-blue-300/60">Per 100k inhabitants</p>
                                </div>
                                <div className="text-right">
                                    <span className="text-5xl font-black text-immigration block">{data?.immigration}</span>
                                </div>
                            </div>
                            {/* Crime Stat */}
                            <div className="flex items-center justify-between pb-6">
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
                        <TrendChart data={data} color={chartColor} domainMax={domainMax} metricName={metricName} />
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
}
