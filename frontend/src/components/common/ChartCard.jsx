import { glassCardClasses } from '../../config/theme';

export default function ChartCard({ title, children, className = "" }) {
    return (
        <div className={`${glassCardClasses} ${className} p-6 flex flex-col min-h-100`}>
            {title && (
                <h3 className="text-2xl font-bold text-white mb-6 text-center tracking-wide">
                    {title}
                </h3>
            )}
            <div className="w-full flex-1 min-h-0">
                {children}
            </div>
        </div>
    );
}
