import React from 'react';
import { Activity, Globe } from 'lucide-react';

export default function InfoSection() {
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
