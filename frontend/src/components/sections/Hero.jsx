import React from 'react';
import DataMap from '../DataMap';

export default function Hero({ rawData, loading }) {
    return (
        <section className="relative z-10 max-w-6xl mx-auto px-6 pt-12 pb-32">
            <div className="grid md:grid-cols-2 gap-6 items-center">
                <div className="space-y-8">
                    <h1 className="text-5xl md:text-7xl font-bold text-yellow-400 leading-tight">
                        Crime & Immigration in the EU <br />
                        <span className="text-white font-normal">is it a Problem?</span>
                    </h1>
                    <p className="text-lg md:text-xl text-blue-100 leading-relaxed max-w-xl">
                        Are crime and immigration related in the EU, or is it just a political ploy to gain voters?
                    </p>
                    <div className="animate-bounce m-20">
                        <div className="text-center text-xs font-mono uppercase tracking-widest text-blue-300">
                            Scroll to Begin
                        </div>
                        <div className="w-[1px] h-16 bg-yellow-400 mx-auto mt-4" />
                    </div>
                </div>
                <div className="flex justify-center items-center mt-8 md:mt-0">
                    <DataMap rawData={rawData} loading={loading} />
                </div>
            </div>
        </section>
    );
}
