import React from 'react';

export default function Hero() {
    return (
        <section className="relative z-10 max-w-6xl mx-auto px-6 pt-24 pb-32">
            <div className="grid md:grid-cols-2 gap-12 items-center">
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
                <div className="hidden md:flex justify-center items-center">
                    <div className="relative w-full max-w-md aspect-square bg-blue-800/50 rounded-2xl border-2 border-blue-400/30 p-8 flex items-center justify-center backdrop-blur-sm">
                        <span className="text-blue-300 font-mono text-sm tracking-widest text-center">
                            EU MAP VISUALIZATION
                            <br />
                            Interactive elements disabled
                        </span>
                        <div className="absolute top-1/4 left-1/4 w-3 h-3 bg-yellow-400 rounded-full animate-pulse" />
                        <div className="absolute bottom-1/3 right-1/4 w-3 h-3 bg-yellow-400 rounded-full animate-pulse delay-75" />
                        <div className="absolute top-1/2 left-1/2 w-3 h-3 bg-yellow-400 rounded-full animate-pulse delay-150" />
                    </div>
                </div>
            </div>
        </section>
    );
}
