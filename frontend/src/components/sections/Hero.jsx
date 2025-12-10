import euMap from '../../assets/Flag_map_of_the_European_Union.png';

export default function Hero() {
    return (
        <section className="relative z-10 max-w-6xl mx-auto px-6 pt-12 pb-32">
            <div className="grid md:grid-cols-2 gap-6 items-center">
                <div className="space-y-8">
                    <h1 className="text-5xl md:text-7xl font-bold text-accent leading-tight">
                        Crime & Immigration in the EU <br />
                        <span className="text-white font-normal text-5xl">is there a correlation?</span>
                    </h1>
                    <p className="text-lg md:text-xl text-blue-100 leading-relaxed max-w-xl">
                        Are crime and immigration related in the EU, or is it just a political ploy to gain voters?
                    </p>
                    <div className="animate-bounce m-20">
                        <div className="text-center text-xs font-mono uppercase tracking-widest text-blue-300">
                            Scroll to Begin
                        </div>
                        <div className="w-[1px] h-16 bg-accent mx-auto mt-4" />
                    </div>
                </div>
                <div className="flex justify-center items-center mt-8 md:mt-0">
                    <img
                        src={euMap}
                        alt="Flag Map of the European Union"
                        className="w-full max-w-lg h-auto drop-shadow-[0_0_20px_rgba(0,0,0,0.8)] opacity-95 hover:opacity-100 transition-opacity duration-700"
                    />
                </div>
            </div>
        </section>
    );
}
