import React, { useRef, useState, useEffect } from 'react';
import VisualElement from '../ui/VisualElement';

export default function StickySection({ title, text, visuals }) {
    const containerRef = useRef(null);
    const [activeVisualIndex, setActiveVisualIndex] = useState(0);

    useEffect(() => {
        const handleScroll = () => {
            if (!containerRef.current) return;
            const rect = containerRef.current.getBoundingClientRect();
            const sectionHeight = rect.height;
            const viewportHeight = window.innerHeight;

            // Calculate how far we've scrolled into this section
            const scrolled = -rect.top;

            if (scrolled < 0) {
                setActiveVisualIndex(0);
                return;
            }

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
                    <VisualElement visual={visuals[activeVisualIndex]} />
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
