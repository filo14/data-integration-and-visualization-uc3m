import React, { useRef, useState, useEffect } from 'react';
import VisualElement from '../story/VisualElement';

export default function StickySection({ title, subtitle, text, followText, visuals }) {
    const containerRef = useRef(null);
    const [activeVisualIndex, setActiveVisualIndex] = useState(0);

    useEffect(() => {
        const handleScroll = () => {
            if (!containerRef.current) return;
            const rect = containerRef.current.getBoundingClientRect();
            const sectionHeight = rect.height;
            const viewportHeight = window.innerHeight;

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
            <div className="sticky top-0 h-screen w-full flex flex-col md:flex-row overflow-hidden bg-primary-dark">

                {/* LEFT: VISUALS (Changing) */}
                <div className="md:w-1/2 h-full flex items-center justify-center p-8 bg-black/20">
                    <VisualElement visual={visuals[activeVisualIndex]} />
                </div>

                {/* RIGHT: TEXT (Static) */}
                <div className="md:w-1/2 h-full flex items-center justify-center p-8 md:p-16">
                    <div className="max-w-xl">
                        <h2 className="text-4xl md:text-5xl font-bold mb-8">{title}</h2>
                        <h3 className="text-xl md:text-2xl font-light mb-8">{subtitle}</h3>
                        <p className="text-xl text-blue-100 leading-relaxed mb-8">
                            {text}
                        </p>
                        {followText && (
                            <p className="text-xl font-bold text-blue-100 leading-relaxed mb-8">
                                {followText}
                            </p>
                        )}
                    </div>
                </div>

            </div>
        </div>
    );
}
