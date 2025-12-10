import { useRef, useEffect } from 'react';

export default function ScrollStep({ step, index, setActiveStep }) {
    const ref = useRef(null);

    useEffect(() => {
        const observer = new IntersectionObserver(
            ([entry]) => {
                if (entry.isIntersecting) setActiveStep(index);
            },
            { root: null, rootMargin: "-40% 0px -40% 0px", threshold: 0.2 }
        );

        if (ref.current) observer.observe(ref.current);

        return () => {
            if (ref.current) observer.unobserve(ref.current);
        };
    }, [index, setActiveStep]);

    return (
        <div ref={ref} className="min-h-screen flex items-center justify-center p-8 md:p-16 border-l border-blue-800/50">
            <div className="transition-opacity duration-500">
                <span className="font-bold text-6xl opacity-40 block mb-4">
                    {String(index + 1).padStart(2, '0')}
                </span>
                <h2 className="text-3xl md:text-4xl font-bold text-white mb-6">
                    {step.title}
                </h2>
                <p className="text-lg md:text-xl text-blue-100 leading-relaxed">
                    {step.content}
                </p>
            </div>
        </div>
    );
}
