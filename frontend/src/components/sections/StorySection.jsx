import React, { useState } from 'react';
import VisualElement from '../story/VisualElement';
import ScrollStep from '../story/ScrollStep';

export default function StorySection({ steps }) {
    const [activeStep, setActiveStep] = useState(0);

    const getActiveVisual = () => {
        const step = steps[activeStep];
        if (!step) return { type: 'flag', color: 'bg-blue-800' };

        return {
            type: step.visualType,
            color: step.visualColor,
            label: step.label || step.title,
            subLabel: `FIG ${step.id + 1}.0`,
            data: step.data,
            correlation: step.correlation,
            referenceLine: step.referenceLine // Pass reference line for benchmark
        };
    };

    return (
        <div className="relative max-w-7xl mx-auto px-4 py-32">
            <div className="flex flex-col md:flex-row-reverse">

                {/* VISUAL COLUMN (Right) */}
                <div className="md:w-1/2 sticky top-0 h-screen flex items-center justify-center p-8">
                    <VisualElement visual={getActiveVisual()} />
                </div>

                {/* SCROLLING TEXT COLUMN (Left) */}
                <div className="md:w-1/2">
                    {steps.map((step, index) => (
                        <ScrollStep
                            key={step.id}
                            step={step}
                            index={index}
                            setActiveStep={setActiveStep}
                        />
                    ))}
                    <div className="h-[50vh]"></div>
                </div>

            </div>
        </div>
    );
}
