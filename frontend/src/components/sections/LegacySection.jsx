import React, { useState } from 'react';
import VisualElement from '../ui/VisualElement';
import ScrollStep from '../ui/ScrollStep';

export default function LegacySection({ steps }) {
    const [activeStep, setActiveStep] = useState(0);

    const getActiveVisual = () => {
        const step = steps[activeStep];
        if (!step) return { type: 'flag', color: 'bg-blue-800' };

        return {
            type: step.visualType,
            color: step.visualColor,
            label: `${step.title} Data`,
            subLabel: `FIG ${step.id + 1}.0`
        };
    };

    return (
        <div className="relative max-w-7xl mx-auto px-4 py-32">
            <div className="flex flex-col md:flex-row">

                {/* LEFT COLUMN: Sticky Visuals */}
                <div className="md:w-1/2 sticky top-0 h-screen flex items-center justify-center p-8">
                    <VisualElement visual={getActiveVisual()} />
                </div>

                {/* RIGHT COLUMN: Scrolling Text */}
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
