import React from 'react';
import euStarsWithBackground from './assets/European_stars.svg.png';

export default function EuropeanStars() {
    return (
        <div
            className="fixed top-0 right-0 z-0 pointer-events-none select-none"
        >
            <img
                src={euStarsWithBackground}
                alt=""
                className="w-[500px] h-[500px] max-w-none opacity-60 mix-blend-screen"
                style={{
                    transform: 'translate(45%, -45%)',
                }}
            />
        </div>
    );
}
