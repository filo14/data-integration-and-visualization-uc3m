// Application Color Theme Configuration
// Centralizes all color definitions to ensure consistency and easier maintenance.

export const THEME = {
    colors: {
        // Semantic Data Colors
        crime: '#ef4444',      // Red-500
        immigration: '#10b981', // Emerald-500

        // Brand / Layout Colors
        primary: '#003399',    // EU Blue (approx)
        accent: '#facc15',     // Yellow-400 (Stars)

        // Chart Specifics
        grid: '#ffffff20',
        textStrong: '#e2efffff', // Slate-200
        text: '#94a3b8',       // Slate-400
        tooltipBg: '#1e293b',  // Slate-800

        // Map Colors
        mapLand: '#1e293b',
        mapCountry: '#334155',
        mapHover: 'rgba(15, 23, 42, 0.85)'
    },
    layout: {
        glass: 'bg-white/5 backdrop-blur-sm border border-white/10 shadow-2xl',
        cardPadding: 'p-6'
    }
};

// Helper for Tailwind class composition if needed
export const glassCardClasses = "bg-white/5 backdrop-blur-sm border border-white/10 shadow-2xl rounded-xl";
