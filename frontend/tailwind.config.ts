/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        mil: {
          olive: '#4A5D23',
          'olive-dark': '#3D4F1E',
          'olive-light': '#6B8C2A',
          navy: '#1B3A5C',
          'navy-light': '#2C5F8A',
          sand: '#F5F3EE',
          'sand-dark': '#EAE6DC',
          'sand-deep': '#D4CFC3',
          red: '#8B1A1A',
          steel: '#5A5F63',
          'steel-light': '#8B9298',
          khaki: '#7D6B3A',
          black: '#1A1A1A',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
        display: ['Inter', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
