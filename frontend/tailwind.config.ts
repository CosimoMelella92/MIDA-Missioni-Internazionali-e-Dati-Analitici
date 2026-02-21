/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        mida: {
          navy: '#264653',
          teal: '#2A9D8F',
          gold: '#E9C46A',
          coral: '#E76F51',
          green: '#06D6A0',
          dark: '#1A1A2E',
          light: '#FAFAFA',
        },
        org: {
          onu: '#1F77B4',
          nato: '#2CA02C',
          ue: '#FF7F0E',
          ita: '#D62728',
          bilateral: '#9467BD',
          multinational: '#8C564B',
          coalizione: '#E377C2',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      },
    },
  },
  plugins: [],
}
