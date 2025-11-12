/** @type {import('tailwindcss').Config} */
export default {
  darkMode: 'class', // <-- ADD THIS LINE
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  safelist: [
    { pattern: /text-(indigo|blue|green|red|yellow)-400/ },
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}