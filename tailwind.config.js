/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./frontend/index.html",
    "./frontend/react-app.js",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ["Inter", "ui-sans-serif", "system-ui"],
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
      colors: {
        pulse: {
          bg:      "rgb(var(--c-pulse-bg) / <alpha-value>)",
          panel:   "rgb(var(--c-pulse-panel) / <alpha-value>)",
          card:    "rgb(var(--c-pulse-card) / <alpha-value>)",
          line:    "rgb(var(--c-pulse-line) / <alpha-value>)",
          ink:     "rgb(var(--c-pulse-ink) / <alpha-value>)",
          muted:   "rgb(var(--c-pulse-muted) / <alpha-value>)",
          dim:     "rgb(var(--c-pulse-dim) / <alpha-value>)",
          cyan:    "rgb(var(--c-pulse-cyan) / <alpha-value>)",
          magenta: "rgb(var(--c-pulse-magenta) / <alpha-value>)",
          lime:    "rgb(var(--c-pulse-lime) / <alpha-value>)",
          amber:   "rgb(var(--c-pulse-amber) / <alpha-value>)",
          red:     "rgb(var(--c-pulse-red) / <alpha-value>)",
          green:   "rgb(var(--c-pulse-green) / <alpha-value>)",
        }
      },
      boxShadow: {
        glow:    "0 0 28px rgb(var(--c-pulse-cyan) / .14)",
        magenta: "0 0 28px rgb(var(--c-pulse-magenta) / .12)",
      }
    }
  },
  plugins: [],
};
