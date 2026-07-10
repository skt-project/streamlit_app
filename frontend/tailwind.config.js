/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        primary: {
          50:  "#eff6ff",
          100: "#dbeafe",
          200: "#bfdbfe",
          300: "#93c5fd",
          400: "#60a5fa",
          500: "#3b82f6",
          600: "#2563eb",
          700: "#1d4ed8",
          800: "#1e40af",
          900: "#1e3a8a",
        },
        // Surface tokens for consistent usage
        surface: {
          DEFAULT: "#f8fafc",   // slate-50 — page background
          elevated: "#ffffff",  // white — card surface
          border: "#e2e8f0",    // slate-200
          muted: "#f1f5f9",     // slate-100
        },
      },
      fontFamily: {
        sans: [
          "Inter",
          "ui-sans-serif",
          "system-ui",
          "-apple-system",
          "BlinkMacSystemFont",
          '"Segoe UI"',
          "Roboto",
          '"Helvetica Neue"',
          "Arial",
          "sans-serif",
        ],
        mono: [
          "ui-monospace",
          "SFMono-Regular",
          "Menlo",
          "Monaco",
          "Consolas",
          '"Liberation Mono"',
          '"Courier New"',
          "monospace",
        ],
      },
      fontSize: {
        "2xs": ["0.6875rem", { lineHeight: "1rem" }],     // 11px — caps labels
      },
      boxShadow: {
        card:    "0 1px 3px 0 rgb(0 0 0 / 0.07), 0 1px 2px -1px rgb(0 0 0 / 0.05)",
        "card-md": "0 4px 6px -1px rgb(0 0 0 / 0.07), 0 2px 4px -2px rgb(0 0 0 / 0.05)",
        "card-lg": "0 10px 15px -3px rgb(0 0 0 / 0.07), 0 4px 6px -4px rgb(0 0 0 / 0.04)",
        primary: "0 4px 14px 0 rgb(37 99 235 / 0.22)",
        inner:   "inset 0 1px 2px 0 rgb(0 0 0 / 0.06)",
      },
      borderRadius: {
        sm:  "0.375rem",   // 6px
        md:  "0.5rem",     // 8px
        DEFAULT: "0.5rem", // 8px
        lg:  "0.75rem",    // 12px
        xl:  "0.875rem",   // 14px
        "2xl": "1rem",     // 16px
        "3xl": "1.25rem",  // 20px
      },
      animation: {
        "pulse-fast":    "pulse 1.2s cubic-bezier(0.4, 0, 0.6, 1) infinite",
        "fade-in":       "fadeIn 0.18s ease-out both",
        "slide-down":    "slideDown 0.2s ease-out both",
        "slide-up":      "slideUp 0.2s ease-out both",
        "scale-in":      "scaleIn 0.15s ease-out both",
      },
      keyframes: {
        fadeIn: {
          "0%":   { opacity: "0" },
          "100%": { opacity: "1" },
        },
        slideDown: {
          "0%":   { opacity: "0", transform: "translateY(-6px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
        slideUp: {
          "0%":   { opacity: "0", transform: "translateY(6px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
        scaleIn: {
          "0%":   { opacity: "0", transform: "scale(0.96)" },
          "100%": { opacity: "1", transform: "scale(1)" },
        },
      },
    },
  },
  plugins: [],
};
