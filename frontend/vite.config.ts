import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { "@": "/src" },
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          // Recharts + d3-* helpers (~300 kB) — cached separately, never changes with app code
          if (id.includes("recharts") || id.includes("node_modules/d3-")) {
            return "vendor-charts";
          }
          // React core — changes only on React version upgrades
          if (
            id.includes("node_modules/react/") ||
            id.includes("node_modules/react-dom/") ||
            id.includes("node_modules/scheduler/")
          ) {
            return "vendor-react";
          }
          // React Router
          if (id.includes("node_modules/react-router")) {
            return "vendor-router";
          }
          // TanStack Query
          if (id.includes("node_modules/@tanstack/")) {
            return "vendor-query";
          }
          // date-fns
          if (id.includes("node_modules/date-fns")) {
            return "vendor-dates";
          }
        },
      },
    },
  },
});
