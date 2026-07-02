import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// In dev, Vite serves the SPA and proxies API calls to the collector so the
// frontend and backend can run separately. In prod the collector serves the
// built assets itself, so no proxy is needed.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8765",
    },
  },
});
