/// <reference types="vitest/config" />
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  // In development the browser talks only to Vite (one origin, so the
  // session cookie is first-party). Vite forwards API and WebSocket traffic
  // to the load balancer in front of the two server instances.
  const backend = loadEnv(mode, ".", "").BACKEND_URL || "http://localhost:8080";
  return {
    plugins: [react()],
    server: {
      host: true,
      port: 5173,
      strictPort: true,
      proxy: {
        "/api": { target: backend },
        "/ws": { target: backend, ws: true },
      },
    },
    test: {
      environment: "jsdom",
      include: ["src/**/*.test.{ts,tsx}"],
    },
  };
});
