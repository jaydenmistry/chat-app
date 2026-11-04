import { defineConfig, devices } from "@playwright/test";

// The only Node global this file uses, declared here instead of pulling in
// all of @types/node.
declare const process: { env: Record<string, string | undefined> };

/**
 * End-to-end tests run against the full local stack (`docker compose up`):
 * the Vite dev server in front of both server instances. Set BASE_URL to
 * point elsewhere.
 */
export default defineConfig({
  testDir: "e2e",
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: 0,
  reporter: process.env.CI ? [["list"], ["html", { open: "never" }]] : "list",
  use: {
    baseURL: process.env.BASE_URL ?? "http://localhost:5173",
    trace: "retain-on-failure",
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "firefox", use: { ...devices["Desktop Firefox"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },
  ],
});
