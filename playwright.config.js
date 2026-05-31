// @ts-check
const { defineConfig } = require('@playwright/test');

/**
 * Test harness: starts a frontend proxy server on :4321 that forwards
 * /api/* and /v1/* to the FastAPI backend on :8000.
 *
 * Before running tests, start the backend:
 *   cd backend && SP_PASS=stockpicker2024 uvicorn main:app --port 8000
 *   (or set SP_USER / SP_PASS env vars to match your admin credentials)
 */
module.exports = defineConfig({
  testDir: './tests',
  timeout: 30000,
  use: {
    baseURL: 'http://localhost:4321',
    headless: true,
  },
  webServer: {
    command: 'node tests/server.js',
    port: 4321,
    reuseExistingServer: true,
    timeout: 10000,
  },
});
