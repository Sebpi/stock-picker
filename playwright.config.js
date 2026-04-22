// @ts-check
const { defineConfig } = require('@playwright/test');

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
