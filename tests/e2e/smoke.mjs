#!/usr/bin/env node
/**
 * E2E smoke test for StockLens.
 *
 * Checks for regressions that API tests can't catch:
 *   - JS runtime errors on load
 *   - Blank-screen crashes
 *   - Stale service-worker serving a broken bundle
 *
 * Run:
 *   node tests/e2e/smoke.mjs                           # against live deploy
 *   TARGET_URL=http://localhost:8000 node …            # against local dev
 *
 * Requires Playwright (installed by CI via `npm i playwright`).
 */

import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);

let chromium = null;
try { ({ chromium } = require('playwright')); } catch {
  console.error('FAIL: playwright not found — npm i playwright');
  process.exit(2);
}

const TARGET = process.env.TARGET_URL || 'https://stock-picker-sp.fly.dev';

const checks = [];
function check(name, ok, detail = '') {
  checks.push({ name, ok, detail });
  console.log(`${ok ? '✓' : '✗'} ${name}${detail ? '  — ' + detail : ''}`);
}

(async () => {
  const browser = await chromium.launch();
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await ctx.newPage();

  const consoleErrors = [];
  const pageErrors = [];
  page.on('pageerror', (e) => pageErrors.push(e.message));
  page.on('console', (m) => { if (m.type() === 'error') consoleErrors.push(m.text()); });

  // Load the page
  const response = await page.goto(TARGET, { waitUntil: 'networkidle' });
  check('page returns 200', response?.status() === 200, `status: ${response?.status()}`);

  // App root has rendered content (no blank-screen crash)
  const appRootChildren = await page.evaluate(
    () => document.querySelector('#root')?.children.length || 0
  );
  check('app root has children (no blank screen)', appRootChildren > 0);

  // Page renders either a login form or the authenticated app
  const hasLoginInput = await page.locator('input[type="password"]').count() > 0;
  const hasAppChrome  = await page.locator('h1, [data-tab], .tab-btn').count() > 0;
  check('page renders auth or app content', hasLoginInput || hasAppChrome);

  // Health API is responding
  const health = await page.evaluate(async (base) => {
    try {
      const r = await fetch(`${base}/api/health`);
      return r.ok ? r.json() : null;
    } catch { return null; }
  }, TARGET);
  check('health API returns ok', health?.status === 'ok',
        health ? `status=${health.status}` : 'no response');

  // No uncaught page-level JS errors
  check('no page-level JS errors', pageErrors.length === 0,
        pageErrors.length ? pageErrors[0] : '');

  // Ignore noisy CDN/workbox console errors; only hard crashes matter
  const hardErrors = consoleErrors.filter((m) =>
    /Uncaught|TypeError|Cannot read|undefined is not/i.test(m)
  );
  check('no hard console errors', hardErrors.length === 0,
        hardErrors.length ? hardErrors[0] : '');

  await browser.close();

  const failed = checks.filter((c) => !c.ok);
  if (failed.length) {
    console.error(`\n${failed.length}/${checks.length} checks FAILED`);
    process.exit(1);
  }
  console.log(`\nAll ${checks.length} checks passed against ${TARGET}`);
})().catch((e) => { console.error('FATAL:', e); process.exit(2); });
