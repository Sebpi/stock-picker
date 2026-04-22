const { test } = require('@playwright/test');

test('debug page state', async ({ page }) => {
  const intercepted = [];
  page.on('request', req => { if (req.url().includes('/api/')) intercepted.push(req.url()); });
  page.on('console', msg => console.log('PAGE LOG:', msg.text()));
  page.on('pageerror', err => console.log('PAGE ERROR:', err.message));

  const mockPredictions = [
    { date: '2026-04-16', ticker: 'TODAY', name: 'Today Stock', direction: 'bullish', confidence: 'high', score: 75, predicted_pct: 1.2, factor_scores: {} }
  ];

  await page.route('**/api/**', route => {
    const url = route.request().url();
    if (url.includes('/api/auth/me')) return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ username: 'testuser' }) });
    if (url.includes('/api/predictions') && !url.includes('/generate') && !url.includes('/backtest') && !url.includes('/simulate')) {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(mockPredictions) });
    }
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify([]) });
  });

  await page.goto('/');
  await page.evaluate(() => localStorage.setItem('sp_token', 'fake-test-token'));
  await page.reload();
  await page.waitForTimeout(2000);
  console.log('body classes after reload:', await page.evaluate(() => document.body.className));

  // Check state before clicking tab
  const loginVisible = await page.evaluate(() => document.getElementById('login-overlay')?.style?.display);
  console.log('login-overlay display before click:', loginVisible);
  console.log('intercepted API calls so far:', intercepted);

  await page.click('button[data-tab="predictions"]');
  await page.waitForTimeout(3000);
  await page.screenshot({ path: 'tests/debug2.png', fullPage: true });

  console.log('All intercepted API calls:', intercepted);

  // Try calling loadPredictions directly
  const callResult = await page.evaluate(async () => {
    if (typeof loadPredictions === 'function') {
      try { await loadPredictions(true); return 'called'; } catch(e) { return 'error: ' + e.message; }
    }
    return 'not a function';
  });
  console.log('loadPredictions direct call:', callResult);
  await page.waitForTimeout(2000);

  const predBody = await page.locator('#pred-body').innerHTML().catch(() => 'NOT FOUND');
  console.log('pred-body innerHTML (first 500):', predBody.slice(0, 500));
  const predStatus = await page.locator('#pred-status').textContent().catch(() => 'NOT FOUND');
  console.log('pred-status text:', predStatus);
});
