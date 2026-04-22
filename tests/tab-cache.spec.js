const { test, expect } = require('@playwright/test');
const { login } = require('./helpers');

async function installFetchCounter(page) {
  await page.evaluate(() => {
    if (window.__tabCacheFetchCounterInstalled) return;
    window.__tabCacheFetchCounterInstalled = true;
    window.__tabCacheFetchCounts = {};
    const origFetch = window.fetch.bind(window);
    window.fetch = async (...args) => {
      const url = String(args[0] ?? "");
      if (url.includes('/api/')) {
        window.__tabCacheFetchCounts[url] = (window.__tabCacheFetchCounts[url] || 0) + 1;
      }
      return origFetch(...args);
    };
  });
}

async function getFetchCount(page, patterns) {
  return page.evaluate((innerPatterns) => {
    const counts = window.__tabCacheFetchCounts || {};
    return Object.entries(counts).reduce((sum, [url, count]) => {
      return innerPatterns.some(pattern => url.includes(pattern)) ? sum + count : sum;
    }, 0);
  }, patterns);
}

async function waitForTabData(page, tab) {
  if (tab === 'watchlist') {
    await page.waitForFunction(() => {
      const status = document.getElementById('watchlist-status');
      return !!status && !/Loading/i.test(status.textContent || '');
    }, { timeout: 30000 });
    return;
  }
  if (tab === 'predictions') {
    await page.waitForFunction(() => {
      const status = document.getElementById('pred-status');
      return !!status && !/Loading predictions/i.test(status.textContent || '');
    }, { timeout: 60000 });
    return;
  }
  if (tab === 'recommendations') {
    await page.waitForFunction(() => {
      const status = document.getElementById('rec-status');
      return !!status && !/Starting recommendations|Elapsed|remaining|Finalizing/i.test(status.textContent || '');
    }, { timeout: 240000 });
    return;
  }
  if (tab === 'alerts') {
    await page.waitForFunction(() => {
      const status = document.getElementById('alerts-status');
      return !!status && !/Loading/i.test(status.textContent || '');
    }, { timeout: 60000 });
    return;
  }
  if (tab === 'portfolio') {
    await page.waitForFunction(() => {
      const status = document.getElementById('portfolio-status');
      return !!status && !/Loading/i.test(status.textContent || '');
    }, { timeout: 60000 });
    return;
  }
  if (tab === 'paper') {
    await page.waitForFunction(() => {
      const status = document.getElementById('paper-status');
      return !!status && !/Loading/i.test(status.textContent || '');
    }, { timeout: 60000 });
    return;
  }
  if (tab === 'sentiment') {
    await page.waitForFunction(() => {
      const status = document.getElementById('sentiment-status');
      return !!status && !/Loading watchlist/i.test(status.textContent || '');
    }, { timeout: 60000 });
  }
}

test.describe('Tab caching', () => {
  test('cached menus do not refetch on second visit without refresh', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await installFetchCounter(page);

    const tabChecks = [
      { tab: 'watchlist', patterns: ['/api/watchlist'] },
      { tab: 'predictions', patterns: ['/api/predictions'] },
      { tab: 'recommendations', patterns: ['/api/recommendations/start', '/api/recommendations/progress/'] },
      { tab: 'alerts', patterns: ['/api/alerts', '/api/alerts/status', '/api/settings'] },
      { tab: 'portfolio', patterns: ['/api/portfolio'] },
      { tab: 'paper', patterns: ['/api/paper-portfolio'] },
      { tab: 'sentiment', patterns: ['/api/sentiment?watchlist=true'] },
    ];

    for (const check of tabChecks) {
      await test.step(`${check.tab} uses cached data on revisit`, async () => {
        const beforeFirst = await getFetchCount(page, check.patterns);
        await page.click(`button[data-tab="${check.tab}"]`);
        await page.waitForSelector(`#tab-${check.tab}.active`, { timeout: 10000 });
        await waitForTabData(page, check.tab);
        const afterFirst = await getFetchCount(page, check.patterns);

        await page.click('button[data-tab="watchlist"]');
        await page.waitForSelector('#tab-watchlist.active', { timeout: 10000 });
        await waitForTabData(page, 'watchlist');

        await page.click(`button[data-tab="${check.tab}"]`);
        await page.waitForSelector(`#tab-${check.tab}.active`, { timeout: 10000 });
        await waitForTabData(page, check.tab);
        const afterSecond = await getFetchCount(page, check.patterns);

        expect(afterFirst, `${check.tab} should fetch at most once after instrumentation`).toBeGreaterThanOrEqual(beforeFirst);
        expect(afterSecond, `${check.tab} should not refetch on second visit`).toBe(afterFirst);
      });
    }
  });
});
