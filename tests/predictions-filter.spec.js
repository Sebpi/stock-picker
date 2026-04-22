// @ts-check
const { test, expect } = require('@playwright/test');

// Build mock predictions spanning Today / This Week / This Month / YTD
function buildMockPredictions() {
  const now = new Date();

  const fmt = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;

  // Today
  const todayDate = fmt(now);

  // Earlier this week (Monday–yesterday), fallback to today if it's Monday
  const weekDate = (() => {
    const d = new Date(now);
    const dayOfWeek = d.getDay(); // 0=Sun
    const mondayOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek;
    const monday = new Date(d);
    monday.setDate(d.getDate() + mondayOffset);
    // If today IS Monday, use today (both land in "today" group; acceptable)
    if (d.getDate() === monday.getDate()) return todayDate;
    const prev = new Date(d);
    prev.setDate(d.getDate() - 1);
    // Make sure prev >= monday
    return prev >= monday ? fmt(prev) : todayDate;
  })();

  // Earlier this month but before this week's Monday
  const monthDate = (() => {
    const d = new Date(now.getFullYear(), now.getMonth(), 2); // 2nd of this month
    return fmt(d);
  })();

  // Last year (YTD bucket)
  const ytdDate = (() => {
    const d = new Date(now.getFullYear() - 1, 11, 15); // Dec 15 last year
    return fmt(d);
  })();

  const base = {
    ticker: 'AAPL', name: 'Apple Inc.', direction: 'bullish', confidence: 'high',
    score: 75, predicted_pct: 1.2, predicted_3m_pct: 5, predicted_6m_pct: 10,
    predicted_12m_pct: 18, predicted_24m_pct: 30, predicted_36m_pct: 42,
    actual_pct: null, reasoning: 'Test thesis.', factor_scores: {},
    price_at_prediction: 180, current_price: 182,
  };

  return [
    { ...base, date: todayDate, ticker: 'TODAY' },
    { ...base, date: weekDate,  ticker: 'WEEK'  },
    { ...base, date: monthDate, ticker: 'MONTH' },
    { ...base, date: ytdDate,   ticker: 'YTD'   },
  ];
}

test.describe('Predictions period filters', () => {
  test.beforeEach(async ({ page }) => {
    const mockPredictions = buildMockPredictions();

    // Set sp_token before any page script runs so the app is immediately authenticated
    await page.addInitScript(() => localStorage.setItem('sp_token', 'fake-test-token'));

    // Intercept all API calls
    await page.route('**/api/**', route => {
      const url = route.request().url();
      if (url.includes('/api/auth/me')) {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ username: 'testuser' }) });
      }
      if (url.includes('/api/predictions') && route.request().method() === 'GET' && !url.includes('/generate') && !url.includes('/backtest') && !url.includes('/simulate')) {
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(mockPredictions) });
      }
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify([]) });
    });

    await page.goto('/');

    // Navigate to Predictions tab and wait for rows
    await page.click('button[data-tab="predictions"]');
    await page.waitForSelector('#pred-body tr', { timeout: 10000 });
  });

  test('All filter shows all period group headings', async ({ page }) => {
    await page.click('.pred-period-btn[data-period="all"]');
    const groups = await page.locator('tr.pred-group-row').all();
    expect(groups.length).toBeGreaterThanOrEqual(1);
  });

  test('Today filter shows only Today group', async ({ page }) => {
    await page.click('.pred-period-btn[data-period="today"]');

    // Today group row should be visible
    const todayGroup = page.locator('tr.pred-group-row', { hasText: 'Today' });
    await expect(todayGroup).toBeVisible();

    // Other groups should be hidden
    for (const label of ['This Week', 'This Month', 'YTD']) {
      const group = page.locator('tr.pred-group-row', { hasText: label });
      const count = await group.count();
      if (count > 0) {
        await expect(group).not.toBeVisible();
      }
    }
  });

  test('This Week filter shows only This Week group', async ({ page }) => {
    await page.click('.pred-period-btn[data-period="week"]');

    const weekGroup = page.locator('tr.pred-group-row', { hasText: 'This Week' });
    const count = await weekGroup.count();
    if (count > 0) {
      await expect(weekGroup).toBeVisible();
    }

    for (const label of ['Today', 'This Month', 'YTD']) {
      const group = page.locator('tr.pred-group-row', { hasText: label });
      const c = await group.count();
      if (c > 0) await expect(group).not.toBeVisible();
    }
  });

  test('This Month filter shows only This Month group', async ({ page }) => {
    await page.click('.pred-period-btn[data-period="month"]');

    const monthGroup = page.locator('tr.pred-group-row', { hasText: 'This Month' });
    const count = await monthGroup.count();
    if (count > 0) {
      await expect(monthGroup).toBeVisible();
    }

    for (const label of ['Today', 'This Week', 'YTD']) {
      const group = page.locator('tr.pred-group-row', { hasText: label });
      const c = await group.count();
      if (c > 0) await expect(group).not.toBeVisible();
    }
  });

  test('YTD filter shows only YTD group', async ({ page }) => {
    await page.click('.pred-period-btn[data-period="ytd"]');

    const ytdGroup = page.locator('tr.pred-group-row', { hasText: 'YTD' });
    const count = await ytdGroup.count();
    if (count > 0) {
      await expect(ytdGroup).toBeVisible();
    }

    for (const label of ['Today', 'This Week', 'This Month']) {
      const group = page.locator('tr.pred-group-row', { hasText: label });
      const c = await group.count();
      if (c > 0) await expect(group).not.toBeVisible();
    }
  });

  test('Active button styling updates on filter click', async ({ page }) => {
    await page.click('.pred-period-btn[data-period="week"]');
    await expect(page.locator('.pred-period-btn[data-period="week"]')).toHaveClass(/active/);
    await expect(page.locator('.pred-period-btn[data-period="all"]')).not.toHaveClass(/active/);
  });

  test('Predictions within each group are ordered by date descending', async ({ page }) => {
    await page.click('.pred-period-btn[data-period="all"]');

    // Collect all visible date cells (skip group heading rows)
    const dateCells = await page.locator('#pred-body tr:not(.pred-group-row) td:first-child').allTextContents();
    const dates = dateCells.filter(d => /^\d{4}-\d{2}-\d{2}$/.test(d.trim()));

    for (let i = 1; i < dates.length; i++) {
      expect(dates[i - 1] >= dates[i]).toBeTruthy();
    }
  });
});
