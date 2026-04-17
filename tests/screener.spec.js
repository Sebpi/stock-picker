const { test, expect } = require('@playwright/test');
const { login } = require('./helpers');

test.describe('Stock Screener', () => {

  test.beforeEach(async ({ page }) => {
    await login(page);
    // Click Screener tab if not already active
    const screenerTab = page.locator('button[data-tab="screener"]');
    await screenerTab.waitFor({ timeout: 5000 });
    const isActive = await screenerTab.evaluate(el => el.classList.contains('active'));
    if (!isActive) await screenerTab.click();
    await expect(page.locator('#btn-screen')).toBeVisible({ timeout: 5000 });
  });

  test('blank filters return results', async ({ page }) => {
    // Ensure all filters are empty
    await page.fill('#filter-pe', '');
    await page.fill('#filter-peg', '');
    await page.fill('#filter-pb', '');
    await page.fill('#filter-ev', '');
    await page.fill('#filter-fcf', '');
    await page.fill('#filter-cap', '');
    await page.fill('#filter-vol', '');
    await page.fill('#filter-rev-growth', '');
    await page.selectOption('#filter-index', '');
    await page.selectOption('#filter-sector', '');

    await page.click('#btn-screen');

    // Wait up to 90s — screener fetches hundreds of live tickers from yfinance
    await expect(page.locator('#screen-status')).not.toContainText('Screening', { timeout: 90_000 });

    const status = await page.locator('#screen-status').textContent();
    expect(status).not.toMatch(/no stocks matched/i);

    const rows = page.locator('#screen-body tr');
    await expect(rows.first()).toBeVisible({ timeout: 5000 });
    const count = await rows.count();
    expect(count).toBeGreaterThan(0);

    console.log(`Screener returned ${count} stocks with blank filters`);
  });

  test('sector filter narrows results', async ({ page }) => {
    await page.selectOption('#filter-sector', 'Technology');
    await page.click('#btn-screen');

    await expect(page.locator('#screen-status')).not.toContainText('Screening', { timeout: 90_000 });

    const rows = page.locator('#screen-body tr');
    const count = await rows.count();

    if (count > 0) {
      // Every visible sector cell should say Technology
      const sectors = await page.locator('#screen-body tr td:nth-child(3)').allTextContents();
      for (const s of sectors) {
        expect(s.toLowerCase()).toContain('tech');
      }
      console.log(`Technology filter returned ${count} stocks`);
    } else {
      // yfinance may return nothing outside market hours — not a failure
      console.log('No results for Technology sector (may be outside market hours)');
    }
  });

  test('tooltips update when sector changes', async ({ page }) => {
    // Default (All Sectors) tooltip for P/E should mention generic thresholds
    const peTip = page.locator('.filter-group:has(#filter-pe) .tip');
    await expect(peTip).toBeVisible();

    const defaultTip = await peTip.getAttribute('data-tip');
    expect(defaultTip).toContain('Price-to-Earnings');

    // Switch to Technology — tooltip should update to tech-specific guidance
    await page.selectOption('#filter-sector', 'Technology');
    const techTip = await peTip.getAttribute('data-tip');
    expect(techTip).toContain('Technology');
    expect(techTip).not.toBe(defaultTip);

    // Placeholder should also update
    const peInput = page.locator('#filter-pe');
    const placeholder = await peInput.getAttribute('placeholder');
    expect(placeholder).toBe('e.g. 35');

    // Switch to Real Estate — tooltip should warn P/E is less meaningful for REITs
    await page.selectOption('#filter-sector', 'Real Estate');
    const reitTip = await peTip.getAttribute('data-tip');
    expect(reitTip).toContain('FFO');

    console.log('Sector tooltip updates working correctly');
  });

  test('P/E max filter excludes high-PE stocks', async ({ page }) => {
    await page.selectOption('#filter-index', 'sp500');

    // Set P/E ≤ 20
    const peBtn = page.locator('.filter-op-btn[data-filter="pe"]');
    if ((await peBtn.textContent()) !== '≤') await peBtn.click();
    await page.fill('#filter-pe', '20');

    await page.click('#btn-screen');
    await expect(page.locator('#screen-status')).not.toContainText('Screening', { timeout: 90_000 });

    const rows = page.locator('#screen-body tr');
    const count = await rows.count();
    console.log(`P/E ≤ 20 on S&P 500 returned ${count} stocks`);

    // If results exist, spot-check a few P/E values in column 5 (index 4)
    if (count > 0) {
      const peCells = await page.locator('#screen-body tr td:nth-child(5)').allTextContents();
      for (const cell of peCells.slice(0, 10)) {
        const val = parseFloat(cell);
        if (!isNaN(val)) {
          expect(val).toBeLessThanOrEqual(20.5); // small float tolerance
        }
      }
    }
  });

  test('search by ticker finds specific stock', async ({ page }) => {
    // The screener search box triggers /api/search on Enter — wait for that status message
    await page.fill('#screener-search', 'AAPL');
    await page.keyboard.press('Enter');

    // Search uses "results for" phrasing, not "found"
    await expect(page.locator('#screen-status')).toContainText(/results? for|found|No stocks/, { timeout: 30_000 });

    const status = await page.locator('#screen-status').textContent();
    expect(status).not.toMatch(/no stocks/i);

    const rows = page.locator('#screen-body tr');
    await expect(rows.first()).toBeVisible({ timeout: 5000 });
    const count = await rows.count();
    expect(count).toBeGreaterThanOrEqual(1);

    const firstTicker = await page.locator('#screen-body tr:first-child td:first-child').textContent();
    expect(firstTicker?.trim()).toBe('AAPL');
    console.log('Ticker search for AAPL found correctly');
  });

});
