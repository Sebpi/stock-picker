const { test, expect } = require('@playwright/test');
const { login } = require('./helpers');

test.describe('Predictions refresh', () => {
  test('not analysed rows do not show Pending in Actual percent', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await page.click('button[data-tab="predictions"]');
    await page.waitForSelector('#tab-predictions.active', { timeout: 10000 });

    await page.waitForFunction(() => {
      const status = document.getElementById('pred-status');
      return !!status && !/Loading predictions/i.test(status.textContent || '');
    }, { timeout: 30000 });

    const offendingRows = await page.locator('#pred-body tr').evaluateAll(rows => {
      return rows
        .map(row => {
          const cells = row.querySelectorAll('td');
          if (cells.length < 14) return null;
          const ticker = cells[1]?.textContent?.trim() || '';
          const actualText = cells[10]?.textContent?.trim() || '';
          const confidenceText = cells[13]?.textContent?.trim() || '';
          return { ticker, actualText, confidenceText };
        })
        .filter(row => row && /NOT ANALYSED/i.test(row.confidenceText) && /Pending/i.test(row.actualText));
    });

    expect(offendingRows, `Rows still showing Pending actuals for NOT ANALYSED stocks: ${JSON.stringify(offendingRows)}`).toEqual([]);
  });

  test('refresh button generates analysis for pending rows when needed', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await page.click('button[data-tab="predictions"]');
    await page.waitForSelector('#tab-predictions.active', { timeout: 10000 });

    await page.evaluate(() => {
      predictionsSnapshotCache = [
        {
          date: '',
          ticker: 'RR.L',
          name: 'Rolls-Royce Holdings plc',
          predicted_pct: null,
          direction: 'pending',
          score: null,
          confidence: 'pending',
          reasoning: 'Not yet analysed.',
          actual_pct: null,
          predicted_3m_pct: null,
          predicted_6m_pct: null,
          predicted_12m_pct: null,
          predicted_24m_pct: null,
          predicted_36m_pct: null,
        },
      ];
      predictionsRenderedCache = null;
      renderPredictionsTable(predictionsSnapshotCache);
      document.getElementById('pred-empty').classList.remove('visible');
    });

    await page.route('**/api/predictions', async route => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify([
          {
            date: '',
            ticker: 'RR.L',
            name: 'Rolls-Royce Holdings plc',
            predicted_pct: null,
            direction: 'pending',
            score: null,
            confidence: 'pending',
            reasoning: 'Not yet analysed.',
            actual_pct: null,
          },
        ]),
      });
    });

    let generateCalled = 0;
    await page.route('**/api/predictions/generate', async route => {
      generateCalled += 1;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          predictions: [
            {
              date: '2026-04-21',
              ticker: 'RR.L',
              name: 'Rolls-Royce Holdings plc',
              predicted_pct: 1.2,
              confidence: 'medium',
            },
          ],
        }),
      });
    });

    await page.click('#btn-refresh-preds');
    await expect.poll(() => generateCalled).toBe(1);
  });
});
