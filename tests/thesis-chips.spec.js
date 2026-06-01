const { test, expect } = require('@playwright/test');

/**
 * Verifies the Thesis tab watchlist chips are colour-coded (RAG) by their
 * latest composite score, pre-loaded from /v1/thesis/scores on page load
 * — without the user having to click each ticker.
 *
 * Bands (see Thesis component in react-app.js):
 *   score >= 70 → emerald (green)
 *   score >= 45 → amber
 *   score <  45 → red
 *   no score    → neutral (pulse-line)
 *   active chip → cyan (overrides colour)
 */

const WATCHLIST = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'NVDA'];
const SCORES = { AAPL: 82, GOOGL: 88, MSFT: 55, TSLA: 30 }; // NVDA intentionally absent

function thesisFor(ticker, score) {
  return {
    thesis_id: `thesis-${ticker}`,
    run_id: 'run-1',
    ticker,
    company_name: `${ticker} Inc.`,
    generated_at: '2026-05-05T08:00:00Z',
    current_price: 100,
    composite_score: score,
    risk_rating: 'medium',
    evidence_quality: 'moderate',
    forecast: {
      '3m': { base_return_pct: 1, bull_return_pct: 3, bear_return_pct: -2, confidence: 0.6 },
      '6m': { base_return_pct: 2, bull_return_pct: 5, bear_return_pct: -3, confidence: 0.6 },
      '12m': { base_return_pct: 4, bull_return_pct: 9, bear_return_pct: -5, confidence: 0.6 },
    },
    drivers: [], risks: [],
    agent_scores: {}, agent_meta: {}, weighted_scores: {},
    narrative: { bull: '', base: '', bear: '' },
    quality_flags: [], decision_log: [],
  };
}

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('stocklens_token', 'test-token');
  });

  // Catch-all fallbacks first (lower priority than later, specific routes)
  await page.route('**/v1/**', route =>
    route.fulfill({ status: 200, contentType: 'application/json', body: '{}' }));
  await page.route('**/api/**', route =>
    route.fulfill({ status: 200, contentType: 'application/json', body: '{}' }));

  await page.route('**/api/auth/me', route =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ username: 'admin' }) }));

  await page.route('**/api/watchlist**', route =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ watchlist: WATCHLIST }) }));

  await page.route('**/v1/thesis/scores**', route =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(SCORES) }));

  // loadLatest(first) fires on mount for AAPL, plus any chip click
  await page.route(/\/v1\/thesis\/[A-Z]+\/latest/, route => {
    const m = route.request().url().match(/\/v1\/thesis\/([A-Z]+)\/latest/);
    const t = m ? m[1] : 'AAPL';
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(thesisFor(t, SCORES[t] ?? 50)) });
  });

  await page.route(/\/v1\/thesis\/[A-Z]+\/history/, route =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ticker: 'AAPL', theses: [] }) }));
});

test('watchlist chips are RAG colour-coded from pre-loaded scores', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Thesis' }).click();

  // Chips render once the watchlist resolves
  await expect(page.getByTestId('thesis-chip-GOOGL')).toBeVisible();

  // GOOGL 88 → emerald (green); not the active ticker
  await expect(page.getByTestId('thesis-chip-GOOGL')).toHaveClass(/emerald/);
  // MSFT 55 → amber
  await expect(page.getByTestId('thesis-chip-MSFT')).toHaveClass(/amber/);
  // TSLA 30 → red
  await expect(page.getByTestId('thesis-chip-TSLA')).toHaveClass(/red/);
  // NVDA has no score → neutral default (no RAG colour)
  await expect(page.getByTestId('thesis-chip-NVDA')).toHaveClass(/pulse-line/);
  await expect(page.getByTestId('thesis-chip-NVDA')).not.toHaveClass(/emerald|amber|red/);

  // Band data-attribute mirrors the score bands
  await expect(page.getByTestId('thesis-chip-GOOGL')).toHaveAttribute('data-band', 'high');
  await expect(page.getByTestId('thesis-chip-MSFT')).toHaveAttribute('data-band', 'mid');
  await expect(page.getByTestId('thesis-chip-TSLA')).toHaveAttribute('data-band', 'low');
});

test('first watchlist ticker is the active (cyan) chip on load', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Thesis' }).click();

  // AAPL is auto-selected on mount → cyan active style, not its emerald score colour
  await expect(page.getByTestId('thesis-chip-AAPL')).toHaveClass(/pulse-cyan/);
});

test('clicking a chip makes it active and colours it cyan', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Thesis' }).click();

  const tsla = page.getByTestId('thesis-chip-TSLA');
  await expect(tsla).toHaveClass(/red/);   // starts red (score 30)
  await tsla.click();
  await expect(tsla).toHaveClass(/pulse-cyan/);  // becomes active
});
