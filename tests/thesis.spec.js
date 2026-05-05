const { test, expect } = require('@playwright/test');

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('sp_token', 'test-token');
  });

  await page.route('**/api/auth/me', route => {
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ username: 'admin' }),
    });
  });

  await page.route('**/v1/agents/health', route => {
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        generated_at: '2026-05-05T08:00:00Z',
        summary: { total_agents: 8, healthy: 2, stale: 1, never_run: 5 },
        agents: {
          'agent.fundamentals': {
            agent_id: 'agent.fundamentals',
            last_run: '2026-05-05T07:55:00Z',
            last_status: 'completed',
            avg_duration_secs: 1.2,
            success_rate: 1,
            total_runs_7d: 3,
            stale: false,
          },
          'agent.valuation': {
            agent_id: 'agent.valuation',
            last_run: null,
            last_status: 'never_run',
            avg_duration_secs: null,
            success_rate: null,
            total_runs_7d: 0,
            stale: true,
          },
        },
      }),
    });
  });

  await page.route('**/v1/thesis/MSFT/latest', route => {
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        thesis_id: 'thesis-1',
        run_id: 'run-1',
        ticker: 'MSFT',
        generated_at: '2026-05-05T08:00:00Z',
        current_price: 410.25,
        composite_score: 68.4,
        risk_rating: 'medium_low',
        evidence_quality: 'moderate',
        forecast: {
          '3m': { base_return_pct: 2.5, bull_return_pct: 6.1, bear_return_pct: -3.2, confidence: 0.61 },
          '6m': { base_return_pct: 5.8, bull_return_pct: 11.4, bear_return_pct: -5.7, confidence: 0.62 },
          '12m': { base_return_pct: 11.2, bull_return_pct: 22.5, bear_return_pct: -8.4, confidence: 0.64 },
        },
        drivers: ['Strong margin profile', 'Positive revision momentum'],
        risks: ['Valuation remains elevated'],
        agent_scores: {
          'agent.fundamentals': 74,
          'agent.valuation': 58,
          'agent.technical_risk': 66,
        },
        weighted_scores: { '3m': 64, '6m': 67, '12m': 68.4 },
        narrative: {
          bull: 'Upside case improves if growth accelerates.',
          base: 'Base case remains constructive.',
          bear: 'Downside case is valuation compression.',
        },
        quality_flags: [],
        decision_log: [],
      }),
    });
  });

  await page.route('**/v1/thesis/MSFT/quality', route => {
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        ticker: 'MSFT',
        thesis_id: 'thesis-1',
        evidence_quality: 'moderate',
        thesis_flags: {},
        agent_flags: {},
        usable_agents: 8,
        total_agents: 8,
      }),
    });
  });

  await page.route('**/v1/backtest/MSFT', route => {
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        ticker: 'MSFT',
        summary: {},
        calibration: {},
      }),
    });
  });
});

test('multi-agent thesis tab renders health and latest thesis', async ({ page }) => {
  await page.goto('/');
  await expect(page.locator('#login-overlay')).toBeHidden();

  await page.getByRole('button', { name: 'Thesis' }).click();
  await expect(page.locator('#tab-thesis')).toBeVisible();
  await expect(page.locator('#thesis-health')).toContainText('Agent Health');

  await page.fill('#thesis-ticker', 'MSFT');
  await page.getByRole('button', { name: 'Refresh Latest' }).click();

  await expect(page.locator('#thesis-output')).toContainText('Composite');
  await expect(page.locator('#thesis-output')).toContainText('68.4');
  await expect(page.locator('#thesis-output')).toContainText('Strong margin profile');
  await expect(page.locator('#thesis-output')).toContainText('Base case remains constructive');
});
