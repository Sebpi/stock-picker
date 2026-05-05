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

  await page.route('**/v1/operations/status', route => {
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        generated_at: '2026-05-05T08:05:00Z',
        agents: { total_agents: 8, healthy: 2, stale: 1, never_run: 5 },
        thesis_scheduler: {
          enabled: false,
          active: false,
          runs_started: 1,
          last_run: '2026-05-05T07:00:00Z',
          last_error: null,
        },
        evaluation_scheduler: {
          enabled: true,
          active: false,
          runs_started: 2,
          last_evaluated_count: 3,
          last_error: null,
        },
        forecast_outcomes: {
          total: 9,
          pending: 6,
          evaluated: 3,
          matured_pending: 1,
          last_evaluated_at: '2026-05-05 07:30:00',
          by_horizon: {},
        },
        recent_runs: [
          {
            run_id: 'run-1',
            status: 'completed',
            tickers: ['MSFT'],
            started_at: '2026-05-05T07:00:00Z',
            completed: ['MSFT'],
            failed: [],
          },
        ],
        recent_failures: [],
      }),
    });
  });

  await page.route('**/v1/evaluate**', route => {
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ ok: true, message: 'Evaluation job started in background' }),
    });
  });

  const mockThesis = {
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
    agent_meta: {
      'agent.fundamentals': { direction: 'positive', confidence: 'high', flags: [], usable: true },
      'agent.valuation':    { direction: 'neutral',  confidence: 'medium', flags: ['MISSING_FIELD'], usable: true },
      'agent.technical_risk': { direction: 'positive', confidence: 'medium', flags: [], usable: true },
    },
    weighted_scores: { '3m': 64, '6m': 67, '12m': 68.4 },
    narrative: {
      bull: 'Upside case improves if growth accelerates.',
      base: 'Base case remains constructive.',
      bear: 'Downside case is valuation compression.',
    },
    quality_flags: ['LLM_UNVERIFIED'],
    decision_log: [
      { action: 'agents_run', agent_id: '', reason: 'Fresh run requested' },
      { action: 'stale_agents', agent_id: 'agent.macro_liquidity', reason: 'Stale: [agent.macro_liquidity]' },
    ],
  };

  await page.route('**/v1/thesis/MSFT/latest', route => {
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(mockThesis),
    });
  });

  await page.route('**/v1/thesis/MSFT/history**', route => {
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        ticker: 'MSFT',
        theses: [
          { thesis_id: 'thesis-1', generated_at: '2026-05-05T08:00:00Z', composite_score: 68.4, risk_rating: 'medium_low', evidence_quality: 'moderate', current_price: 410.25 },
          { thesis_id: 'thesis-0', generated_at: '2026-05-04T08:00:00Z', composite_score: 65.1, risk_rating: 'medium',     evidence_quality: 'moderate', current_price: 405.10 },
        ],
      }),
    });
  });

  await page.route('**/v1/metrics/latest**', route => {
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        buffer_size: 500,
        metrics: [
          { metric: 'thesis_run_duration_secs', value: 12.3, labels: { ticker: 'MSFT', status: 'ok' }, timestamp: '2026-05-05T08:00:01Z' },
          { metric: 'agent_score', value: 74.0, labels: { agent: 'agent.fundamentals', ticker: 'MSFT' }, timestamp: '2026-05-05T08:00:00Z' },
        ],
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
  await expect(page.locator('#thesis-ops')).toContainText('Operations Status');
  await expect(page.locator('#thesis-ops')).toContainText('1 ready to evaluate');

  await page.fill('#thesis-ticker', 'MSFT');
  await page.getByRole('button', { name: 'Refresh Latest' }).click();

  await expect(page.locator('#thesis-output')).toContainText('Composite');
  await expect(page.locator('#thesis-output')).toContainText('68.4');
  await expect(page.locator('#thesis-output')).toContainText('Strong margin profile');
  await expect(page.locator('#thesis-output')).toContainText('Base case remains constructive');

  await page.getByRole('button', { name: 'Evaluate Outcomes' }).click();
  await expect(page.locator('#thesis-status')).toContainText('Evaluation job started');
});

test('agent score panel shows direction arrows and confidence labels', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Thesis' }).click();
  await page.fill('#thesis-ticker', 'MSFT');
  await page.getByRole('button', { name: 'Refresh Latest' }).click();

  const scoreList = page.locator('.thesis-agent-score-list');
  await expect(scoreList).toBeVisible();
  // fundamentals is positive → ▲
  await expect(scoreList).toContainText('▲');
  // valuation is neutral → –
  await expect(scoreList).toContainText('–');
  // confidence labels present
  await expect(scoreList).toContainText('high');
  await expect(scoreList).toContainText('medium');
});

test('decision log panel is rendered and collapsible', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Thesis' }).click();
  await page.fill('#thesis-ticker', 'MSFT');
  await page.getByRole('button', { name: 'Refresh Latest' }).click();

  const log = page.locator('.thesis-decision-log').first();
  await expect(log).toBeVisible();
  await expect(log.locator('summary')).toContainText('How this thesis was built');
  await expect(log.locator('summary')).toContainText('2 steps');

  // Expand and check contents
  await log.locator('summary').click();
  await expect(log).toContainText('agents_run');
  await expect(log).toContainText('Fresh run requested');
  await expect(log).toContainText('stale_agents');
});

test('thesis history panel loads and shows past snapshots', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Thesis' }).click();
  await page.fill('#thesis-ticker', 'MSFT');
  await page.getByRole('button', { name: 'Refresh Latest' }).click();

  const historyDetails = page.locator('#thesis-history-details');
  await expect(historyDetails).toBeVisible();
  await historyDetails.locator('summary').click();

  // History rows should populate
  await expect(page.locator('#thesis-history-content')).toContainText('68.4');
  await expect(page.locator('#thesis-history-content')).toContainText('65.1');
});

test('ops dashboard shows thesis and evaluation scheduler cards', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Thesis' }).click();

  const ops = page.locator('#thesis-ops');
  await expect(ops).toContainText('Thesis Scheduler');
  await expect(ops).toContainText('Evaluation Scheduler');
  await expect(ops).toContainText('Forecast Outcomes');
  // Scheduler status values
  await expect(ops).toContainText('disabled'); // thesis_scheduler.enabled = false
  await expect(ops).toContainText('enabled');  // evaluation_scheduler.enabled = true
});

test('quality flags show tooltip text on hover', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Thesis' }).click();
  await page.fill('#thesis-ticker', 'MSFT');
  await page.getByRole('button', { name: 'Refresh Latest' }).click();

  // LLM_UNVERIFIED pill should have a title attribute
  const pill = page.locator('.thesis-pill', { hasText: 'LLM_UNVERIFIED' });
  await expect(pill).toBeVisible();
  const title = await pill.getAttribute('title');
  expect(title).toBeTruthy();
  expect(title.length).toBeGreaterThan(10);
});

test('recent metrics panel loads after clicking Load', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Thesis' }).click();
  await page.getByRole('button', { name: 'Operations' }).click();

  const metricsSection = page.locator('#thesis-metrics-section');
  await expect(metricsSection).toBeVisible();
  await metricsSection.getByRole('button', { name: 'Load' }).click();

  await expect(page.locator('#thesis-metrics-content')).toContainText('thesis_run_duration_secs');
  await expect(page.locator('#thesis-metrics-content')).toContainText('agent_score');
  await expect(page.locator('#thesis-metrics-content')).toContainText('12.30');
});
