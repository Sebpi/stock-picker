const { test, expect } = require('@playwright/test');
const { login, USERNAME, PASSWORD } = require('./helpers');

const API = 'http://localhost:8000';
let cachedToken = null;

async function getToken(request) {
  if (cachedToken) return cachedToken;
  if (process.env.TEST_UNLOCK_SECRET) {
    await request.post(`${API}/api/auth/unlock-test`, {
      data: { secret: process.env.TEST_UNLOCK_SECRET },
    });
  }
  const res = await request.post(`${API}/api/auth/login`, {
    data: { username: USERNAME, password: PASSWORD },
  });
  expect(res.ok()).toBeTruthy();
  const body = await res.json();
  cachedToken = body.access_token;
  return cachedToken;
}

async function resetPaperPortfolio(request) {
  const token = await getToken(request);
  const res = await request.delete(`${API}/api/paper-portfolio/reset`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  expect(res.ok()).toBeTruthy();
}

async function updateSettings(request, patch) {
  const token = await getToken(request);
  const currentRes = await request.get(`${API}/api/settings`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  expect(currentRes.ok()).toBeTruthy();
  const current = await currentRes.json();
  const next = { ...current, ...patch };
  const res = await request.post(`${API}/api/settings`, {
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    data: next,
  });
  expect(res.ok()).toBeTruthy();
  return res.json();
}

test.describe('Paper trading', () => {
  test.beforeEach(async ({ request }) => {
    await resetPaperPortfolio(request);
    await updateSettings(request, { initial_float: 200000 });
  });

  test('API: paper buy and sell update positions and realised P&L', async ({ request }) => {
    const token = await getToken(request);
    const headers = {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };

    let res = await request.post(`${API}/api/paper-portfolio/buy`, {
      headers,
      data: { ticker: 'AAPL', qty: 2, price: 100 },
    });
    expect(res.ok()).toBeTruthy();

    res = await request.get(`${API}/api/paper-portfolio`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(res.ok()).toBeTruthy();
    let body = await res.json();
    expect(body.positions).toHaveLength(1);
    expect(body.positions[0].ticker).toBe('AAPL');
    expect(body.positions[0].shares).toBe(2);

    res = await request.post(`${API}/api/paper-portfolio/sell`, {
      headers,
      data: { ticker: 'AAPL', qty: 1, price: 110 },
    });
    expect(res.ok()).toBeTruthy();

    res = await request.get(`${API}/api/paper-portfolio`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(res.ok()).toBeTruthy();
    body = await res.json();
    expect(body.positions).toHaveLength(1);
    expect(body.positions[0].ticker).toBe('AAPL');
    expect(body.positions[0].shares).toBe(1);
    expect(body.summary.realised_pnl).toBe(10);
  });

  test('API: editable start capital is reflected in paper portfolio summary', async ({ request }) => {
    await updateSettings(request, { initial_float: 125000 });
    const token = await getToken(request);
    const res = await request.get(`${API}/api/paper-portfolio`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(res.ok()).toBeTruthy();
    const body = await res.json();
    expect(body.summary.initial_float).toBe(125000);
    expect(body.summary.cash).toBe(125000);
  });

  test('UI: paper buy and sell buttons execute trades', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await page.click('button[data-tab="recommendations"]');
    await page.waitForSelector('#tab-recommendations.active', { timeout: 10000 });

    await page.evaluate(() => {
      renderRecommendations({
        buys: [
          {
            ticker: 'AAPL',
            name: 'Apple Inc.',
            confidence: 'high',
            accuracy_pct: 72,
            direction: 'bullish',
            score_value: 81,
            current_price: 100,
            qty: 2,
            estimated_cost: 200,
            reasoning: 'Test buy recommendation.',
            factor_scores: { value: 60, momentum: 75, quality: 80, growth: 78 },
          },
        ],
        sells: [],
        summary: {},
      });
    });

    await expect(page.locator('.btn-paper-buy')).toBeVisible();
    await page.click('.btn-paper-buy');
    await expect(page.locator('#tab-recommendations')).toHaveClass(/active/);
    await page.click('button[data-tab="paper"]');
    await page.waitForSelector('#tab-paper.active', { timeout: 15000 });
    await expect(page.locator('#paper-positions-body tr')).toHaveCount(1);
    await expect(page.locator('#paper-positions-body')).toContainText('AAPL');
    await expect(page.locator('#paper-history-body')).toContainText('BUY');

    await page.evaluate(() => {
      renderRecommendations({
        buys: [],
        sells: [
          {
            ticker: 'AAPL',
            name: 'Apple Inc.',
            trigger: 'TAKE PROFIT',
            qty: 1,
            current_price: 110,
            estimated_proceeds: 110,
            unrealised_pnl: 10,
            unrealised_pct: 10,
            score_value: 35,
            direction: 'bearish',
            reasoning: 'Test sell recommendation.',
            factor_scores: { quality: 80 },
          },
        ],
        summary: {},
      });
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelector('.tab-btn[data-tab="recommendations"]')?.classList.add('active');
      document.getElementById('tab-recommendations')?.classList.add('active');
    });

    await page.click('.btn-paper-sell');
    await expect(page.locator('#tab-recommendations')).toHaveClass(/active/);
    await page.click('button[data-tab="paper"]');
    await page.waitForSelector('#tab-paper.active', { timeout: 15000 });
    await expect(page.locator('#paper-positions-body tr')).toHaveCount(1);
    await expect(page.locator('#paper-positions-body')).toContainText('AAPL');
    await expect(page.locator('#paper-positions-body')).toContainText('1');
    await expect(page.locator('#paper-history-body')).toContainText('SELL');
    await expect(page.locator('#paper-realised')).toContainText('10.00');
  });

  test('UI: live recommendations expose working paper trade buttons', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await page.click('button[data-tab="recommendations"]');
    await page.waitForSelector('#tab-recommendations.active', { timeout: 10000 });

    await page.click('#btn-load-recs');
    await page.waitForFunction(() => {
      const status = document.getElementById('rec-status');
      return !!status && !/Starting recommendations|Elapsed|remaining|Finalizing/i.test(status.textContent || '');
    }, { timeout: 240000 });

    const buyButtons = page.locator('.btn-paper-buy');
    const sellButtons = page.locator('.btn-paper-sell');
    const buyCount = await buyButtons.count();
    const sellCount = await sellButtons.count();

    console.log(`Live recommendation buttons: ${buyCount} buy, ${sellCount} sell`);
    expect(buyCount + sellCount).toBeGreaterThan(0);

    if (buyCount > 0) {
      await buyButtons.first().click();
      await expect(page.locator('#tab-recommendations')).toHaveClass(/active/);
      await page.click('button[data-tab="paper"]');
      await page.waitForSelector('#tab-paper.active', { timeout: 15000 });
      await expect(page.locator('#paper-history-body tr')).toHaveCount(1);
      await expect(page.locator('#paper-history-body')).toContainText('BUY');
      return;
    }

    await sellButtons.first().click();
    await expect(page.locator('#tab-recommendations')).toHaveClass(/active/);
    await page.click('button[data-tab="paper"]');
    await page.waitForSelector('#tab-paper.active', { timeout: 15000 });
    await expect(page.locator('#paper-history-body tr')).toHaveCount(1);
    await expect(page.locator('#paper-history-body')).toContainText('SELL');
  });

  test('UI: recommendations tab stays focused after paper trade', async ({ page }) => {
    await login(page);
    await page.goto('/');
    await page.click('button[data-tab="recommendations"]');
    await page.waitForSelector('#tab-recommendations.active', { timeout: 10000 });

    await page.evaluate(() => {
      renderRecommendations({
        buys: [
          {
            ticker: 'MSFT',
            name: 'Microsoft Corp.',
            confidence: 'high',
            accuracy_pct: 68,
            direction: 'bullish',
            score_value: 79,
            current_price: 100,
            qty: 1,
            estimated_cost: 100,
            reasoning: 'Focus retention buy test.',
            factor_scores: { value: 55, momentum: 74, quality: 84, growth: 80 },
          },
        ],
        sells: [],
        summary: {},
      });
    });

    await expect(page.locator('.btn-paper-buy')).toBeVisible();
    await page.click('.btn-paper-buy');
    await expect(page.locator('button[data-tab="recommendations"]')).toHaveClass(/active/);
    await expect(page.locator('#tab-recommendations')).toHaveClass(/active/);
    await expect(page.locator('button[data-tab="paper"]')).not.toHaveClass(/active/);

    await page.evaluate(() => {
      renderRecommendations({
        buys: [],
        sells: [
          {
            ticker: 'MSFT',
            name: 'Microsoft Corp.',
            trigger: 'TAKE PROFIT',
            qty: 1,
            current_price: 110,
            estimated_proceeds: 110,
            unrealised_pnl: 10,
            unrealised_pct: 10,
            score_value: 36,
            direction: 'bearish',
            reasoning: 'Focus retention sell test.',
            factor_scores: { quality: 84 },
          },
        ],
        summary: {},
      });
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelector('.tab-btn[data-tab="recommendations"]')?.classList.add('active');
      document.getElementById('tab-recommendations')?.classList.add('active');
    });

    await expect(page.locator('.btn-paper-sell')).toBeVisible();
    await page.click('.btn-paper-sell');
    await expect(page.locator('button[data-tab="recommendations"]')).toHaveClass(/active/);
    await expect(page.locator('#tab-recommendations')).toHaveClass(/active/);
    await expect(page.locator('button[data-tab="paper"]')).not.toHaveClass(/active/);
  });
});
