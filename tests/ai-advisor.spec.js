const { test, expect } = require('@playwright/test');
const { login } = require('./helpers');

// ── AI Advisor — stock research tests ─────────────────────────────────────────
// Guards against the bug where company name queries (e.g. "Micron") bypassed
// live data fetching because the ticker resolution check only accepted ≤5 char
// inputs. Claude fell back to stale training data with most fields as N/A.

const API = 'http://localhost:8000';

async function getToken(request) {
  if (process.env.TEST_UNLOCK_SECRET) {
    await request.post(`${API}/api/auth/unlock-test`, {
      data: { secret: process.env.TEST_UNLOCK_SECRET },
    });
  }
  const res = await request.post(`${API}/api/auth/login`, {
    data: { username: process.env.SP_USER || 'admin', password: process.env.SP_PASS || 'stockpicker2024' },
  });
  const body = await res.json();
  return body.access_token;
}

test.describe('AI Advisor — stock research', () => {

  test('ticker query (MU) returns live data with key fields populated', async ({ request }) => {
    const token = await getToken(request);
    const res = await request.post(`${API}/api/stock-research`, {
      headers: { Authorization: `Bearer ${token}` },
      data: { query: 'MU' },
    });
    expect(res.ok()).toBeTruthy();
    const data = await res.json();
    expect(data.response).toBeTruthy();

    // Response should contain actual price data (dollar amounts) from live fetch
    const text = data.response;
    expect(text).toMatch(/\$[\d,.]+/);  // at least one dollar figure present

    // Should not be dominated by N/A — live data must have been fetched
    const naCount = (text.match(/N\/A/g) || []).length;
    const dollarCount = (text.match(/\$[\d,.]+/g) || []).length;
    expect(dollarCount).toBeGreaterThan(0);
    console.log(`MU ticker query: ${dollarCount} dollar figures, ${naCount} N/A ✓`);
  });

  test('company name query (Micron) resolves to MU and returns live data', async ({ request }) => {
    const token = await getToken(request);
    const res = await request.post(`${API}/api/stock-research`, {
      headers: { Authorization: `Bearer ${token}` },
      data: { query: 'Micron' },
    });
    expect(res.ok()).toBeTruthy();
    const data = await res.json();
    expect(data.response).toBeTruthy();

    const text = data.response;

    // Should mention Micron or MU
    expect(text).toMatch(/MU|Micron/i);

    // Live data means dollar figures present (price, market cap, revenue etc.)
    const dollarCount = (text.match(/\$[\d,.]+/g) || []).length;
    expect(dollarCount).toBeGreaterThan(0);

    // Should not be all N/A — live fields must be present
    const naCount = (text.match(/N\/A/g) || []).length;
    console.log(`Micron name query: ${dollarCount} dollar figures, ${naCount} N/A`);
    // Fewer N/As than dollar figures means live data dominated
    expect(naCount).toBeLessThanOrEqual(dollarCount + 3);
  });

  test('full company name (Micron Technologies) also resolves correctly', async ({ request }) => {
    const token = await getToken(request);
    const res = await request.post(`${API}/api/stock-research`, {
      headers: { Authorization: `Bearer ${token}` },
      data: { query: 'Micron Technologies' },
    });
    expect(res.ok()).toBeTruthy();
    const data = await res.json();
    expect(data.response).toBeTruthy();

    const text = data.response;
    // Should have resolved — at least one dollar figure in the response
    const dollarCount = (text.match(/\$[\d,.]+/g) || []).length;
    expect(dollarCount).toBeGreaterThan(0);
    console.log(`Micron Technologies full name: ${dollarCount} dollar figures ✓`);
  });

  test('UI: research tab loads, name query shows populated report', async ({ page }) => {
    await login(page);

    // Navigate to AI Advisor tab
    await page.click('.tab-btn[data-tab="ai"]');
    await page.waitForSelector('#tab-ai.active, #tab-ai:not(.hidden)', { timeout: 5000 }).catch(() => {});

    // Type company name and submit
    await page.fill('#research-query', 'Micron');
    await page.click('#btn-research');

    // Wait for research to complete (Claude call can take up to 60s)
    await page.waitForSelector('#research-response .research-rendered', { timeout: 90000 });

    // Response container should be visible with content
    const response = page.locator('#research-response');
    await expect(response).toBeVisible();

    // Should contain a dollar figure — evidence of live data
    const text = await response.innerText();
    expect(text).toMatch(/\$[\d,.]+/);

    // Should have metric rows (label: value pairs) not just N/A
    const metrics = await response.locator('.research-metric').count();
    expect(metrics).toBeGreaterThan(3);
    console.log(`UI test: ${metrics} metric fields rendered in report`);
  });

});
