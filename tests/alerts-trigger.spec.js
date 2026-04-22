const { test, expect } = require('@playwright/test');
const { login, USERNAME, PASSWORD } = require('./helpers');

const API = 'http://localhost:8000';

async function getToken(request) {
  const res = await request.post(`${API}/api/auth/login`, {
    data: { username: USERNAME, password: PASSWORD },
  });
  expect(res.ok()).toBeTruthy();
  const body = await res.json();
  expect(body.access_token).toBeTruthy();
  return body.access_token;
}

test.describe('Alert triggers', () => {
  test('API: recommendation alert snapshot returns trigger candidates and channel status', async ({ request }) => {
    const token = await getToken(request);

    const [statusRes, snapshotRes] = await Promise.all([
      request.get(`${API}/api/alerts/status`, {
        headers: { Authorization: `Bearer ${token}` },
      }),
      request.get(`${API}/api/alerts/debug-snapshot`, {
        headers: { Authorization: `Bearer ${token}` },
      }),
    ]);

    expect(statusRes.ok()).toBeTruthy();
    expect(snapshotRes.ok()).toBeTruthy();

    const status = await statusRes.json();
    const snapshot = await snapshotRes.json();

    expect(status).toHaveProperty('strategy');
    expect(snapshot).toHaveProperty('buys');
    expect(snapshot).toHaveProperty('sells');
    expect(Array.isArray(snapshot.buys)).toBeTruthy();
    expect(Array.isArray(snapshot.sells)).toBeTruthy();
    expect(typeof snapshot.buy_count).toBe('number');
    expect(typeof snapshot.sell_count).toBe('number');

    const combined = [...snapshot.buys, ...snapshot.sells];
    expect(combined.length).toBeGreaterThan(0);

    for (const alert of combined.slice(0, 3)) {
      expect(alert.ticker).toBeTruthy();
      expect(alert.action).toMatch(/BUY|SELL/);
      expect(alert.trigger).toBeTruthy();
      expect(typeof alert.score_value).toBe('number');
      expect(alert.confidence).toBeTruthy();
    }

    expect(snapshot.whatsapp_for_recommendation_alerts).toBeTruthy();
    expect(snapshot.whatsapp_note).toMatch(/attempt WhatsApp delivery/i);

    console.log(
      `Alert snapshot: ${snapshot.buy_count} buy trigger(s), ${snapshot.sell_count} sell trigger(s). ` +
      `WhatsApp for recommendation alerts: ${snapshot.whatsapp_for_recommendation_alerts}. ` +
      `Configured SMS channel: ${status.notifications?.sms}.`
    );
  });

  test('UI: alerts page exposes a dedicated WhatsApp test button', async ({ page }) => {
    await login(page);

    await page.route('**/api/alerts/test-whatsapp', async route => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ sms_sent: true, status: 'queued', sid: 'SM123' }),
      });
    });

    await page.click('button[data-tab="alerts"]');
    await page.waitForSelector('#tab-alerts.active', { timeout: 10_000 });

    const button = page.locator('#btn-test-whatsapp');
    await expect(button).toBeVisible();
    await expect(button).toHaveText(/Send Test WhatsApp/i);

    await button.click();
    await expect(page.locator('#alerts-status')).toContainText(/WhatsApp accepted by Twilio/i);
  });
});
