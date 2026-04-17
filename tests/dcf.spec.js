const { test, expect } = require('@playwright/test');
const { login } = require('./helpers');

// ── DCF Margin of Safety sanity tests ─────────────────────────────────────────
// These guard against the bug where distorted FCF figures (e.g. BAX, CNC) produced
// nonsensical MoS values like 491% or 882% due to negative-EPS companies with
// inflated reported free cash flow.

const API = 'http://localhost:8000';

async function getToken(request) {
  // Clear lockout if TEST_UNLOCK_SECRET is set (dev environments)
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

test.describe('DCF Margin of Safety guardrails', () => {

  test('DCF returns null for loss-making stock with inflated FCF (CNC)', async ({ request }) => {
    const token = await getToken(request);
    // Trigger a fresh stock detail fetch which runs compute_dcf_valuation
    const res = await request.get(`${API}/api/stock/CNC`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(res.ok()).toBeTruthy();
    const data = await res.json();
    const mos = data?.dcf?.margin_of_safety_pct ?? null;

    // CNC: EPS -$13, FCF/share > 3x price → should be suppressed (null)
    expect(mos).toBeNull();
    console.log(`CNC MoS: ${mos} (correctly suppressed)`);
  });

  test('DCF returns null for BAX (loss-making, FCF/share > 3x price)', async ({ request }) => {
    const token = await getToken(request);
    const res = await request.get(`${API}/api/stock/BAX`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(res.ok()).toBeTruthy();
    const data = await res.json();
    const mos = data?.dcf?.margin_of_safety_pct ?? null;
    expect(mos).toBeNull();
    console.log(`BAX MoS: ${mos} (correctly suppressed)`);
  });

  test('DCF MoS is within ±150% for healthy large-cap (AAPL)', async ({ request }) => {
    const token = await getToken(request);
    const res = await request.get(`${API}/api/stock/AAPL`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(res.ok()).toBeTruthy();
    const data = await res.json();
    const mos = data?.dcf?.margin_of_safety_pct ?? null;

    if (mos !== null) {
      expect(Math.abs(mos)).toBeLessThanOrEqual(150);
      console.log(`AAPL MoS: ${mos}% (within bounds)`);
    } else {
      // Acceptable — AAPL may not have all fields yfinance needs
      console.log('AAPL DCF returned null (acceptable)');
    }
  });

  test('no prediction in stored file has MoS outside ±150%', async ({ request }) => {
    const token = await getToken(request);
    const res = await request.get(`${API}/api/predictions`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(res.ok()).toBeTruthy();
    const body = await res.json();
    const predictions = body.predictions || body;

    const outliers = predictions.filter(p => {
      const mos = p?.dcf?.margin_of_safety_pct;
      return mos !== null && mos !== undefined && Math.abs(mos) > 150;
    });

    if (outliers.length > 0) {
      console.log('Outlier DCF predictions:');
      outliers.forEach(p => console.log(`  ${p.ticker} ${p.date}: MoS ${p.dcf.margin_of_safety_pct}%`));
    }

    expect(outliers).toHaveLength(0);
  });

});
