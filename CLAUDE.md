# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> Lives at `~/code/stock-picker`. Own git repo, remote `Sebpi/stock-picker` on GitHub. Deploys to the Fly app `stock-picker-sp`. Current version: **v3.9.0** (defined in `package.json`).

## Architecture

StockPicker is an AI-driven equity research / portfolio / alerts tool. Single-process FastAPI backend that also serves a hand-written React SPA loaded via CDN UMD — there is **no frontend build step**.

### Backend — `backend/main.py` (single ~9k-line FastAPI app)
- Python 3.12, FastAPI + uvicorn, JWT auth via `python-jose`, bcrypt password hashing, slowapi rate limiting, APScheduler for background jobs, Anthropic SDK for LLM calls, `yfinance`/`yahooquery` for market data, Twilio + SMTP for alerts, `reportlab`/`pypdf` for PDFs, `pyotp` for TOTP MFA, `stripe` for billing webhooks.
- Storage is **dual-format on purpose**:
  - SQLite at `$DATA_DIR/stockpicker.db` — multi-agent signals, thesis history, decision log, agent accuracy, calibrated weights, all multi-user tables (see `backend/db.py`). Also contains an `insider_transactions` table managed by `backend/insider_transactions.py` (CREATE TABLE IF NOT EXISTS, no migration script needed).
  - JSON files under `$DATA_DIR` are now **legacy / admin fallbacks only**: `users.json`, `watchlist.json`, `predictions.json`, `portfolio.json`, `paper_portfolio.json`, `alerts.json`, `settings.json`, `lockout_state.json`, `sentiment_agent_state.json`. All user-facing API endpoints read from SQLite via the per-user helpers (see Per-user data isolation below) — the JSON files are only touched when no app_users row is found (i.e., unauthenticated legacy admin path).
  - `db.py` auto-migrates a legacy `backend/stockpicker.db` into `$DATA_DIR` on first boot if the latter doesn't exist. JSON writes use a temp-file + rename atomic pattern (`_atomic_write`).
- **`SECRET_KEY`** drives JWT signing. If unset, the app generates an ephemeral one and **all sessions invalidate on restart** — set it in `backend/.env` for any non-toy run.
- Auth surface: bcrypt + JWT (HS256), per-account lockout (5 attempts → 15-min cool-off, persisted to `lockout_state.json`), password reset via email token, default `admin` user auto-created on first run with a **random password printed to stdout once**.
- **Multi-user auth** (v3.9.0): `app_users` SQLite table with UUID PK, bcrypt hash, role, tier (free/pro/premium), email_verified, TOTP MFA. Short-lived access tokens (`_ACCESS_TOKEN_MINUTES=15`) + 30-day rotating refresh tokens. Existing `users.json` admin login unchanged — new system is additive.
- **Portal JWT integration (LENS shared-session):** `get_current_user` and `auth_middleware` both accept JWTs minted by `seb-portal /api/auth/jwt` when `PORTAL_JWT_SECRET` is set. Only tokens with `iss="seb-portal"` pass through; portal-routed identity is returned as `"portal:<sub>"` so audit logs can distinguish it from local logins. Portal users don't need a `users.json` entry.
- Three middleware stacks on top of FastAPI: trusted-host (`ALLOWED_HOSTS`), CORS (`ALLOWED_ORIGINS`), and an HTTPS-redirect / origin-check pair gated by `REQUIRE_HTTPS`.
- Ticker validation is centralised: `_validate_ticker()` enforces `^[A-Z0-9.\-]{1,10}$` — call this on every user-supplied ticker. `load_watchlist()` also validates tickers on read and logs a warning for any it skips.
- A startup hook clears any `*_PROXY` env var pointing at a dead local port (port 9), to survive macOS leftover proxy envs.

### Multi-agent forecasting — `backend/agents/`
- **Twenty-one agents** (`analyst_consensus`, `capital_allocation`, `credit_risk`, `dividend_quality`, `earnings_quality`, `earnings_surprise`, `financial_distress`, `fundamentals`, `growth_revisions`, `industry_competition`, `insider_activity`, `institutional_flow`, `macro_liquidity`, `options_flow`, `piotroski`, `portfolio_risk`, `price_momentum`, `sentiment_news`, `short_interest`, `technical_risk`, `valuation`, plus the `orchestrator`). All agents emit `AgentSignal` (defined in `schemas.py`); the orchestrator aggregates them into an `InvestmentThesis` with bull/base/bear `HorizonForecast`s and a Claude-generated narrative.
- **`insider_activity`** (`backend/agents/insider_activity.py`) reads the 60-day Form 4 summary from `insider_transactions.summarize_ticker` (refreshing from SEC EDGAR if the cache is cold). Scoring ladder: 88 cluster-buying → 32 heavy multi-insider selling. Weighted heaviest on the 3-month horizon (0.10) where Form 4 cluster signals are strongest; 0.04 at 12m.
- **`options_flow`** (`backend/agents/options_flow.py`) reads the live yfinance options chain for the nearest 1–3 expiries. Scores on put/call ratio (PCR), IV skew (near-ATM put IV minus call IV), and volume direction. PCR < 0.50 → 85 (heavy call bias); PCR > 1.55 → 26 (heavy put buying). Weighted 0.09 at 3m, 0.04 at 6m, 0.02 at 12m. LOW_COVERAGE for stocks with no options or < 100 total chain volume.
- **`earnings_quality`** (`backend/agents/earnings_quality.py`) computes Beneish M-score (8-factor manipulation detector; M > −1.78 = elevated risk) and Sloan accruals ratio ((NI − CFO) / avg assets; > 0.10 = expect earnings reversion) from annual financial statements. No new data source — uses same yfinance financials/balance_sheet/cashflow already fetched. Weighted 0.04 at 3m, 0.09 at 6m, 0.10 at 12m (accruals revert over 6–18 months).
- **`short_interest`** (`backend/agents/short_interest.py`) reads FINRA short interest via yfinance (`shortPercentOfFloat`, `shortRatio`, `sharesShort`, `sharesShortPriorMonth`). Scores on short % of float (low = bullish) and MoM change direction (falling = covering = bullish). Squeeze risk bonus when short % ≥ 10% + days-to-cover ≥ 8 + MoM falling. Weighted 0.06 at 3m, 0.05 at 6m, 0.03 at 12m.
- **`credit_risk`** (`backend/agents/credit_risk.py`) combines macro credit spread environment with company leverage profile. Uses FRED OAS spreads (BAMLH0A0HYM2 HY, BAMLC0A0CM IG) when `FRED_API_KEY` is set, otherwise falls back to HYG/LQD ETF 30-day momentum. Leverage adjustment: D/E < 0.5 → +5; D/E > 3.0 or interest coverage < 3× → −15; negative FCF + high leverage → additional −5. Weighted 0.07 at 3m, 0.05 at 6m, 0.04 at 12m. Credit spreads lead equity stress by 2–8 weeks.
- **`institutional_flow`** (`backend/agents/institutional_flow.py`) tracks institutional ownership % from 13F-derived yfinance data (`institutionsPercentHeld`, `insidersPercentHeld`, `institutionsCount`, `institutional_holders`). Base score: 40–70% ownership → 65 (healthy); >85% → 38 (crowded). Insider ownership >20% → +8; <1% → −5. Weighted 0.06 at 3m, 0.07 at 6m, 0.06 at 12m.
- **`earnings_surprise`** (`backend/agents/earnings_surprise.py`) scores on trailing 4-quarter EPS beat rate (4/4 → 72; 0/4 → 30), average surprise magnitude, and net estimate revisions in the last 7 days (from `eps_revisions`). Exploits analyst anchoring bias — companies that beat consistently keep doing so. Weighted 0.07 at 3m, 0.06 at 6m, 0.04 at 12m.
- **`price_momentum`** (`backend/agents/price_momentum.py`) computes the classic 12-1 month momentum factor (Jegadeesh-Titman 1993): total return from 12 months ago to 1 month ago (skipping last month to avoid reversal). Also scores relative momentum vs SPY and 3-month trend confirmation. Weighted 0.09 at 3m (peak momentum horizon), 0.06 at 6m, 0.02 at 12m (decays quickly).
- **`dividend_quality`** (`backend/agents/dividend_quality.py`) evaluates dividend yield tier, payout sustainability (payoutRatio), FCF coverage of dividends, and buyback yield (net share count reduction). Total shareholder yield = dividend + buyback. High yield + payout > 90% → danger zone. Weighted 0.03 at 3m, 0.05 at 6m, 0.07 at 12m (long-duration quality signal).
- **`capital_allocation`** (`backend/agents/capital_allocation.py`) scores ROE (compounding quality), FCF conversion (FCF/NI > 1.5 → +7), goodwill growth (rising intangibles → acquisition risk penalty), and asset turnover trend. Weighted 0.02 at 3m, 0.05 at 6m, 0.07 at 12m (most impactful long-term).
- **`analyst_consensus`** (`backend/agents/analyst_consensus.py`) aggregates sell-side recommendation key (`recommendationKey`), target price upside vs current price, target spread (conviction), and analyst count. Strong_buy → 75 base; strong_sell → 24. Weighted 0.07 at 3m, 0.06 at 6m, 0.04 at 12m.
- **`financial_distress`** (`backend/agents/financial_distress.py`) computes the sector-agnostic Altman Z''-Score (6.56·X1 + 3.26·X2 + 6.72·X3 + 1.05·X4) from balance sheet and income statement. Z'' > 2.60 = safe; < 1.10 = distress. Weighted 0.04 at 3m, 0.05 at 6m, 0.06 at 12m.
- **`piotroski`** (`backend/agents/piotroski.py`) computes the 9-factor Piotroski F-Score across profitability (ROA, CFO, accruals), leverage/liquidity (debt trend, current ratio, dilution), and efficiency (gross margin, asset turns). F ≥ 8 → 75; F ≤ 1 → 28. Weighted 0.03 at 3m, 0.06 at 6m, 0.08 at 12m.
- **Minimum 3 agents** must return successfully or the thesis is rejected (`MIN_AGENTS_REQUIRED`). Alert snapshot requires **≥5 of 11 agents** net-positive (`alert_min_positive_agents`). Score → 12-month return uses linear interpolation over the `SCORE_TO_12M_RETURN` anchor table.
- `HORIZON_WEIGHTS` in `schemas.py` defines per-agent weights per horizon. The orchestrator's `_weighted_score` normalises by sum of usable weights, so ratios matter, not totals. Weights can drift from their factory defaults over time as the learning system recalibrates them (see Learning system below) — current live weights are always in memory and persisted to the `calibrated_weights` SQLite table.
- Two distinct models in play:
  - `ANTHROPIC_MODEL` (default `claude-haiku-4-5-20251001`) for routine predictions/screens.
  - `THESIS_MODEL` (default `claude-sonnet-4-6`) for the thesis narrative — higher quality, higher cost.
- `PREDICTIONS_MAX_TOKENS` (default 8192) must be large enough for **all watchlist stocks in one response** — roughly 100 tokens per stock. Bump it before adding many tickers.

### Learning system — `backend/agent_accuracy.py` + `backend/evaluation.py`
The learning system measures how well each agent's directional calls translated into real returns, then automatically adjusts `HORIZON_WEIGHTS` over time.

- **Forecast outcomes** — `db.store_thesis()` calls `record_forecast_outcome()`, which inserts one pending row per horizon into `forecast_outcome` for every new thesis. Horizons tracked: `1m` (30-day fast-track — uses 3m base_return_pct as direction proxy), `3m` (91d), `6m` (182d), `12m` (365d). The 1m row exists specifically to get the first accuracy signal ~30 days post-thesis rather than waiting 91 days.
- **Evaluation** — `backend/evaluation.py` resolves matured rows: fetches the real price change via yfinance, computes `realised_return_pct`, `direction_match`, and benchmark-relative return. `HORIZON_DAYS = {"1m": 30, "3m": 91, "6m": 182, "12m": 365}`. Runs as a daily APScheduler job.
- **Per-agent accuracy** (`agent_accuracy.py`) — `rebuild_all()` joins `forecast_outcome → investment_thesis → agent_signal` for 30/60/90-day windows, computing directional hit rate, Spearman score-return correlation, and a bounded `suggested_weight_adj` per (agent, horizon) pair. Results written to the `agent_accuracy` SQLite table. Baseline hit rate = 55%; agents above 60% are "strong", below 50% are "weak".
- **Phase 2 recalibration** — `apply_weight_adjustments()` reads `suggested_weight_adj` and applies a blended update: `new = current × (1 + 0.20 × adj)`. Hard-capped at [50%, 200%] of the factory default per weight. Persisted to the `calibrated_weights` table. Runs as an APScheduler cron job every **Monday 02:00 UTC** (`auto_recalibrate_weights`). `load_calibrated_weights()` is called at startup after `init_db()` to restore the latest calibration.
- **Score buckets** — `_compute_score_buckets()` tracks what composite score ranges (0–50, 50–60, …, 90–100) actually returned per horizon. Written to `score_bucket` table. Used by the Learning tab to show calibration gaps.
- **API endpoints** (all require auth):
  - `GET /v1/learning/summary?window_days=90` — full learning summary (hit rates, strong/weak agents, score buckets)
  - `POST /v1/learning/rebuild` — recompute accuracy stats from raw data
  - `GET /v1/learning/weights` — current vs default weights with delta %
  - `POST /v1/learning/weights/recalibrate` — apply suggested adjustments immediately
  - `POST /v1/learning/weights/reset` — restore factory defaults and clear calibration history
- **Factory defaults** are snapshotted at import time in `_DEFAULT_WEIGHTS = copy.deepcopy(HORIZON_WEIGHTS)` so reset is always available.

### SEC EDGAR insider transactions — `backend/insider_transactions.py`
- Fetches Form 4 filings via SEC EDGAR for any ticker. Rate-limited to 0.12s between requests (under SEC's 10 req/s cap). Requires `EDGAR_USER_AGENT` env var (defaults to a courtesy string if unset).
- Key functions: `refresh_ticker` (walk last 40 filings, idempotent INSERT OR IGNORE), `summarize_ticker` (30/60-day rollup: net $ flow, purchase/sale counts, unique-insider count, `cluster_buying` flag ≥3 insiders making P transactions), `list_transactions`.
- CLI: `python3 -m insider_transactions --tickers NVDA MSFT --refresh`
- Endpoint: `GET /api/insider/{ticker}?days=30&refresh=false` — rate-limited 60/hour. `refresh=true` walks EDGAR live (10–20s first time, then SQLite cache).
- **XSL path gotcha:** SEC's `submissions.json` returns `primary_doc` as `xslF345X06/filename.xml` (the rendered HTML view). The fetcher strips any leading `xsl*/` segment to reach the raw XML. Don't break this if touching `_fetch_form4_xml`.

### Recommendation engine layers (Phases 1–5)
- **Phase 1** — BUY/SELL collision fix: BUY floor raised to score ≥ 70, post-sell cooldown 21d, vol-scaled stop (`max(−15%, min(−8%, −2.5×daily_vol))`), THESIS FLIPPED exit (composite < 55), TRAIL STOP (8% off 30-day peak). Alert snapshot skips BUY when ticker is held in either real *or* paper portfolio.
- **Phase 2** — Market-regime gate (`get_market_regime`): blocks BUYs when SPY < N-day DMA (default 200) **or** VIX > threshold (default 25). Fails **open** on data outage (quality filter, not safety lock). Configurable: `regime_gate_enabled`, `regime_spy_dma_period`, `regime_vix_max`.
- **Phase 3** — Portfolio-relative sizing with hard caps: base weight ≈ 1/`N_target` × conviction × confidence × vol_adj. Hard caps applied in order: per-position 12%, per-sector 30%, 95% 1-month VaR 8%, then cash cap. VaR estimated parametrically (uncorrelated). Configurable: `target_positions_count` (default 10), `position_max_pct`, `sector_max_pct`, `portfolio_var_max_pct`.
- **Phase 5** — Trajectory math: `_p_hit_target()` and `_expected_return_from_score()` drive a "Probability of hitting portfolio target" trajectory block in the `/api/recommendations` response. Cross-engine consistency badge per BUY (agree / contradiction / no_thesis / stale >24h) uses the cached alert snapshot.
- **Confidence calibration** — `compute_confidence_calibration` adds a `buckets` key to `/api/predictions/calibration`: per-confidence hit rate vs `CONFIDENCE_PRIOR_HIT_RATE` ({high: 0.75, medium: 0.62, low: 0.55}), MAE, and ECE (expected calibration error — lower is better, <0.05 ≈ well-calibrated). Rendered as `ConfidenceCalibrationTile` on the Predictions tab.

### Multi-user system — `backend/db.py` + `backend/main.py`
Added in v3.9.0. Three-layer security: Auth (JWT+MFA) → Service (user_id scoping) → Data (row-level SQLite).

- **SQLite tables:** `app_users`, `email_verification_tokens`, `refresh_tokens`, `user_watchlist`, `user_settings`, `apns_device_tokens`, **`user_data_store`** (generic JSON blob store — holds portfolio, paper_portfolio, predictions, alerts per user).
- **`user_data_store` schema:** `(user_id TEXT, data_key TEXT, data_json TEXT, updated_at TEXT, PRIMARY KEY(user_id, data_key), FK → app_users CASCADE)`. Keys in use: `"portfolio"`, `"paper_portfolio"`, `"predictions"`, `"alerts"`.
- **Tier limits:** free = 10 watchlist tickers / 5 thesis per month; pro = 50 / unlimited; premium = 999 / unlimited. `check_and_increment_thesis_count()` enforces per-user monthly quota.
- **Auth endpoints** (all public — no JWT required):
  - `POST /api/auth/register` — create account, send verification email
  - `GET /api/auth/verify-email?token=` — consume single-use token, set `email_verified=1`
  - `POST /api/auth/refresh` — rotate 30-day refresh token, issue 15-min access token
- **Auth endpoints** (require JWT):
  - `POST /api/auth/logout-all` — revoke all refresh tokens for current user
  - `POST /api/auth/mfa/setup` — generate TOTP secret + provisioning URI (for QR scan)
  - `POST /api/auth/mfa/verify` — confirm 6-digit code and enable MFA
  - `POST /api/auth/mfa/disable` — disable MFA with confirmation code
- **User data endpoints** (all under `/v1/users/me/`, require JWT):
  - `GET/POST/DELETE /v1/users/me/watchlist[/{ticker}]` — per-user watchlist, tier-gated
  - `GET/POST/DELETE /v1/users/me/portfolio[/{ticker}]` — per-user portfolio (real + paper)
  - `GET/PUT /v1/users/me/settings[/{key}]` — per-user key-value settings
  - `POST/DELETE /v1/users/me/device-tokens[/{token}]` — APNs device tokens (pro/premium only)
- **Admin endpoints** (require admin role):
  - `GET /v1/users` — list all registered users
  - `PUT /v1/users/{user_id}/tier` — change a user's tier (free/pro/premium)
- **Stripe webhook:** `POST /api/billing/stripe/webhook` — handles `subscription.created/updated/deleted`, updates user tier.
- **APNs push:** `_send_apns_push()` via HTTP/2 + ES256 JWT; `_broadcast_earnings_push()` used by earnings scheduler. Requires `APNS_KEY_ID`, `APNS_TEAM_ID`, `APNS_BUNDLE_ID`, `APNS_KEY_PATH` env vars.
- **Tests:** `backend/tests/test_multiuser.py` — 22 tests covering all DB helpers, using per-test in-memory SQLite via `monkeypatch`.

### Per-user data isolation — `backend/main.py` per-user helpers
Every user-facing API endpoint is fully isolated per authenticated user. No data bleeds between accounts.

- **Core resolver:** `_get_app_user_id(username) → str | None` — looks up `app_users.user_id` for a logged-in username. If no row found (legacy path), helpers fall back to global JSON files.
- **Per-user helpers in `main.py`:**
  - `_get_user_watchlist(username)` / `_add_user_watchlist_ticker` / `_remove_user_watchlist_ticker` — use `user_watchlist` SQLite table.
  - `_load_user_portfolio(username)` / `_save_user_portfolio(username, txns)` — `user_data_store` key `"portfolio"`.
  - `_load_user_paper_portfolio` / `_save_user_paper_portfolio` — key `"paper_portfolio"`.
  - `_load_user_predictions` / `_save_user_predictions` — key `"predictions"`.
  - `_load_user_alerts` / `_save_user_alerts` — key `"alerts"`.
  - `_load_user_settings` / `_save_user_settings` — `user_settings` table (merges global defaults).
- **Isolated endpoints** (all require JWT, all scoped to `current_user`):
  - Watchlist: `GET/POST/DELETE /api/watchlist[/{ticker}]`
  - Portfolio: `GET /api/portfolio`, `POST /api/portfolio/buy|sell`, `GET /api/portfolio/transactions`, `DELETE /api/portfolio/transaction/{id}`, `POST /api/portfolio/import|import-pdf`, `DELETE /api/portfolio/reset`
  - Paper portfolio: `GET /api/paper-portfolio`, `POST /api/paper-portfolio/buy|sell`, `DELETE /api/paper-portfolio/reset`
  - Predictions: `GET /api/predictions`, `POST /api/predictions/generate`, `DELETE /api/predictions`
  - Alerts: `GET /api/alerts`, `DELETE /api/alerts`, `GET /api/alerts/status`
  - Settings: `GET /api/settings`, `POST /api/settings`
- **Predictions cache** (`_predictions_cache`) is a `dict[str, dict]` keyed by username — each user has an independent in-memory cache entry.
- **`_generate_predictions_impl(username)`** accepts a username and uses per-user load/save/watchlist throughout — each user generates predictions for their own watchlist and those predictions are stored in their own `user_data_store` row.
- **Admin seeding on startup:** `ensure_admin_app_user()` (in `db.py`) creates an `app_users` row for the legacy `admin` if one doesn't exist yet. `_init_multiagent_db()` then seeds the admin's `user_data_store` and `user_watchlist` from the global JSON files exactly once (idempotent — skips if data already exists). This means existing admin data is preserved on upgrade.

### Frontend — `frontend/`
- `index.html` + `react-app.js` (~4400 lines) is the entire active app. The SPA uses **React 18 UMD from unpkg** and a **pre-built local Tailwind CSS** (`frontend/tailwind.css`) — no bundler, but Tailwind must be rebuilt when new utility classes are added.
- `frontend/react-app.js` contains a `FLAG_TOOLTIPS` constant covering all `QualityFlag` enum values. Quality flags are rendered as amber pills in the thesis detail view when present.
- **Login/Register flow** (v3.9.0): The `Login` component supports four modes: `login`, `register`, `forgot`, `reset`. Auto-verifies email on page load when `?verify_token=` is in the URL. Registration calls `POST /api/auth/register` and requires a 12-char minimum password.
- **Account tab** (v3.9.0): Shows profile info (username, email, tier, email verification, MFA status), TOTP MFA setup/disable, and a "Sign out all devices" button. When signed in as `admin`, also shows a users table with all registered accounts and an inline tier dropdown.
- **Rebuilding Tailwind CSS:** run `./node_modules/.bin/tailwindcss -i tailwind.input.css -o frontend/tailwind.css --minify` from the repo root whenever you add new Tailwind classes to `react-app.js` or `index.html`. After regenerating: recompute the SRI hash (`python3 -c "import hashlib,base64,sys; d=open('frontend/tailwind.css','rb').read(); print('sha256-'+base64.b64encode(hashlib.sha256(d).digest()).decode())"`) and update the `integrity=` attribute in `index.html`. The config lives in `tailwind.config.js` at the repo root.
- The `?v=` cache-buster and page title in `index.html` use the `__APP_VERSION__` placeholder, which the server injects from `package.json` at runtime. **Version bumps only require updating `package.json`** — no hand-editing of `index.html` needed.
- `fetch` uses `API = ""` (relative paths), so the SPA must be served from the same origin as the FastAPI backend in production.
- The `/legacy` route (previously serving old UI files) now **301-redirects to `/`**. The files `frontend/app.js`, `frontend/legacy-app.js`, and `frontend/legacy.html` have been deleted.

### Deploy / runtime
- `Dockerfile` is single-stage Python 3.12; no frontend build runs. `docker-entrypoint.sh` symlinks every JSON state file and the SQLite DB from `backend/` into `$DATA_DIR` (the Fly volume), copying seed values once if the volume is empty. Don't open these files via the symlink target during local dev if the volume isn't mounted — keep them where they are in `backend/` for local runs.
- `fly.toml` mounts `stock_picker_data` at `/app/data`, sets `REQUIRE_HTTPS=true`, runs always-on (`auto_stop_machines = "off"`).

## Commands

```bash
# Install
cd backend && pip install -r requirements.txt
cd .. && npm install && npx playwright install chromium

# Rebuild Tailwind CSS (run after adding new Tailwind classes to react-app.js / index.html)
./node_modules/.bin/tailwindcss -i tailwind.input.css -o frontend/tailwind.css --minify
# Then update the integrity= hash in index.html — see CLAUDE.md "Things to be careful with"

# Run dev (single process — serves SPA + API on the same port)
cd backend && uvicorn main:app --host 0.0.0.0 --port 8000 --reload
# → http://localhost:8000  (admin password printed on first run)

# Playwright (root tests/) — config starts a static FE server on :4321 via tests/server.js
npx playwright test
npx playwright test tests/screener.spec.js
npx playwright test -g "sector filter"
npx playwright show-report test-results-run
# Override admin creds: SP_USER=... SP_PASS=... npx playwright test

# Backend pytest suite (multi-agent unit tests)
cd backend && python -m pytest tests/

# Deploy
flyctl deploy -a stock-picker-sp
```

Required `backend/.env` keys (template at `backend/.env.example`): `SECRET_KEY` (32+ chars; `python -c "import secrets; print(secrets.token_hex(32))"`), `ANTHROPIC_API_KEY`. Optional: `ANTHROPIC_MODEL`, `THESIS_MODEL`, `PREDICTIONS_MAX_TOKENS`, `ALLOWED_HOSTS`, `ALLOWED_ORIGINS`, `REQUIRE_HTTPS`, `APP_URL`, `ALLOW_NGROK_ORIGINS`, `DATA_DIR`, Twilio (`TWILIO_*`) for WhatsApp, SMTP (`SMTP_*`) for email, `PORTAL_JWT_SECRET` (shared with `seb-portal` for LENS cross-app auth), `EDGAR_USER_AGENT` (SEC EDGAR courtesy header for Form 4 fetches). Multi-user (v3.9.0): `ACCESS_TOKEN_MINUTES` (default 15), `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `APNS_KEY_ID`, `APNS_TEAM_ID`, `APNS_BUNDLE_ID`, `APNS_KEY_PATH` (path to .p8 file). Stripe and APNs are optional — the app degrades gracefully without them.

## Things to be careful with

- **Playwright test harness.** `tests/server.js` is a proxy server — it serves the frontend static files on `:4321` AND proxies `/api/*` and `/v1/*` to the FastAPI backend on `:8000`. Start the backend first (`uvicorn main:app --port 8000`), then `npx playwright test`. The harness won't work without a running backend. React DOM IDs are baked into `react-app.js` and `index.html` for test selectors — don't rename them without updating the tests.
- **`ALLOWED_IDEA_KEYS`-style allowlist doesn't apply here**, but the equivalent gotcha is `_TICKER_RE` — anything user-supplied that bypasses `_validate_ticker()` is a bug.
- **Don't reorder the `<link rel="stylesheet">` for `tailwind.css`** in `index.html` — it must load before React mounts or components render unstyled.
- **CSP inline-script hash must stay in sync.** `backend/main.py` `security_headers` middleware uses a `sha256-` hash for the one remaining inline `<script>` block in `index.html` (the theme-detection snippet). If you change its content — even whitespace — recompute the hash: `python3 -c "import hashlib,base64,re; s=re.findall(r'<script(?![^>]*\bsrc\b)[^>]*>(.*?)</script>',open('frontend/index.html').read(),re.DOTALL); print('sha256-'+base64.b64encode(hashlib.sha256(s[0].encode()).digest()).decode())"`.
- **CSP inline-style hash must also stay in sync.** The `<style>` block in `index.html` defines CSS custom properties for dark/light theming. `style-src` includes its hash (`sha256-JY2CPnAQYQylZcG0tTBIGoRtNV0dsTWGI3U7cL/c9Rc=`). If you change the style block, recompute: `python3 -c "import hashlib,base64,re; s=re.findall(r'<style>(.*?)</style>',open('frontend/index.html').read(),re.DOTALL); print('sha256-'+base64.b64encode(hashlib.sha256(s[0].encode()).digest()).decode())"` and update `style-src` in `security_headers`.
- **Always check recent git commits** before making changes (`git log --oneline -15`) to avoid reverting work already merged.
- **PR workflow:** squash-merge every PR, then delete the branch. GitHub auto-deletes on squash merge if the repo setting is on; otherwise `git push origin --delete <branch>`. Always rebase the working branch onto `origin/main` before opening a PR to avoid dirty merge state.
- **State files vs symlinks.** In production, `backend/users.json` etc. are symlinks into `/app/data`. Editing them locally via the symlink path will silently write into your local `data/` directory once `DATA_DIR` is set — be deliberate about which copy you're touching when reproducing prod bugs.
- **Version bumps:** update `package.json` only — the version is injected at runtime into the page title and asset URLs via `__APP_VERSION__`. Run the version metadata tests (`python -m pytest tests/test_version_metadata.py`) to verify.
- **Two LLM models, two cost profiles.** Generating predictions for a large watchlist on the Sonnet thesis model is significantly more expensive than the Haiku default — keep them separate.
- **Two recommendation engines with different sources of truth.** `_build_recommendations` (Signals tab) reads the user's paper portfolio and prediction scores; `_build_recommendation_alert_snapshot` (Alerts) reads the user's portfolio + watchlist and uses the multi-agent thesis. Both now use the per-user helpers (`_load_user_portfolio`, `_load_user_paper_portfolio`, `_get_user_watchlist`) so each user sees only their own data. Phase 1 unifies the "already held" check across both by including paper holdings in the alert snapshot's skip list; Phase 2 wires the market-regime gate into both. Alert snapshot requires ≥5 positive agents. Keep these two engines in sync if you add new BUY filters to one.
- **Learning system weight mutations are live.** `HORIZON_WEIGHTS` is mutated in-place by `apply_weight_adjustments()`. Changes take effect on the next thesis run without restart. If you're debugging unexpected scores, check `GET /v1/learning/weights` to see if weights have drifted from factory defaults. Use `POST /v1/learning/weights/reset` to restore defaults.
- **1m forecast rows are direction proxies only.** The 1m `forecast_outcome` row created by `store_thesis()` uses the 3m `base_return_pct` as the direction proxy (not a real 1m forecast). It exists solely to feed accuracy data back to the learning system 30 days sooner. Don't treat `forecast_return_pct` on 1m rows as a genuine 1-month price target.
- **Multi-user auth is additive, not a replacement.** The existing `users.json` admin login still works and is the only way to access admin role features (including the users list). New `app_users` registrations are separate — they don't get an entry in `users.json` and cannot access admin endpoints unless their `role` column is manually set to `"admin"` in the DB.
- **Refresh token rotation is single-use.** Each `POST /api/auth/refresh` invalidates the old token and issues a new one. If two tabs race, one will get a 401 — that's by design (token theft detection). The frontend doesn't yet handle silent refresh, so 15-minute access tokens will cause visible logouts without a refresh implementation.
- **MFA secrets are stored in plaintext** in `app_users.mfa_secret`. If the DB is compromised, TOTP seeds are exposed. For higher security: encrypt at rest using `SECRET_KEY` as the key before storing.
- **Per-user data isolation: always use the `_load/save_user_*` helpers, never `load/save_portfolio()` etc. directly in user-facing endpoints.** The global `load_portfolio()` / `save_portfolio()` / `load_predictions()` / `save_predictions()` / `load_alerts()` / `save_alerts()` functions still exist and are used by background jobs, the backtest harness, and the admin fallback path — but any endpoint that has `current_user: str = Depends(get_current_user)` must go through the per-user helpers. Adding a new user-facing endpoint and calling `load_portfolio()` directly is a data-isolation bug.
- **`user_data_store` is append-safe but not transactional across keys.** `save_user_data` is an upsert on a single `(user_id, data_key)` row. If you need to atomically update two keys (e.g. portfolio + alerts together), do it in a single SQLite transaction via `get_conn()`. Currently each helper is independent.
- **Backtest harness lives in `backend/backtest_phase4.py`, NOT main.py.** Reachable via `/api/recommendations/backtest?lookback_days=90&sensitivity=true&refit_curve=true`. The harness reads `predictions.json` + yfinance prices and replays the Phase 1-3 BUY/SELL rules — it does **not** call the multi-agent orchestrator (too expensive over hundreds of dates). When you change a rule in `_build_recommendations`, mirror it in `backtest_phase4._simulate_exit` / the sizing block, or backtest results will silently drift from reality. The sensitivity sweep uses `_prefetch_market_data` to fetch SPY/VIX/tickers once and pass a `_market_cache` to each config — don't bypass this or the sweep will hit yfinance rate limits at 36× concurrency.
