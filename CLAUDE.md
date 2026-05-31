# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> Lives at `~/code/stock-picker`. Own git repo, remote `Sebpi/stock-picker` on GitHub. Deploys to the Fly app `stock-picker-sp`. (The `/tmp/stock-picker/` copy mentioned in `SOAR/CODEX_HANDOVER.md` was wiped — this is the canonical working tree now.)

## Architecture

StockPicker is an AI-driven equity research / portfolio / alerts tool. Single-process FastAPI backend that also serves a hand-written React SPA loaded via CDN UMD — there is **no frontend build step**.

### Backend — `backend/main.py` (single ~7k-line FastAPI app)
- Python 3.12, FastAPI + uvicorn, JWT auth via `python-jose`, bcrypt password hashing, slowapi rate limiting, APScheduler for background jobs, Anthropic SDK for LLM calls, `yfinance`/`yahooquery` for market data, Twilio + SMTP for alerts, `reportlab`/`pypdf` for PDFs.
- Storage is **dual-format on purpose**:
  - SQLite at `$DATA_DIR/stockpicker.db` — multi-agent signals, thesis history, decision log (see `backend/db.py`). Also contains an `insider_transactions` table managed by `backend/insider_transactions.py` (CREATE TABLE IF NOT EXISTS, no migration script needed).
  - JSON files under `$DATA_DIR` for legacy UI compatibility: `users.json`, `watchlist.json`, `predictions.json`, `portfolio.json`, `paper_portfolio.json`, `alerts.json`, `settings.json`, `lockout_state.json`, `sentiment_agent_state.json`.
  - `db.py` auto-migrates a legacy `backend/stockpicker.db` into `$DATA_DIR` on first boot if the latter doesn't exist. JSON writes use a temp-file + rename atomic pattern (`_atomic_write`).
- **`SECRET_KEY`** drives JWT signing. If unset, the app generates an ephemeral one and **all sessions invalidate on restart** — set it in `backend/.env` for any non-toy run.
- Auth surface: bcrypt + JWT (HS256), per-account lockout (5 attempts → 15-min cool-off, persisted to `lockout_state.json`), password reset via email token, default `admin` user auto-created on first run with a **random password printed to stdout once**.
- **Portal JWT integration (LENS shared-session):** `get_current_user` and `auth_middleware` both accept JWTs minted by `seb-portal /api/auth/jwt` when `PORTAL_JWT_SECRET` is set. Only tokens with `iss="seb-portal"` pass through; portal-routed identity is returned as `"portal:<sub>"` so audit logs can distinguish it from local logins. Portal users don't need a `users.json` entry.
- Three middleware stacks on top of FastAPI: trusted-host (`ALLOWED_HOSTS`), CORS (`ALLOWED_ORIGINS`), and an HTTPS-redirect / origin-check pair gated by `REQUIRE_HTTPS`.
- Ticker validation is centralised: `_validate_ticker()` enforces `^[A-Z0-9.\-]{1,10}$` — call this on every user-supplied ticker.
- A startup hook clears any `*_PROXY` env var pointing at a dead local port (port 9), to survive macOS leftover proxy envs.

### Multi-agent forecasting — `backend/agents/`
- **Fourteen agents** (`credit_risk`, `earnings_quality`, `fundamentals`, `growth_revisions`, `industry_competition`, `insider_activity`, `macro_liquidity`, `options_flow`, `portfolio_risk`, `sentiment_news`, `short_interest`, `technical_risk`, `valuation`, plus the `orchestrator`). All agents emit `AgentSignal` (defined in `schemas.py`); the orchestrator aggregates them into an `InvestmentThesis` with bull/base/bear `HorizonForecast`s and a Claude-generated narrative.
- **`insider_activity`** (`backend/agents/insider_activity.py`) reads the 60-day Form 4 summary from `insider_transactions.summarize_ticker` (refreshing from SEC EDGAR if the cache is cold). Scoring ladder: 88 cluster-buying → 32 heavy multi-insider selling. Weighted heaviest on the 3-month horizon (0.10) where Form 4 cluster signals are strongest; 0.04 at 12m.
- **`options_flow`** (`backend/agents/options_flow.py`) reads the live yfinance options chain for the nearest 1–3 expiries. Scores on put/call ratio (PCR), IV skew (near-ATM put IV minus call IV), and volume direction. PCR < 0.50 → 85 (heavy call bias); PCR > 1.55 → 26 (heavy put buying). Weighted 0.09 at 3m, 0.04 at 6m, 0.02 at 12m. LOW_COVERAGE for stocks with no options or < 100 total chain volume.
- **`earnings_quality`** (`backend/agents/earnings_quality.py`) computes Beneish M-score (8-factor manipulation detector; M > −1.78 = elevated risk) and Sloan accruals ratio ((NI − CFO) / avg assets; > 0.10 = expect earnings reversion) from annual financial statements. No new data source — uses same yfinance financials/balance_sheet/cashflow already fetched. Weighted 0.04 at 3m, 0.09 at 6m, 0.10 at 12m (accruals revert over 6–18 months).
- **`short_interest`** (`backend/agents/short_interest.py`) reads FINRA short interest via yfinance (`shortPercentOfFloat`, `shortRatio`, `sharesShort`, `sharesShortPriorMonth`). Scores on short % of float (low = bullish) and MoM change direction (falling = covering = bullish). Squeeze risk bonus when short % ≥ 10% + days-to-cover ≥ 8 + MoM falling. Weighted 0.06 at 3m, 0.05 at 6m, 0.03 at 12m.
- **`credit_risk`** (`backend/agents/credit_risk.py`) combines macro credit spread environment with company leverage profile. Uses FRED OAS spreads (BAMLH0A0HYM2 HY, BAMLC0A0CM IG) when `FRED_API_KEY` is set, otherwise falls back to HYG/LQD ETF 30-day momentum. Leverage adjustment: D/E < 0.5 → +5; D/E > 3.0 or interest coverage < 3× → −15; negative FCF + high leverage → additional −5. Weighted 0.08 at 3m, 0.06 at 6m, 0.04 at 12m. Credit spreads lead equity stress by 2–8 weeks.
- **Minimum 3 agents** must return successfully or the thesis is rejected (`MIN_AGENTS_REQUIRED`). Alert snapshot requires **≥5 of 11 agents** net-positive (`alert_min_positive_agents`). Score → 12-month return uses linear interpolation over the `SCORE_TO_12M_RETURN` anchor table.
- `HORIZON_WEIGHTS` in `schemas.py` were rebalanced when `insider_activity` was added — the orchestrator's `_weighted_score` normalises by sum of usable weights, so the ratios matter, not the totals.
- Two distinct models in play:
  - `ANTHROPIC_MODEL` (default `claude-haiku-4-5-20251001`) for routine predictions/screens.
  - `THESIS_MODEL` (default `claude-sonnet-4-6`) for the thesis narrative — higher quality, higher cost.
- `PREDICTIONS_MAX_TOKENS` (default 8192) must be large enough for **all watchlist stocks in one response** — roughly 100 tokens per stock. Bump it before adding many tickers.

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

### Frontend — `frontend/`
- `index.html` + `react-app.js` (~2.6k lines) is the active app; `app.js` / `legacy-app.js` / `legacy.html` are older fallbacks (`/legacy` route). The active SPA uses **React 18 UMD from unpkg** and a **pre-built local Tailwind CSS** (`frontend/tailwind.css`) — no bundler, but Tailwind must be rebuilt when new utility classes are added.
- **Rebuilding Tailwind CSS:** run `./node_modules/.bin/tailwindcss -i tailwind.input.css -o frontend/tailwind.css --minify` from the repo root whenever you add new Tailwind classes to `react-app.js` or `index.html`. After regenerating: recompute the SRI hash (`python3 -c "import hashlib,base64,sys; d=open('frontend/tailwind.css','rb').read(); print('sha256-'+base64.b64encode(hashlib.sha256(d).digest()).decode())"`) and update the `integrity=` attribute in `index.html`. The config lives in `tailwind.config.js` at the repo root.
- Version string in `index.html` (`<title>StockLens vX.Y.Z`) and the `?v=` cache-buster on `react-app.js` are hand-edited — there is no build that injects them.
- `fetch` uses `API = ""` (relative paths), so the SPA must be served from the same origin as the FastAPI backend in production.

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

Required `backend/.env` keys (template at `backend/.env.example`): `SECRET_KEY` (32+ chars; `python -c "import secrets; print(secrets.token_hex(32))"`), `ANTHROPIC_API_KEY`. Optional: `ANTHROPIC_MODEL`, `THESIS_MODEL`, `PREDICTIONS_MAX_TOKENS`, `ALLOWED_HOSTS`, `ALLOWED_ORIGINS`, `REQUIRE_HTTPS`, `APP_URL`, `ALLOW_NGROK_ORIGINS`, `DATA_DIR`, Twilio (`TWILIO_*`) for WhatsApp, SMTP (`SMTP_*`) for email, `PORTAL_JWT_SECRET` (shared with `seb-portal` for LENS cross-app auth), `EDGAR_USER_AGENT` (SEC EDGAR courtesy header for Form 4 fetches).

## Things to be careful with

- **Playwright test harness.** `tests/server.js` is a proxy server — it serves the frontend static files on `:4321` AND proxies `/api/*` and `/v1/*` to the FastAPI backend on `:8000`. Start the backend first (`uvicorn main:app --port 8000`), then `npx playwright test`. The harness won't work without a running backend. React DOM IDs are baked into `react-app.js` and `index.html` for test selectors — don't rename them without updating the tests.
- **`ALLOWED_IDEA_KEYS`-style allowlist doesn't apply here**, but the equivalent gotcha is `_TICKER_RE` — anything user-supplied that bypasses `_validate_ticker()` is a bug.
- **Don't reorder the `<link rel="stylesheet">` for `tailwind.css`** in `index.html` — it must load before React mounts or components render unstyled.
- **CSP inline-script hash must stay in sync.** `backend/main.py` `security_headers` middleware uses a `sha256-` hash for the one remaining inline `<script>` block in `index.html` (the theme-detection snippet). If you change its content — even whitespace — recompute the hash: `python3 -c "import hashlib,base64,re; s=re.findall(r'<script(?![^>]*\bsrc\b)[^>]*>(.*?)</script>',open('frontend/index.html').read(),re.DOTALL); print('sha256-'+base64.b64encode(hashlib.sha256(s[0].encode()).digest()).decode())"`. `style-src 'self'` is now clean — no more `unsafe-inline` since Tailwind is served as a static pre-built file.
- **Always check recent git commits** before making changes (`git log --oneline -15`) to avoid reverting work already merged.
- **PR workflow:** squash-merge every PR, then delete the branch. GitHub auto-deletes on squash merge if the repo setting is on; otherwise `git push origin --delete <branch>`. Always rebase the working branch onto `origin/main` before opening a PR to avoid dirty merge state.
- **State files vs symlinks.** In production, `backend/users.json` etc. are symlinks into `/app/data`. Editing them locally via the symlink path will silently write into your local `data/` directory once `DATA_DIR` is set — be deliberate about which copy you're touching when reproducing prod bugs.
- **Version bumps are manual.** Change the `<title>` and the `?v=` query in `index.html` together; otherwise browsers serve stale `react-app.js`.
- **Two LLM models, two cost profiles.** Generating predictions for a large watchlist on the Sonnet thesis model is significantly more expensive than the Haiku default — keep them separate.
- **Two recommendation engines with different sources of truth.** `_build_recommendations` (Signals tab) reads `paper_portfolio.json` and prediction scores; `_build_recommendation_alert_snapshot` (Alerts) reads `portfolio.json` + the watchlist and uses the 10-agent thesis. Phase 1 unifies the "already held" check across both by including paper holdings in the alert snapshot's skip list; Phase 2 wires the market-regime gate into both. Alert snapshot requires ≥5 positive agents (5-of-10). Keep these two engines in sync if you add new BUY filters to one.
- **Backtest harness lives in `backend/backtest_phase4.py`, NOT main.py.** Reachable via `/api/recommendations/backtest?lookback_days=90&sensitivity=true&refit_curve=true`. The harness reads `predictions.json` + yfinance prices and replays the Phase 1-3 BUY/SELL rules — it does **not** call the multi-agent orchestrator (too expensive over hundreds of dates). When you change a rule in `_build_recommendations`, mirror it in `backtest_phase4._simulate_exit` / the sizing block, or backtest results will silently drift from reality. The sensitivity sweep uses `_prefetch_market_data` to fetch SPY/VIX/tickers once and pass a `_market_cache` to each config — don't bypass this or the sweep will hit yfinance rate limits at 36× concurrency.
