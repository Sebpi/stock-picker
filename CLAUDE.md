# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> Lives at `~/code/stock-picker`. Own git repo, remote `Sebpi/stock-picker` on GitHub. Deploys to the Fly app `stock-picker-sp`. (The `/tmp/stock-picker/` copy mentioned in `SOAR/CODEX_HANDOVER.md` was wiped — this is the canonical working tree now.)

## Architecture

StockPicker is an AI-driven equity research / portfolio / alerts tool. Single-process FastAPI backend that also serves a hand-written React SPA loaded via CDN UMD — there is **no frontend build step**.

### Backend — `backend/main.py` (single ~7k-line FastAPI app)
- Python 3.12, FastAPI + uvicorn, JWT auth via `python-jose`, bcrypt password hashing, slowapi rate limiting, APScheduler for background jobs, Anthropic SDK for LLM calls, `yfinance`/`yahooquery` for market data, Twilio + SMTP for alerts, `reportlab`/`pypdf` for PDFs.
- Storage is **dual-format on purpose**:
  - SQLite at `$DATA_DIR/stockpicker.db` — multi-agent signals, thesis history, decision log (see `backend/db.py`).
  - JSON files under `$DATA_DIR` for legacy UI compatibility: `users.json`, `watchlist.json`, `predictions.json`, `portfolio.json`, `paper_portfolio.json`, `alerts.json`, `settings.json`, `lockout_state.json`, `sentiment_agent_state.json`.
  - `db.py` auto-migrates a legacy `backend/stockpicker.db` into `$DATA_DIR` on first boot if the latter doesn't exist. JSON writes use a temp-file + rename atomic pattern (`_atomic_write`).
- **`SECRET_KEY`** drives JWT signing. If unset, the app generates an ephemeral one and **all sessions invalidate on restart** — set it in `backend/.env` for any non-toy run.
- Auth surface: bcrypt + JWT (HS256), per-account lockout (5 attempts → 15-min cool-off, persisted to `lockout_state.json`), password reset via email token, default `admin` user auto-created on first run with a **random password printed to stdout once**.
- Three middleware stacks on top of FastAPI: trusted-host (`ALLOWED_HOSTS`), CORS (`ALLOWED_ORIGINS`), and an HTTPS-redirect / origin-check pair gated by `REQUIRE_HTTPS`.
- Ticker validation is centralised: `_validate_ticker()` enforces `^[A-Z0-9.\-]{1,10}$` — call this on every user-supplied ticker.
- A startup hook clears any `*_PROXY` env var pointing at a dead local port (port 9), to survive macOS leftover proxy envs.

### Multi-agent forecasting — `backend/agents/`
- Nine agents (`fundamentals`, `growth_revisions`, `industry_competition`, `macro_liquidity`, `portfolio_risk`, `sentiment_news`, `technical_risk`, `valuation`, plus the `orchestrator`). All agents emit `AgentSignal` (defined in `schemas.py`); the orchestrator aggregates them into an `InvestmentThesis` with bull/base/bear `HorizonForecast`s and a Claude-generated narrative.
- **Minimum 3 agents** must return successfully or the thesis is rejected (`MIN_AGENTS_REQUIRED`). Score → 12-month return uses linear interpolation over the `SCORE_TO_12M_RETURN` anchor table.
- Two distinct models in play:
  - `ANTHROPIC_MODEL` (default `claude-haiku-4-5-20251001`) for routine predictions/screens.
  - `THESIS_MODEL` (default `claude-sonnet-4-6`) for the thesis narrative — higher quality, higher cost.
- `PREDICTIONS_MAX_TOKENS` (default 8192) must be large enough for **all watchlist stocks in one response** — roughly 100 tokens per stock. Bump it before adding many tickers.

### Frontend — `frontend/`
- `index.html` + `react-app.js` (~2.6k lines) is the active app; `app.js` / `legacy-app.js` / `legacy.html` are older fallbacks (`/legacy` route). The active SPA uses **React 18 UMD from unpkg + Tailwind Play CDN** — no bundler, no npm install for the frontend itself.
- `tailwind.config` **must** be set in a script tag **after** the Tailwind CDN loads, or the custom `pulse-*` color utilities silently fail to generate (this is called out in an inline comment in `index.html` and is easy to break).
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

Required `backend/.env` keys (template at `backend/.env.example`): `SECRET_KEY` (32+ chars; `python -c "import secrets; print(secrets.token_hex(32))"`), `ANTHROPIC_API_KEY`. Optional: `ANTHROPIC_MODEL`, `THESIS_MODEL`, `PREDICTIONS_MAX_TOKENS`, `ALLOWED_HOSTS`, `ALLOWED_ORIGINS`, `REQUIRE_HTTPS`, `APP_URL`, `ALLOW_NGROK_ORIGINS`, `DATA_DIR`, Twilio (`TWILIO_*`) for WhatsApp, SMTP (`SMTP_*`) for email.

## Things to be careful with

- **Playwright config mismatch.** `playwright.config.js` only starts `tests/server.js` (a static frontend server on :4321) — it does **not** start the FastAPI backend. Since the SPA uses relative URLs (`API = ""`), tests effectively need both servers reachable on the same origin, which the static server cannot do alone. The current `test-results/` is full of `error-context.md` failures consistent with this. If you're touching tests, expect to either run `uvicorn` on :4321 yourself, run it on :8000 and reverse-proxy, or rework the test harness — don't assume the existing `npx playwright test` invocation is green out of the box.
- **`ALLOWED_IDEA_KEYS`-style allowlist doesn't apply here**, but the equivalent gotcha is `_TICKER_RE` — anything user-supplied that bypasses `_validate_ticker()` is a bug.
- **Don't rearrange the `tailwind.config` script tag** in `index.html` (must come after the CDN script) or the entire dark theme renders white. There's a comment in the file explaining why.
- **State files vs symlinks.** In production, `backend/users.json` etc. are symlinks into `/app/data`. Editing them locally via the symlink path will silently write into your local `data/` directory once `DATA_DIR` is set — be deliberate about which copy you're touching when reproducing prod bugs.
- **Version bumps are manual.** Change the `<title>` and the `?v=` query in `index.html` together; otherwise browsers serve stale `react-app.js`.
- **Two LLM models, two cost profiles.** Generating predictions for a large watchlist on the Sonnet thesis model is significantly more expensive than the Haiku default — keep them separate.
- **Two recommendation engines with different sources of truth.** `_build_recommendations` (Signals tab) reads `paper_portfolio.json` and prediction scores; `_build_recommendation_alert_snapshot` (Alerts) reads `portfolio.json` + the watchlist and uses the 9-agent thesis. Phase 1 unifies the "already held" check across both by including paper holdings in the alert snapshot's skip list — keep them in sync if you add new BUY filters to one.
- **Backtest harness lives in `backend/backtest_phase4.py`, NOT main.py.** Reachable via `/api/recommendations/backtest?lookback_days=90&sensitivity=true&refit_curve=true`. The harness reads `predictions.json` + yfinance prices and replays the Phase 1-3 BUY/SELL rules — it does **not** call the multi-agent orchestrator (too expensive over hundreds of dates). When you change a rule in `_build_recommendations`, mirror it in `backtest_phase4._simulate_exit` / the sizing block, or backtest results will silently drift from reality.
