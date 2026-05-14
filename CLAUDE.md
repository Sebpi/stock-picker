# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Backend
```bash
# Install dependencies
cd backend && pip install -r requirements.txt

# Run the server (serves frontend + API at http://localhost:8000)
cd backend && uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# First run: admin account is auto-created with a random password printed to the terminal
```

### Tests (Playwright — backend must be running on port 8000)
```bash
npm install && npx playwright install chromium  # one-time setup

npx playwright test                              # all tests
npx playwright test tests/screener.spec.js       # single file
npx playwright show-report test-results-run      # view HTML report
```

Test credentials default to the auto-created admin account; override with `SP_USER` / `SP_PASS` env vars.

## Architecture

### Request Flow
FastAPI (`backend/main.py`) serves both the REST API (`/api/*`) and the static frontend (`frontend/`). The frontend is plain HTML/JS with no build step. All API endpoints require a JWT Bearer token (24-hour lifetime) except `/api/auth/login`, `/api/auth/forgot-password`, `/api/auth/reset-password`, and `/api/health`.

### Two Storage Layers
- **JSON files** (`backend/*.json`) — `watchlist.json`, `predictions.json`, `portfolio.json`, `paper_portfolio.json`, `alerts.json`, `settings.json`, `users.json`. These back the UI directly and are excluded from git.
- **SQLite** (`backend/stockpicker.db`) — Primary store for all agent pipeline output: `agent_signal`, `investment_thesis`, `forecast_outcome`, `agent_run`, `thesis_run`, `consensus_history`, `valuation_history`, `ticker_master`, `alert_log`. Schema and helpers live in `db.py`.

### 9-Agent Thesis Pipeline
`backend/agents/` contains 8 deterministic data agents (no LLM) and one orchestrator:

| Agent | Signal |
|---|---|
| `FundamentalsAgent` | Revenue/EPS growth, margins, cash conversion |
| `ValuationAgent` | P/E, PEG, P/B, EV/EBITDA, DCF sensitivity, peer percentiles |
| `TechnicalRiskAgent` | MA20/50/200 trend, RSI, ATR, drawdown, support/resistance |
| `MacroLiquidityAgent` | Fed rates, yield curve, inflation, VIX, sector sensitivity |
| `GrowthRevisionsAgent` | Analyst consensus, EPS/revenue revisions, guidance vs. consensus |
| `SentimentNewsAgent` | News sentiment (24h/7d), narrative shifts |
| `IndustryCompetitionAgent` | Peer comparisons, relative growth/margin ranks |
| `PortfolioRiskAgent` | Sector concentration, correlation, beta |

`agents/orchestrator.py` runs all 8 agents, applies horizon-specific weights (`HORIZON_WEIGHTS` in `schemas.py` for 3m/6m/12m), aggregates into a composite score (0–100), generates bull/base/bear return forecasts, then calls Claude (default `THESIS_MODEL=claude-sonnet-4-6`) to produce the narrative. The result is stored as an `InvestmentThesis` in SQLite with `forecast_outcome` rows for each horizon.

All agents extend `BaseAgent` (`agents/__init__.py`), which provides `_timed_fetch()` (thread + timeout wrapper for yfinance/HTTP calls), retry logic via tenacity, and automatic SQLite run logging.

### Scheduled Jobs (APScheduler)
All jobs respect market hours (9:30 AM – 4:00 PM ET, Mon–Fri):

| Job | Interval | Purpose |
|---|---|---|
| `monitor_stocks` | 5 min | Price/volume/momentum alerts → SMS/email |
| `auto_predict` | 15 min | Factor score predictions for watchlist |
| `auto_thesis` | configurable (default off) | Full 9-agent pipeline per ticker |
| `auto_evaluate` | daily (1440 min) | Backtest matured 3m/6m/12m forecasts |
| `screener_prewarm` | 6 h | Pre-warm stock universe cache |

Scheduler settings (intervals, enable/disable) can be changed at runtime via the UI and are persisted in `scheduler_settings.py`.

### Claude API Usage
The app uses three distinct Claude call sites in `main.py`:
1. **`/api/recommend`** — conversational stock advice (model: `ANTHROPIC_MODEL`, default Haiku)
2. **`_stock_research_impl`** — live-data stock research reports (same model)
3. **Predictions** — structured JSON predictions for the full watchlist (same model)
4. **PDF import** — parsing brokerage PDF statements into trade transactions (same model)

The orchestrator (`agents/orchestrator.py`) uses a separate `THESIS_MODEL` (default `claude-sonnet-4-6`) for narrative generation. `sentiment_agent.py` is a standalone script (not imported by main.py) for hourly WhatsApp/email market alerts.

### Forecast Evaluation
`evaluation.py` is run by the scheduler to compare matured predictions against realised yfinance returns, updating `forecast_outcome.realised_return_pct` and `direction_match`, and computing directional accuracy, MAE, and alpha per agent and horizon.

## Key Environment Variables

Minimum required: `SECRET_KEY` and `ANTHROPIC_API_KEY`. Everything else has a usable default.

```
SECRET_KEY=<32+ char hex>             # python -c "import secrets; print(secrets.token_hex(32))"
ANTHROPIC_API_KEY=<key>
ANTHROPIC_MODEL=claude-haiku-4-5      # AI Advisor + Predictions (Haiku = fast/cheap)
THESIS_MODEL=claude-sonnet-4-6        # Thesis narrative (Sonnet recommended for quality)
PREDICTIONS_MAX_TOKENS=8192           # Must fit all watchlist stocks (~100 tokens/stock)
THESIS_AUTO_RUN_ENABLED=false         # Set true to enable scheduled thesis runs
FRED_API_KEY=                         # Optional: improves macro data quality (free at fred.stlouisfed.org)
ALLOW_NGROK_ORIGINS=true              # Set true when exposing via ngrok
APP_URL=https://your-subdomain...     # Public URL (used in password reset emails)
```

Alert channels (both optional): SMTP vars (`SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `ALERT_EMAIL`) for email, and Twilio vars (`TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER`, `TWILIO_TO_NUMBER`) for SMS/WhatsApp.
