# StockPicker

AI-powered stock analysis: predictions, screening, portfolio tracking, and WhatsApp/email alerts.

## Quick start

### 1. Clone and install

```bash
git clone <repo-url>
cd StockPicker

# Python deps
cd backend
pip install -r requirements.txt

# Node deps (Playwright tests only)
cd ..
npm install
npx playwright install chromium
```

### 2. Configure environment

```bash
cp backend/.env.example backend/.env
```

Open `backend/.env` and fill in at minimum:

| Key | Required | Notes |
|-----|----------|-------|
| `SECRET_KEY` | Yes | Random 32+ char string — `python -c "import secrets; print(secrets.token_hex(32))"` |
| `ANTHROPIC_API_KEY` | Yes | From [console.anthropic.com](https://console.anthropic.com) |
| Twilio vars | No | WhatsApp alerts only |
| SMTP vars | No | Email alerts only |

### 3. Run

```bash
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Open **http://localhost:8000** in your browser.

**First run:** the app auto-creates an `admin` account with a random password printed to the terminal. Change it after first login.

---

## Runtime data files (not in git)

These files live in `backend/` and are created automatically on first use. They are excluded from git intentionally — back them up separately if you want to preserve data across machines.

| File | Contents |
|------|----------|
| `users.json` | Login credentials |
| `watchlist.json` | Tracked tickers |
| `predictions.json` | Historical AI predictions |
| `portfolio.json` | Real portfolio transactions |
| `paper_portfolio.json` | Paper trading transactions |
| `alerts.json` | Configured alert rules |
| `settings.json` | App settings |

To migrate to a new machine, copy these files from `backend/` on the old machine.

---

## Running tests

The backend must be running on port 8000 before running Playwright tests.

```bash
# Run all tests
npx playwright test

# Run a specific suite
npx playwright test tests/screener.spec.js

# Open HTML report after a run
npx playwright show-report test-results-run
```

Set `SP_USER` / `SP_PASS` env vars if your admin credentials differ from the defaults.

---

## Key environment variables

See `backend/.env.example` for the full list with descriptions. The most important ones:

- **`ANTHROPIC_MODEL`** — defaults to `claude-haiku-4-5-20251001`. Switch to a Sonnet model for higher quality predictions at higher cost.
- **`PREDICTIONS_MAX_TOKENS`** — defaults to `8192`. Must be high enough to fit all watchlist stocks in one response (~100 tokens per stock).
- **`ALLOW_NGROK_ORIGINS`** — set to `true` if exposing the app via ngrok.
- **`APP_URL`** — set to your public URL so password-reset emails contain the right link.
