# Stock Research Command

Research stocks, sectors, or company types based on the provided input: **$ARGUMENTS**

Determine what was provided:
- If it looks like **stock tickers** (e.g. `IONQ RGTI QBTS`): research each ticker individually
- If it looks like a **sector or theme** (e.g. `quantum computing sector`): identify the top publicly traded companies in that space
- If it looks like a **company description** (e.g. `pre-IPO AI companies 2026`): find and evaluate matching candidates

## Research Steps

For each stock or company identified, perform the following:

### 1. Company Overview
- Full company name, ticker, exchange
- Sector, industry, and sub-sector
- Business model summary (what they do and how they make money)
- Stage: early-stage, growth, mature, or turnaround

### 2. Recent News & Catalysts
Search the web for the latest news (past 30–90 days). Highlight:
- Earnings results or guidance updates
- Product launches, partnerships, or contracts
- Regulatory approvals or government contracts
- Executive changes or insider activity
- Analyst upgrades/downgrades

### 3. Financial Snapshot
Gather and summarize key metrics:
- Market cap and enterprise value
- Revenue (TTM) and YoY growth rate
- Gross margin and operating margin
- Net income or adjusted EBITDA
- Cash position and burn rate (if pre-profit)
- P/E, P/S, EV/Revenue multiples vs. sector peers

### 4. Technical Analysis
- Current price vs. 52-week range
- Trend: is it in an uptrend, downtrend, or consolidation?
- Key support and resistance levels
- Recent volume patterns (any unusual spikes?)
- RSI / momentum indicator if available

### 5. Bull Case
List 3–5 reasons this stock could outperform:
- TAM expansion or market share gains
- Upcoming catalysts (product launch, FDA decision, contract win, etc.)
- Competitive moat or proprietary technology
- Improving unit economics or path to profitability

### 6. Bear Case
List 3–5 risks:
- Competition or commoditization risk
- Dilution risk (frequent share issuances)
- Regulatory, macro, or geopolitical headwinds
- Execution risk or management credibility
- Valuation stretched relative to fundamentals

### 7. Verdict
Provide a concise investment summary:
- **Outlook**: Bullish / Neutral / Bearish
- **Time horizon**: Short-term trade vs. long-term hold
- **Entry considerations**: Current price attractive, wait for pullback, or avoid
- **Key thing to watch**: The single most important metric or event to monitor

---

## Output Format

Present results as a structured report. If multiple tickers were given, use a separate section per ticker, then finish with a **Comparative Summary** table:

| Ticker | Sector | Market Cap | Revenue Growth | Outlook | Key Catalyst |
|--------|--------|------------|----------------|---------|---------------|
| ...    | ...    | ...        | ...            | ...     | ...           |

Be data-driven and concise. Cite sources where possible. Flag any data that could not be verified.
