"""
earnings_report.py — On-demand deep post-earnings analyst report.

Fetches structured data from yfinance using only the attributes confirmed
to work by the existing 21-agent production codebase, adds a _timed_fetch
wrapper for every network call, and synthesises a full analyst-style report
using Claude Sonnet. Results are cached in-memory for 4 hours.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time as _time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

_T = TypeVar("_T")
_FETCH_TIMEOUT = float(os.getenv("FETCH_TIMEOUT_SECS", "15"))

# 4-hour in-memory cache keyed by ticker
_REPORT_CACHE: dict[str, dict] = {}
_CACHE_TTL = 4 * 3600


# ── helpers ───────────────────────────────────────────────────────────────────

def _timed_fetch(fn: Callable[[], _T], label: str = "") -> _T | None:
    """Run fn() in a thread with a hard timeout. Same pattern as BaseAgent."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fn)
        try:
            return future.result(timeout=_FETCH_TIMEOUT)
        except FuturesTimeoutError:
            logger.debug("[earnings_report] timeout: %s", label)
            future.cancel()
            return None
        except Exception as exc:
            logger.debug("[earnings_report] error [%s]: %s", label, exc)
            return None


def _sf(val: Any) -> float | None:
    try:
        f = float(val)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _row(df: Any, *names: str, col: int = 0) -> float | None:
    """Return float value for the first matching row name in a DataFrame column."""
    if df is None or df.empty or df.shape[1] <= col:
        return None
    for name in names:
        if name in df.index:
            val = df.loc[name, df.columns[col]]
            if val is not None and val == val:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
    return None


def _fmt_period(col: Any) -> str:
    try:
        dt = col.to_pydatetime() if hasattr(col, "to_pydatetime") else (
            datetime.fromisoformat(str(col)[:10]) if isinstance(col, str) else col
        )
        return f"Q{(dt.month - 1) // 3 + 1} {dt.year}"
    except Exception:
        return str(col)[:10]


# ── data fetchers — only use attributes confirmed working in production ────────

def _fetch_quarterly_results(t: Any) -> list[dict]:
    """
    Merge EPS beat/miss data (earnings_history — confirmed working) with
    quarterly P&L (quarterly_financials — backward-compat alias confirmed
    to have same row labels as t.financials).
    """
    # EPS history: confirmed working via earnings_surprise agent
    eps_by_date: dict[str, dict] = {}
    eh = _timed_fetch(lambda: t.earnings_history, "earnings_history")
    if eh is not None and not eh.empty:
        try:
            for idx, row in eh.sort_index(ascending=False).head(4).iterrows():
                date_str = str(idx)[:10]
                eps_by_date[date_str] = {
                    "eps_estimate": _sf(row.get("epsEstimate") or row.get("EPS Estimate")),
                    "eps_actual":   _sf(row.get("epsActual")   or row.get("Reported EPS") or row.get("EPS Actual")),
                    "eps_surprise_pct": _sf(row.get("surprisePercent") or row.get("Surprise(%)")),
                }
        except Exception as exc:
            logger.debug("[earnings_report] earnings_history parse: %s", exc)

    # Quarterly P&L — try quarterly_financials (backward-compat alias for
    # quarterly_income_stmt); same row labels as t.financials (confirmed working)
    rev_rows: list[dict] = []
    qfin = _timed_fetch(lambda: t.quarterly_financials, "quarterly_financials")
    if qfin is not None and not qfin.empty:
        try:
            all_cols = list(qfin.columns)
            for i, col in enumerate(all_cols[:4]):
                period = _fmt_period(col)
                date_str = str(col)[:10]

                rev = _row(qfin, "Total Revenue",    col=i)
                gp  = _row(qfin, "Gross Profit",     col=i)
                op  = _row(qfin, "Operating Income", "EBIT", "Operating Profit", col=i)
                ni  = _row(qfin, "Net Income", "Net Income Common Stockholders", col=i)

                # Same-quarter prior year is 4 quarters back
                prev_rev = _row(qfin, "Total Revenue", col=i + 4) if i + 4 < len(all_cols) else None
                rev_yoy = (
                    round((rev - prev_rev) / abs(prev_rev) * 100, 1)
                    if (rev and prev_rev and prev_rev != 0) else None
                )

                row_dict: dict = {
                    "period": period, "date": date_str,
                    "revenue_actual_bn":    round(rev / 1e9, 2) if rev else None,
                    "revenue_yoy_pct":      rev_yoy,
                    "gross_margin_pct":     round(gp / rev * 100, 1) if (gp and rev and rev != 0) else None,
                    "operating_margin_pct": round(op / rev * 100, 1) if (op and rev and rev != 0) else None,
                    "net_margin_pct":       round(ni / rev * 100, 1) if (ni and rev and rev != 0) else None,
                }

                # Attach nearest EPS record (within 45 days)
                try:
                    q_dt = datetime.fromisoformat(date_str)
                    best = min(
                        eps_by_date,
                        key=lambda d: abs((datetime.fromisoformat(d) - q_dt).days),
                        default=None,
                    )
                    if best and abs((datetime.fromisoformat(best) - q_dt).days) <= 45:
                        row_dict.update(eps_by_date[best])
                        e, a = row_dict.get("eps_estimate"), row_dict.get("eps_actual")
                        if a is not None and e is not None:
                            row_dict["beat"] = a >= e
                except Exception:
                    pass

                rev_rows.append(row_dict)
        except Exception as exc:
            logger.debug("[earnings_report] quarterly_financials parse: %s", exc)

    if rev_rows:
        return rev_rows

    # Fallback: EPS-only rows from earnings_history
    results = []
    for date_str, eps in sorted(eps_by_date.items(), reverse=True)[:4]:
        try:
            dt = datetime.fromisoformat(date_str)
            period = f"Q{(dt.month-1)//3+1} {dt.year}"
        except Exception:
            period = date_str
        e, a = eps.get("eps_estimate"), eps.get("eps_actual")
        results.append({
            "period": period, "date": date_str, **eps,
            "beat": (a >= e if (a is not None and e is not None) else None),
        })
    return results


def _fetch_analyst_actions(t: Any, days: int = 90) -> list[dict]:
    """Fetch recent upgrades/downgrades. Falls back to empty list gracefully."""
    actions: list[dict] = []
    ud = _timed_fetch(lambda: t.upgrades_downgrades, "upgrades_downgrades")
    if ud is None or ud.empty:
        return actions
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        for idx, row in ud.iterrows():
            try:
                dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                if getattr(dt, "tzinfo", None) is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt < cutoff:
                    continue
            except Exception:
                pass
            actions.append({
                "date": str(idx)[:10],
                "firm": str(row.get("Firm") or ""),
                "action": str(row.get("Action") or "").lower(),
                "from_grade": str(row.get("FromGrade") or ""),
                "to_grade": str(row.get("ToGrade") or ""),
            })
            if len(actions) >= 12:
                break
    except Exception as exc:
        logger.debug("[earnings_report] upgrades_downgrades parse: %s", exc)
    return actions


def _fetch_next_quarter(info: dict) -> dict:
    """
    Next-quarter estimates from info dict (no separate API call needed).
    earningsTimestamp gives the next earnings date; forward EPS/revenue
    estimates come from forwardEps and analystGrowth fields.
    """
    est: dict = {}
    try:
        ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
        if ts:
            est["date"] = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        pass

    fwd_eps = _sf(info.get("forwardEps"))
    if fwd_eps is not None:
        est["eps_estimate_mean"] = fwd_eps

    fwd_rev = _sf(info.get("revenueEstimate") or info.get("expectedRevenue"))
    if fwd_rev is not None:
        est["revenue_mean_bn"] = round(fwd_rev / 1e9, 2)

    return est


def _fetch_technicals(t: Any, info: dict, quarterly_results: list[dict]) -> dict:
    """
    Compute price-action technicals from 1-year daily history.
    Uses t.history() — confirmed working across all technical/momentum agents.
    """
    tech: dict = {}
    hist = _timed_fetch(lambda: t.history(period="1y"), "history_1y")
    if hist is None or hist.empty:
        return tech
    try:
        closes  = hist["Close"]
        volumes = hist.get("Volume")
        dates   = [str(d)[:10] for d in hist.index]

        current = _sf(closes.iloc[-1])
        if current is None:
            return tech
        tech["current_price"] = round(current, 2)

        high52 = float(closes.max())
        low52  = float(closes.min())
        if high52 > low52:
            tech["week52_position_pct"] = round((current - low52) / (high52 - low52) * 100, 1)

        for period_days, key in [(20, "ma20"), (50, "ma50"), (200, "ma200")]:
            if len(closes) >= period_days:
                ma = float(closes.iloc[-period_days:].mean())
                tech[key] = round(ma, 2)
                if ma > 0:
                    tech[f"{key}_vs_price"] = round((current - ma) / ma * 100, 1)

        for lookback, key in [(21, "return_1m_pct"), (63, "return_3m_pct")]:
            if len(closes) >= lookback:
                past = float(closes.iloc[-lookback])
                if past > 0:
                    tech[key] = round((current - past) / past * 100, 1)

        # RSI-14
        if len(closes) >= 15:
            deltas = closes.diff().dropna().iloc[-14:]
            gains  = float(deltas.clip(lower=0).mean())
            losses = float((-deltas.clip(upper=0)).mean())
            if losses > 0:
                tech["rsi_14"] = round(100 - 100 / (1 + gains / losses), 1)
            else:
                tech["rsi_14"] = 100.0

        avg_vol_30 = None
        if volumes is not None and not volumes.empty:
            avg_vol_30 = float(volumes.iloc[-30:].mean()) if len(volumes) >= 30 else float(volumes.mean())
            tech["avg_volume_30d"] = int(avg_vol_30)

        # Post-earnings price reaction
        earnings_date_str: str | None = quarterly_results[0].get("date") if quarterly_results else None
        if earnings_date_str and earnings_date_str in dates:
            idx = dates.index(earnings_date_str)
            pre  = float(closes.iloc[idx - 1]) if idx >= 1 else None
            day0 = float(closes.iloc[idx])
            day1 = float(closes.iloc[idx + 1]) if idx + 1 < len(closes) else None
            if pre and pre > 0:
                tech["post_earnings_day0_pct"] = round((day0 - pre) / pre * 100, 2)
            if pre and day1 and pre > 0:
                tech["post_earnings_day1_pct"] = round((day1 - pre) / pre * 100, 2)
            if avg_vol_30 and avg_vol_30 > 0:
                try:
                    earn_vol = float(volumes.iloc[idx])
                    tech["earnings_day_volume_ratio"] = round(earn_vol / avg_vol_30, 2)
                except Exception:
                    pass

        # Trend label
        ma50, ma200 = tech.get("ma50"), tech.get("ma200")
        if ma50 and ma200:
            tech["trend"] = (
                "uptrend"    if current > ma50 > ma200 else
                "downtrend"  if current < ma50 < ma200 else
                "above_200d" if current > ma200 else
                "below_200d"
            )
    except Exception as exc:
        logger.debug("[earnings_report] technicals: %s", exc)
    return tech


def _fetch_market_sentiment(t: Any, info: dict, technicals: dict) -> dict:
    """Aggregate market sentiment from info fields + price action."""
    sentiment: dict = {}

    # Short interest + institutional ownership (confirmed in info dict via
    # existing short_interest and institutional_flow agents)
    for src, dest, scale in [
        ("shortPercentOfFloat",    "short_pct_float",    100.0),
        ("shortRatio",             "days_to_cover",      1.0),
        ("institutionsPercentHeld","inst_ownership_pct", 100.0),
    ]:
        v = _sf(info.get(src))
        if v is not None:
            sentiment[dest] = round(v * scale, 1) if scale != 1.0 else round(v, 1)

    # Post-earnings market reaction characterisation
    day0 = technicals.get("post_earnings_day0_pct")
    if day0 is not None:
        sentiment["post_earnings_reaction"] = (
            "strong_positive" if day0 >= 5 else
            "positive"        if day0 >= 2 else
            "strong_negative" if day0 <= -5 else
            "negative"        if day0 <= -2 else
            "muted"
        )

    pos = technicals.get("week52_position_pct")
    if pos is not None:
        sentiment["positioning"] = (
            "near_52w_high" if pos >= 80 else
            "near_52w_low"  if pos <= 20 else
            "mid_range"
        )

    # News headlines — handle both old-style flat dict and new nested structure
    try:
        raw_news = _timed_fetch(lambda: t.news, "news") or []
        titles = []
        for n in raw_news[:10]:
            # New yfinance 1.x structure: {"content": {"title": ...}}
            title = (
                (n.get("content") or {}).get("title") or
                n.get("title") or ""
            )
            if title:
                titles.append(title)
        if titles:
            sentiment["recent_news_titles"] = titles[:5]
            sentiment["news_count"] = len(titles)
    except Exception as exc:
        logger.debug("[earnings_report] news: %s", exc)

    return sentiment


# ── LLM synthesis ──────────────────────────────────────────────────────────────

def _synthesise(
    ticker: str,
    company_name: str,
    quarterly_results: list[dict],
    trailing_metrics: dict,
    valuation: dict,
    analyst_consensus: dict,
    analyst_actions: list[dict],
    next_quarter: dict,
    technicals: dict,
    market_sentiment: dict,
) -> dict:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return {"_llm": False, "error": "ANTHROPIC_API_KEY not configured"}

    model = os.getenv("THESIS_MODEL", "claude-sonnet-4-6")
    import anthropic

    def _clean(d: dict) -> dict:
        return {k: v for k, v in d.items() if v is not None and v != [] and v != {}}

    # ── compact text summaries ────────────────────────────────────────────────
    q_lines: list[str] = []
    for q in quarterly_results[:4]:
        parts = [q.get("period", "?")]
        eps_a, eps_e, surp = q.get("eps_actual"), q.get("eps_estimate"), q.get("eps_surprise_pct")
        if eps_a is not None and eps_e is not None:
            tag = "BEAT" if q.get("beat") else "MISS"
            parts.append(f"EPS {tag} ${eps_a:.2f} vs ${eps_e:.2f}" + (f" ({surp:+.1f}%)" if surp is not None else ""))
        elif eps_a is not None:
            parts.append(f"EPS ${eps_a:.2f}")
        rev = q.get("revenue_actual_bn")
        if rev is not None:
            yoy = q.get("revenue_yoy_pct")
            parts.append(f"Rev ${rev:.1f}B" + (f" ({yoy:+.1f}% YoY)" if yoy is not None else ""))
        for label, key in [("GM", "gross_margin_pct"), ("OpMgn", "operating_margin_pct")]:
            if q.get(key) is not None:
                parts.append(f"{label} {q[key]:.1f}%")
        q_lines.append(" | ".join(parts))

    act_lines = [
        f"{a['date']} {a['firm']}: {a['action']}"
        + (f" {a['from_grade']} → {a['to_grade']}" if a.get("from_grade") and a.get("to_grade") else "")
        for a in analyst_actions[:10]
    ]

    tech_lines: list[str] = []
    trend = technicals.get("trend")
    if trend:
        tech_lines.append(f"Trend: {trend.replace('_', ' ')}")
    for label, key in [("1M return", "return_1m_pct"), ("3M return", "return_3m_pct")]:
        v = technicals.get(key)
        if v is not None:
            tech_lines.append(f"{label}: {v:+.1f}%")
    rsi = technicals.get("rsi_14")
    if rsi is not None:
        suffix = " (overbought)" if rsi > 70 else " (oversold)" if rsi < 30 else ""
        tech_lines.append(f"RSI-14: {rsi:.0f}{suffix}")
    pos52 = technicals.get("week52_position_pct")
    if pos52 is not None:
        tech_lines.append(f"52W position: {pos52:.0f}%ile")
    for label, key in [("vs MA20", "ma20_vs_price"), ("vs MA50", "ma50_vs_price"), ("vs MA200", "ma200_vs_price")]:
        v = technicals.get(key)
        if v is not None:
            tech_lines.append(f"{label}: {v:+.1f}%")
    d0 = technicals.get("post_earnings_day0_pct")
    d1 = technicals.get("post_earnings_day1_pct")
    vr = technicals.get("earnings_day_volume_ratio")
    if d0 is not None:
        tech_lines.append(f"Post-earnings day 0: {d0:+.2f}%")
    if d1 is not None:
        tech_lines.append(f"Post-earnings day 1: {d1:+.2f}%")
    if vr is not None:
        tech_lines.append(f"Earnings day volume: {vr:.1f}× avg")

    sent_lines: list[str] = []
    reaction = market_sentiment.get("post_earnings_reaction")
    if reaction:
        sent_lines.append(f"Post-earnings reaction: {reaction.replace('_', ' ')}")
    for label, key, suffix in [
        ("Short % float", "short_pct_float", "%"),
        ("Days to cover", "days_to_cover", "d"),
        ("Inst. ownership", "inst_ownership_pct", "%"),
    ]:
        v = market_sentiment.get(key)
        if v is not None:
            sent_lines.append(f"{label}: {v}{suffix}")
    pos = market_sentiment.get("positioning")
    if pos:
        sent_lines.append(f"52W positioning: {pos.replace('_', ' ')}")
    titles = market_sentiment.get("recent_news_titles", [])
    if titles:
        sent_lines.append("Headlines: " + "; ".join(titles[:4]))

    system_prompt = (
        "You are a senior institutional equity research analyst. "
        "Write a substantive post-earnings report from structured data. "
        "Be specific with numbers. Highlight what matters for the investment case. "
        "Acknowledge data gaps honestly. Return valid JSON only — no markdown fences."
    )

    user_prompt = f"""Post-earnings analyst report for {ticker} ({company_name}).

QUARTERLY RESULTS (newest first):
{chr(10).join(q_lines) if q_lines else 'No quarterly P&L data — EPS trend may be available'}

TRAILING 12M:
{json.dumps(_clean(trailing_metrics))}

VALUATION:
{json.dumps(_clean(valuation))}

ANALYST CONSENSUS:
{json.dumps(_clean(analyst_consensus))}

RECENT ANALYST ACTIONS (last 90 days):
{chr(10).join(act_lines) if act_lines else 'None available'}

TECHNICAL ANALYSIS:
{chr(10).join(tech_lines) if tech_lines else 'No technical data available'}

MARKET SENTIMENT:
{chr(10).join(sent_lines) if sent_lines else 'No sentiment data available'}

NEXT QUARTER ESTIMATES:
{json.dumps(_clean(next_quarter)) if next_quarter else 'Not available'}

Return ONLY this JSON:
{{
  "executive_summary": "<3-4 sentences — what this print means for the investment case>",
  "headline_read": "<2-3 sentences on beat/miss magnitude and primary driver>",
  "margins_and_growth": "<2-3 sentences on margin trajectory and growth quality>",
  "guidance_read": "<2 sentences on what forward estimates signal>",
  "technical_snapshot": "<2-3 sentences on price action, trend, momentum, post-earnings reaction>",
  "market_sentiment": "<2-3 sentences on positioning, short interest, institutional ownership, and news backdrop>",
  "analyst_sentiment": "<2 sentences on analyst action backdrop>",
  "bull_case": "<2-3 sentences — bull perspective on this report>",
  "bear_case": "<2-3 sentences — bear perspective on risks>",
  "key_watch_items": ["<metric to monitor>", "<second>", "<third>"]
}}"""

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=model,
            max_tokens=2000,
            system=[{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw.strip())
        result = json.loads(raw)
        result["_llm"] = True
        result["_model"] = model
        return result
    except Exception as exc:
        logger.warning("[earnings_report] Claude synthesis failed for %s: %s", ticker, exc)
        return {"_llm": False, "error": str(exc)}


# ── public API ────────────────────────────────────────────────────────────────

def generate_report(ticker: str, force: bool = False) -> dict:
    """Generate (or return cached) a deep post-earnings analyst report."""
    ticker = ticker.upper()
    now_ts = _time.time()

    if not force and ticker in _REPORT_CACHE:
        entry = _REPORT_CACHE[ticker]
        if now_ts - entry["ts"] < _CACHE_TTL:
            data = dict(entry["data"])
            data["cached"] = True
            data["cached_at"] = datetime.fromtimestamp(entry["ts"], tz=timezone.utc).isoformat()
            return data

    import yfinance as yf
    t = yf.Ticker(ticker)

    # t.info is the most data-rich source and is confirmed working
    info: dict = _timed_fetch(lambda: t.info, f"{ticker}/info") or {}

    company_name  = info.get("longName") or info.get("shortName") or ticker
    current_price = _sf(info.get("currentPrice") or info.get("regularMarketPrice"))

    quarterly_results = _fetch_quarterly_results(t)
    analyst_actions   = _fetch_analyst_actions(t)
    next_quarter      = _fetch_next_quarter(info)
    technicals        = _fetch_technicals(t, info, quarterly_results)
    market_sentiment  = _fetch_market_sentiment(t, info, technicals)

    valuation = {
        "current_price":  current_price,
        "pe_trailing":    _sf(info.get("trailingPE")),
        "pe_forward":     _sf(info.get("forwardPE")),
        "week52_high":    _sf(info.get("fiftyTwoWeekHigh")),
        "week52_low":     _sf(info.get("fiftyTwoWeekLow")),
        "market_cap_bn":  round(info["marketCap"] / 1e9, 1) if info.get("marketCap") else None,
    }

    analyst_consensus = {
        "rating":        info.get("recommendationKey") or info.get("averageAnalystRating"),
        "target_mean":   _sf(info.get("targetMeanPrice")),
        "target_high":   _sf(info.get("targetHighPrice")),
        "target_low":    _sf(info.get("targetLowPrice")),
        "analyst_count": info.get("numberOfAnalystOpinions"),
    }
    if current_price and analyst_consensus.get("target_mean"):
        analyst_consensus["upside_to_target_pct"] = round(
            (analyst_consensus["target_mean"] - current_price) / current_price * 100, 1
        )

    trailing_metrics = {
        "revenue_bn":           round(info["totalRevenue"] / 1e9, 2) if info.get("totalRevenue") else None,
        "revenue_growth_pct":   round(info["revenueGrowth"] * 100, 1) if info.get("revenueGrowth") else None,
        "gross_margin_pct":     round(info["grossMargins"] * 100, 1) if info.get("grossMargins") else None,
        "operating_margin_pct": round(info["operatingMargins"] * 100, 1) if info.get("operatingMargins") else None,
        "net_margin_pct":       round(info["profitMargins"] * 100, 1) if info.get("profitMargins") else None,
        "eps_trailing":         _sf(info.get("trailingEps")),
        "eps_forward":          _sf(info.get("forwardEps")),
    }

    narrative = _synthesise(
        ticker=ticker,
        company_name=company_name,
        quarterly_results=quarterly_results,
        trailing_metrics=trailing_metrics,
        valuation=valuation,
        analyst_consensus=analyst_consensus,
        analyst_actions=analyst_actions,
        next_quarter=next_quarter,
        technicals=technicals,
        market_sentiment=market_sentiment,
    )

    report = {
        "ticker":                  ticker,
        "company_name":            company_name,
        "generated_at":            datetime.now(timezone.utc).isoformat(),
        "cached":                  False,
        "quarterly_results":       quarterly_results,
        "trailing_metrics":        trailing_metrics,
        "valuation":               valuation,
        "analyst_consensus":       analyst_consensus,
        "recent_analyst_actions":  analyst_actions,
        "next_quarter_estimates":  next_quarter,
        "technicals":              technicals,
        "market_sentiment":        market_sentiment,
        "narrative":               narrative,
    }

    _REPORT_CACHE[ticker] = {"ts": now_ts, "data": report}
    return report
