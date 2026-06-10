"""
earnings_report.py — On-demand deep post-earnings analyst report.

Fetches structured quarterly data, price-action technicals, and market
sentiment from yfinance, then synthesises a full analyst-style report
using Claude Sonnet. Results are cached in-memory for 4 hours.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# 4-hour in-memory cache keyed by ticker
_REPORT_CACHE: dict[str, dict] = {}
_CACHE_TTL = 4 * 3600


# ── helpers ───────────────────────────────────────────────────────────────────

def _sf(val: Any) -> float | None:
    try:
        f = float(val)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _fmt_period(col: Any) -> str:
    try:
        dt = col.to_pydatetime() if hasattr(col, "to_pydatetime") else (
            datetime.fromisoformat(str(col)[:10]) if isinstance(col, str) else col
        )
        return f"Q{(dt.month - 1) // 3 + 1} {dt.year}"
    except Exception:
        return str(col)[:10]


# ── data fetchers ─────────────────────────────────────────────────────────────

def _fetch_quarterly_results(t: Any) -> list[dict]:
    """Last 4 quarters: EPS (actual vs estimate) + revenue + margins."""
    eps_by_date: dict[str, dict] = {}
    try:
        eh = t.earnings_history
        if eh is not None and not eh.empty:
            for idx, row in eh.sort_index(ascending=False).head(4).iterrows():
                date_str = str(idx)[:10]
                eps_by_date[date_str] = {
                    "eps_estimate": _sf(row.get("epsEstimate") or row.get("EPS Estimate")),
                    "eps_actual":   _sf(row.get("epsActual")   or row.get("Reported EPS") or row.get("EPS Actual")),
                    "eps_surprise_pct": _sf(row.get("surprisePercent") or row.get("Surprise(%)")),
                }
    except Exception as exc:
        logger.debug("[earnings_report] earnings_history: %s", exc)

    rev_rows: list[dict] = []
    try:
        qi = t.quarterly_income_stmt
        if qi is not None and not qi.empty:
            all_cols = list(qi.columns)
            for i, col in enumerate(all_cols[:4]):
                period = _fmt_period(col)
                date_str = str(col)[:10]
                rev = _sf(qi.loc["Total Revenue",    col]) if "Total Revenue"    in qi.index else None
                gp  = _sf(qi.loc["Gross Profit",     col]) if "Gross Profit"     in qi.index else None
                op  = (
                    _sf(qi.loc["Operating Income", col]) if "Operating Income" in qi.index else
                    _sf(qi.loc["EBIT",             col]) if "EBIT"             in qi.index else None
                )
                ni  = _sf(qi.loc["Net Income",       col]) if "Net Income"       in qi.index else None

                prev_rev = (
                    _sf(qi.loc["Total Revenue", all_cols[i + 4]])
                    if (i + 4 < len(all_cols) and "Total Revenue" in qi.index)
                    else None
                )
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

                # Attach EPS by date proximity (≤45 days)
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
        logger.debug("[earnings_report] quarterly_income_stmt: %s", exc)

    if rev_rows:
        return rev_rows

    # Fallback: EPS-only rows
    results = []
    for date_str, eps in sorted(eps_by_date.items(), reverse=True)[:4]:
        try:
            dt = datetime.fromisoformat(date_str)
            period = f"Q{(dt.month-1)//3+1} {dt.year}"
        except Exception:
            period = date_str
        e, a = eps.get("eps_estimate"), eps.get("eps_actual")
        results.append({
            "period": period, "date": date_str,
            **eps,
            "beat": (a >= e if (a is not None and e is not None) else None),
        })
    return results


def _fetch_analyst_actions(t: Any, days: int = 90) -> list[dict]:
    actions: list[dict] = []
    try:
        ud = t.upgrades_downgrades
        if ud is None or ud.empty:
            return actions
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
        logger.debug("[earnings_report] upgrades_downgrades: %s", exc)
    return actions


def _fetch_next_quarter(t: Any) -> dict:
    est: dict = {}
    try:
        cal = t.calendar or {}
        if isinstance(cal, dict):
            if "Earnings Date" in cal:
                dates = cal["Earnings Date"]
                if dates:
                    est["date"] = str(dates[0])[:10]
            for k in ("Earnings High", "Earnings Low", "Earnings Average"):
                if k in cal:
                    est[k.lower().replace(" ", "_")] = _sf(cal[k])
            for k in ("Revenue High", "Revenue Low", "Revenue Average"):
                if k in cal and cal[k]:
                    v = _sf(cal[k])
                    est[k.lower().replace(" ", "_") + "_bn"] = round(v / 1e9, 2) if v else None
    except Exception as exc:
        logger.debug("[earnings_report] calendar: %s", exc)

    try:
        ee = t.earnings_estimate
        if ee is not None and not ee.empty:
            row = ee.loc["0q"] if "0q" in ee.index else ee.iloc[0]
            est["eps_estimate_low"]  = _sf(row.get("low")  or row.get("Low"))
            est["eps_estimate_high"] = _sf(row.get("high") or row.get("High"))
            est["eps_estimate_mean"] = _sf(row.get("avg")  or row.get("Average"))
    except Exception as exc:
        logger.debug("[earnings_report] earnings_estimate: %s", exc)

    try:
        re_df = t.revenue_estimate
        if re_df is not None and not re_df.empty:
            row = re_df.loc["0q"] if "0q" in re_df.index else re_df.iloc[0]
            for src_key, est_key in [("low", "revenue_low_bn"), ("high", "revenue_high_bn"), ("avg", "revenue_mean_bn")]:
                v = _sf(row.get(src_key) or row.get(src_key.capitalize()))
                if v:
                    est.setdefault(est_key, round(v / 1e9, 2))
    except Exception as exc:
        logger.debug("[earnings_report] revenue_estimate: %s", exc)

    return est


def _fetch_technicals(t: Any, info: dict, quarterly_results: list[dict]) -> dict:
    """
    Compute price-action technicals from 1-year daily history:
      - Post-earnings reaction (day 0, day 1)
      - 3-month and 1-month returns
      - Price vs 20/50/200 DMA
      - Volume ratio on earnings day
      - RSI-14, 52-week position
    """
    tech: dict = {}
    try:
        hist = t.history(period="1y")
        if hist is None or hist.empty:
            return tech

        closes = hist["Close"]
        volumes = hist.get("Volume", None)
        dates = [str(d)[:10] for d in hist.index]

        # Current price
        current = _sf(closes.iloc[-1])
        if current is None:
            return tech
        tech["current_price"] = round(current, 2)

        # 52-week range position
        high52 = float(closes.max())
        low52  = float(closes.min())
        if high52 > low52:
            tech["week52_position_pct"] = round((current - low52) / (high52 - low52) * 100, 1)

        # Moving averages
        for period_days, key in [(20, "ma20"), (50, "ma50"), (200, "ma200")]:
            if len(closes) >= period_days:
                ma = float(closes.iloc[-period_days:].mean())
                tech[key] = round(ma, 2)
                tech[f"{key}_vs_price"] = round((current - ma) / ma * 100, 1) if ma > 0 else None

        # 1-month and 3-month returns
        for lookback, key in [(21, "return_1m_pct"), (63, "return_3m_pct")]:
            if len(closes) >= lookback:
                past = float(closes.iloc[-lookback])
                tech[key] = round((current - past) / past * 100, 1) if past > 0 else None

        # RSI-14
        if len(closes) >= 15:
            try:
                deltas = closes.diff().dropna().iloc[-14:]
                gains  = deltas.clip(lower=0).mean()
                losses = (-deltas.clip(upper=0)).mean()
                if losses > 0:
                    rs = gains / losses
                    tech["rsi_14"] = round(100 - 100 / (1 + float(rs)), 1)
                else:
                    tech["rsi_14"] = 100.0
            except Exception:
                pass

        # Volume analysis
        if volumes is not None and not volumes.empty:
            avg_vol_30 = float(volumes.iloc[-30:].mean()) if len(volumes) >= 30 else float(volumes.mean())
            tech["avg_volume_30d"] = int(avg_vol_30)

        # Post-earnings reaction — find the most recent earnings date
        earnings_date_str: str | None = None
        if quarterly_results:
            earnings_date_str = quarterly_results[0].get("date")

        if earnings_date_str and earnings_date_str in dates:
            idx = dates.index(earnings_date_str)
            pre = float(closes.iloc[idx - 1]) if idx >= 1 else None
            day0 = float(closes.iloc[idx])
            day1 = float(closes.iloc[idx + 1]) if idx + 1 < len(closes) else None

            if pre and pre > 0:
                tech["post_earnings_day0_pct"] = round((day0 - pre) / pre * 100, 2)
            if pre and day1 and pre > 0:
                tech["post_earnings_day1_pct"] = round((day1 - pre) / pre * 100, 2)

            if volumes is not None and avg_vol_30 > 0:
                try:
                    earn_vol = float(volumes.iloc[idx])
                    tech["earnings_day_volume_ratio"] = round(earn_vol / avg_vol_30, 2)
                except Exception:
                    pass

        # Trend label
        ma50 = tech.get("ma50")
        ma200 = tech.get("ma200")
        if ma50 and ma200:
            if current > ma50 > ma200:
                tech["trend"] = "uptrend"
            elif current < ma50 < ma200:
                tech["trend"] = "downtrend"
            elif current > ma200:
                tech["trend"] = "above_200d"
            else:
                tech["trend"] = "below_200d"

    except Exception as exc:
        logger.debug("[earnings_report] technicals: %s", exc)

    return tech


def _fetch_market_sentiment(t: Any, info: dict, technicals: dict) -> dict:
    """
    Aggregate a market-sentiment view from:
      - News headline sentiment (yfinance news titles)
      - Short interest
      - Institutional ownership
      - Options put/call ratio if available
    """
    sentiment: dict = {}
    try:
        # yfinance news titles
        news = t.news or []
        titles = [n.get("title", "") for n in news[:10] if n.get("title")]
        sentiment["recent_news_titles"] = titles
        sentiment["news_count"] = len(titles)
    except Exception as exc:
        logger.debug("[earnings_report] news fetch: %s", exc)

    # Aggregate fields from info
    for key, dest in [
        ("shortPercentOfFloat", "short_pct_float"),
        ("shortRatio",          "days_to_cover"),
        ("institutionsPercentHeld", "inst_ownership_pct"),
    ]:
        v = _sf(info.get(key))
        if v is not None:
            sentiment[dest] = round(v * 100, 1) if key != "shortRatio" else round(v, 1)

    # Post-earnings price signal
    day0 = technicals.get("post_earnings_day0_pct")
    day1 = technicals.get("post_earnings_day1_pct")
    if day0 is not None:
        if day0 >= 5:
            sentiment["post_earnings_reaction"] = "strong_positive"
        elif day0 >= 2:
            sentiment["post_earnings_reaction"] = "positive"
        elif day0 <= -5:
            sentiment["post_earnings_reaction"] = "strong_negative"
        elif day0 <= -2:
            sentiment["post_earnings_reaction"] = "negative"
        else:
            sentiment["post_earnings_reaction"] = "muted"

    # 52-week position context
    pos = technicals.get("week52_position_pct")
    if pos is not None:
        if pos >= 80:
            sentiment["positioning"] = "near_52w_high"
        elif pos <= 20:
            sentiment["positioning"] = "near_52w_low"
        else:
            sentiment["positioning"] = "mid_range"

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

    # Compact quarterly summary
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
        gm = q.get("gross_margin_pct")
        op = q.get("operating_margin_pct")
        if gm is not None:
            parts.append(f"GM {gm:.1f}%")
        if op is not None:
            parts.append(f"OpMgn {op:.1f}%")
        q_lines.append(" | ".join(parts))

    act_lines = []
    for a in analyst_actions[:10]:
        line = f"{a['date']} {a['firm']}: {a['action']}"
        if a.get("from_grade") and a.get("to_grade"):
            line += f" {a['from_grade']} → {a['to_grade']}"
        act_lines.append(line)

    # Technical summary
    tech_lines = []
    trend = technicals.get("trend")
    if trend:
        tech_lines.append(f"Trend: {trend.replace('_', ' ')}")
    r1m = technicals.get("return_1m_pct")
    r3m = technicals.get("return_3m_pct")
    if r1m is not None:
        tech_lines.append(f"1M return: {r1m:+.1f}%")
    if r3m is not None:
        tech_lines.append(f"3M return: {r3m:+.1f}%")
    rsi = technicals.get("rsi_14")
    if rsi is not None:
        tech_lines.append(f"RSI-14: {rsi:.0f} ({'overbought' if rsi > 70 else 'oversold' if rsi < 30 else 'neutral'})")
    pos52 = technicals.get("week52_position_pct")
    if pos52 is not None:
        tech_lines.append(f"52W position: {pos52:.0f}%ile")
    for ma_key in ("ma20_vs_price", "ma50_vs_price", "ma200_vs_price"):
        v = technicals.get(ma_key)
        if v is not None:
            label = ma_key.replace("_vs_price", "").upper()
            tech_lines.append(f"Price vs {label}: {v:+.1f}%")
    d0 = technicals.get("post_earnings_day0_pct")
    d1 = technicals.get("post_earnings_day1_pct")
    vol_ratio = technicals.get("earnings_day_volume_ratio")
    if d0 is not None:
        tech_lines.append(f"Post-earnings day 0: {d0:+.2f}%")
    if d1 is not None:
        tech_lines.append(f"Post-earnings day 1: {d1:+.2f}%")
    if vol_ratio is not None:
        tech_lines.append(f"Earnings day volume: {vol_ratio:.1f}× avg")

    # Sentiment summary
    sent_lines = []
    reaction = market_sentiment.get("post_earnings_reaction")
    if reaction:
        sent_lines.append(f"Post-earnings market reaction: {reaction.replace('_', ' ')}")
    short_pct = market_sentiment.get("short_pct_float")
    if short_pct is not None:
        sent_lines.append(f"Short interest: {short_pct:.1f}% of float")
    dtc = market_sentiment.get("days_to_cover")
    if dtc is not None:
        sent_lines.append(f"Days to cover: {dtc:.1f}")
    inst_pct = market_sentiment.get("inst_ownership_pct")
    if inst_pct is not None:
        sent_lines.append(f"Institutional ownership: {inst_pct:.1f}%")
    pos = market_sentiment.get("positioning")
    if pos:
        sent_lines.append(f"52-week positioning: {pos.replace('_', ' ')}")
    news_titles = market_sentiment.get("recent_news_titles", [])
    if news_titles:
        sent_lines.append(f"Recent news ({len(news_titles)} headlines): " + "; ".join(news_titles[:5]))

    system = (
        "You are a senior institutional equity research analyst. "
        "Write a substantive post-earnings report covering fundamentals, "
        "technical analysis, and market sentiment. Be specific with numbers. "
        "Surface what matters for the investment case. "
        "Acknowledge gaps where data is unavailable. "
        "Return valid JSON only — no markdown fences."
    )

    user = f"""Post-earnings analyst report for {ticker} ({company_name}).

QUARTERLY RESULTS (newest first):
{chr(10).join(q_lines) if q_lines else 'No quarterly data available'}

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

Return ONLY this JSON (no extra keys, no markdown):
{{
  "executive_summary": "<3-4 sentences — what this print means for the investment case. Lead with the most important signal.>",
  "headline_read": "<2-3 sentences on beat/miss magnitude, primary revenue driver, and whether quality was high or low.>",
  "margins_and_growth": "<2-3 sentences on margin trajectory, mix shift, and sustainability of growth rate.>",
  "guidance_read": "<2 sentences interpreting forward guidance or estimates. Is the bar rising or falling?>",
  "technical_snapshot": "<2-3 sentences on price action, trend, momentum, key technical levels, and post-earnings market reaction.>",
  "market_sentiment": "<2-3 sentences on positioning, short interest, institutional ownership, and how the street is positioned into the next print.>",
  "analyst_sentiment": "<2 sentences synthesising the analyst action backdrop — are upgrades accelerating, targets rising, or consensus lagging?>",
  "bull_case": "<2-3 sentences — what a bull sees in this earnings report and technical setup.>",
  "bear_case": "<2-3 sentences — what a bear sees: execution risk, valuation, technical breakdown, or deteriorating fundamentals.>",
  "key_watch_items": ["<specific metric or catalyst to monitor next quarter>", "<second>", "<third>"]
}}"""

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=model,
            max_tokens=2000,
            system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user}],
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
    info: dict = {}
    try:
        info = t.info or {}
    except Exception as exc:
        logger.debug("[earnings_report] info for %s: %s", ticker, exc)

    company_name = info.get("longName") or info.get("shortName") or ticker
    current_price = _sf(info.get("currentPrice") or info.get("regularMarketPrice"))

    quarterly_results = _fetch_quarterly_results(t)
    analyst_actions   = _fetch_analyst_actions(t)
    next_quarter      = _fetch_next_quarter(t)
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
