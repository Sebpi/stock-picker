"""
Phase 4: backtest harness for the Phase 1-3 BUY/SELL recommendation rules.

Replays the rules over historical predictions and yfinance price data so the
thresholds picked by hand in Phase 1-3 (BUY floor 70, vol-stop multiplier 2.5,
N_target=10, sector cap 30%, VaR cap 8%) can be validated and tuned against
realised expectancy rather than guesswork.

Public entry points:
    run_phase4_backtest(...)               — single configuration
    threshold_sensitivity_sweep(...)       — grid search across key parameters
    refit_score_to_12m_return(trades)      — re-anchor the score→return curve

The harness deliberately does NOT call the multi-agent orchestrator — it only
replays the Signals-tab engine, which is purely deterministic given a
predictions stream + price history. The alert-snapshot path (which calls
Claude for theses) is too expensive to replay over hundreds of historical
dates.

Conventions:
    * Trade record schema is documented in BACKTEST_TRADE_SCHEMA below.
    * All timestamps are UTC naive (datetime.utcnow()-compatible) for cohesion
      with the rest of main.py.
    * yfinance is called once per ticker for the full window, then sliced.
"""
from __future__ import annotations

import asyncio
import logging
import math
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Callable, Optional

import yfinance as yf

logger = logging.getLogger("stockpicker.backtest")


# ── Schema reference (informational; not enforced) ───────────────────────────
BACKTEST_TRADE_SCHEMA = {
    "ticker":          "str",
    "sector":          "str",
    "entry_date":      "YYYY-MM-DD",
    "exit_date":       "YYYY-MM-DD",
    "hold_days":       "int",
    "entry_price":     "float",
    "exit_price":      "float",
    "entry_score":     "int (0-100)",
    "entry_composite": "float (0-100)",
    "confidence":      "low|medium|high",
    "annualised_vol_pct": "float",
    "exit_trigger":    "STOP LOSS | THESIS FLIPPED | TRAIL STOP | HORIZON | END_OF_WINDOW",
    "return_pct":      "float (signed)",
    "position_value":  "float (£)",
    "pnl":             "float (£)",
}

# Mirrors orchestrator.SCORE_TO_12M_RETURN (kept here to avoid an import cycle
# during refit; recompute should write the new anchors back to orchestrator.py).
DEFAULT_SCORE_ANCHORS = [
    (0,   -30.0),
    (30,  -15.0),
    (50,  -2.0),
    (60,  4.0),
    (70,  10.0),
    (80,  18.0),
    (100, 28.0),
]


# ── Data fetching ────────────────────────────────────────────────────────────
async def _fetch_ticker_history(ticker: str, start: date, end: date) -> Optional[Any]:
    """Pull yfinance daily OHLC for a ticker over the window. Cached by caller."""
    try:
        hist = await asyncio.to_thread(
            lambda: yf.Ticker(ticker).history(start=str(start), end=str(end))
        )
        if hist is None or hist.empty:
            return None
        return hist
    except Exception as exc:
        logger.warning("backtest: history fetch failed for %s: %s", ticker, exc)
        return None


async def _fetch_ticker_sector(ticker: str) -> str:
    try:
        info = await asyncio.to_thread(lambda: yf.Ticker(ticker).info)
        return (info or {}).get("sector") or "Unknown"
    except Exception:
        return "Unknown"


def _historical_regime_ok(
    trade_date: date,
    spy_hist: Any,
    vix_hist: Any,
    dma_period: int,
    vix_max: float,
) -> tuple[bool, str]:
    """Compute the SPY-DMA + VIX gate as it would have looked on `trade_date`."""
    if spy_hist is None or spy_hist.empty:
        return True, "regime data unavailable"
    spy_dates = [d.date() for d in spy_hist.index]
    # Find the latest index ≤ trade_date
    cutoff_idx = -1
    for i, d in enumerate(spy_dates):
        if d <= trade_date:
            cutoff_idx = i
        else:
            break
    if cutoff_idx < dma_period:
        return True, "insufficient SPY history at this date"
    spy_close = float(spy_hist["Close"].iloc[cutoff_idx])
    spy_dma   = float(spy_hist["Close"].iloc[cutoff_idx - dma_period + 1: cutoff_idx + 1].mean())
    spy_ok    = spy_close >= spy_dma

    vix_ok = True
    vix_val = float("nan")
    if vix_hist is not None and not vix_hist.empty:
        vix_dates = [d.date() for d in vix_hist.index]
        # Find latest vix close ≤ trade_date
        for i in range(len(vix_dates) - 1, -1, -1):
            if vix_dates[i] <= trade_date:
                vix_val = float(vix_hist["Close"].iloc[i])
                break
        if vix_val == vix_val:  # not NaN
            vix_ok = vix_val <= vix_max

    if spy_ok and vix_ok:
        return True, f"SPY ≥ DMA, VIX={vix_val:.1f}"
    parts = []
    if not spy_ok:
        parts.append(f"SPY {spy_close:.2f} < {dma_period}-DMA {spy_dma:.2f}")
    if not vix_ok:
        parts.append(f"VIX {vix_val:.1f} > {vix_max:.0f}")
    return False, "; ".join(parts)


# ── Trade simulation ─────────────────────────────────────────────────────────
def _simulate_exit(
    ticker_hist: Any,
    entry_idx: int,
    entry_price: float,
    annualised_vol_pct: float,
    composite: float,
    later_predictions: list[dict],
    *,
    stop_multiplier: float,
    trail_stop_pct: float,
    thesis_flip_composite: float,
    max_hold_days: int,
) -> dict:
    """
    Walk forward from entry_idx through the price series, applying:
      • Vol-scaled stop loss      (Phase 1)
      • Thesis flipped trigger    (Phase 1)
      • Trailing stop from peak   (Phase 1)
      • Max hold horizon          (this phase, for backtesting only)
    Returns a dict with exit_date, exit_price, exit_trigger, hold_days, return_pct.
    """
    daily_vol = annualised_vol_pct / math.sqrt(252.0)
    stop_pct  = max(-15.0, min(-8.0, -stop_multiplier * daily_vol))
    closes    = list(ticker_hist["Close"])
    highs     = list(ticker_hist["High"])
    dates     = [d.date() for d in ticker_hist.index]

    # Index later predictions for this ticker by date
    pred_by_date = {p.get("date"): p for p in later_predictions if isinstance(p.get("date"), str)}

    peak = entry_price
    for offset in range(1, min(max_hold_days, len(closes) - entry_idx)):
        i      = entry_idx + offset
        price  = float(closes[i])
        peak   = max(peak, float(highs[i]))
        unrl   = (price / entry_price - 1.0) * 100.0

        # 1. Vol-scaled stop
        if unrl <= stop_pct:
            return {
                "exit_date":    str(dates[i]),
                "exit_price":   price,
                "exit_trigger": "STOP LOSS",
                "hold_days":    offset,
                "return_pct":   unrl,
            }

        # 2. Trailing stop (only if we're meaningfully up — match Phase 1 gate)
        if unrl > 2.0 and price < peak * (1.0 - trail_stop_pct):
            return {
                "exit_date":    str(dates[i]),
                "exit_price":   price,
                "exit_trigger": "TRAIL STOP",
                "hold_days":    offset,
                "return_pct":   unrl,
            }

        # 3. Thesis flipped (composite < threshold on a later prediction)
        pred = pred_by_date.get(str(dates[i]))
        if pred:
            comp = float((pred.get("factor_scores") or {}).get("composite") or composite)
            if comp < thesis_flip_composite:
                return {
                    "exit_date":    str(dates[i]),
                    "exit_price":   price,
                    "exit_trigger": "THESIS FLIPPED",
                    "hold_days":    offset,
                    "return_pct":   unrl,
                }

    # Held to horizon — close at last available price
    last_idx   = min(entry_idx + max_hold_days, len(closes) - 1)
    last_price = float(closes[last_idx])
    return {
        "exit_date":    str(dates[last_idx]),
        "exit_price":   last_price,
        "exit_trigger": "HORIZON" if last_idx == entry_idx + max_hold_days else "END_OF_WINDOW",
        "hold_days":    last_idx - entry_idx,
        "return_pct":   (last_price / entry_price - 1.0) * 100.0,
    }


# ── Aggregate metrics ────────────────────────────────────────────────────────
def _aggregate_metrics(trades: list[dict], initial_float: float) -> dict:
    """Win rate, expectancy, Sharpe, max drawdown — and a sector heatmap."""
    if not trades:
        return {
            "trade_count": 0,
            "win_rate": None, "expectancy_pct": None, "expectancy_per_day": None,
            "sharpe_annualised": None, "max_drawdown_pct": None,
            "by_exit_trigger": {}, "by_sector": {},
        }

    returns      = [t["return_pct"] for t in trades]
    win_count    = sum(1 for r in returns if r > 0)
    win_rate     = win_count / len(returns)
    expectancy   = statistics.mean(returns)
    avg_hold     = statistics.mean(max(1, t["hold_days"]) for t in trades)
    expectancy_per_day = expectancy / avg_hold if avg_hold > 0 else 0.0

    # Annualised Sharpe — daily return basis, assume risk-free ≈ 0.
    daily_returns = [r / max(1, t["hold_days"]) for r, t in zip(returns, trades)]
    if len(daily_returns) > 1:
        d_mean = statistics.mean(daily_returns)
        d_std  = statistics.stdev(daily_returns)
        sharpe = (d_mean / d_std * math.sqrt(252)) if d_std > 0 else None
    else:
        sharpe = None

    # Approximate equity curve in chronological order — simple compounding,
    # no overlap penalty (each trade gets its sized allocation independently).
    sorted_trades = sorted(trades, key=lambda t: t["exit_date"])
    equity = initial_float
    peak   = equity
    max_dd = 0.0
    for t in sorted_trades:
        pnl = float(t.get("pnl") or 0.0)
        equity += pnl
        peak = max(peak, equity)
        dd = (equity / peak - 1.0) * 100.0 if peak > 0 else 0.0
        max_dd = min(max_dd, dd)

    by_trigger: dict[str, dict] = defaultdict(lambda: {"count": 0, "avg_return_pct": 0.0, "total_pnl": 0.0})
    for t in trades:
        b = by_trigger[t["exit_trigger"]]
        b["count"]          += 1
        b["avg_return_pct"] += t["return_pct"]
        b["total_pnl"]      += float(t.get("pnl") or 0.0)
    for b in by_trigger.values():
        b["avg_return_pct"] = round(b["avg_return_pct"] / b["count"], 2)
        b["total_pnl"]      = round(b["total_pnl"], 2)

    by_sector: dict[str, dict] = defaultdict(lambda: {"count": 0, "avg_return_pct": 0.0})
    for t in trades:
        b = by_sector[t.get("sector") or "Unknown"]
        b["count"]          += 1
        b["avg_return_pct"] += t["return_pct"]
    for b in by_sector.values():
        b["avg_return_pct"] = round(b["avg_return_pct"] / b["count"], 2)

    return {
        "trade_count":         len(trades),
        "win_rate":            round(win_rate, 3),
        "expectancy_pct":      round(expectancy, 3),
        "expectancy_per_day":  round(expectancy_per_day, 4),
        "avg_hold_days":       round(avg_hold, 1),
        "sharpe_annualised":   round(sharpe, 2) if sharpe is not None else None,
        "max_drawdown_pct":    round(max_dd, 2),
        "final_equity":        round(equity, 2),
        "total_return_pct":    round((equity / initial_float - 1.0) * 100.0, 2),
        "by_exit_trigger":     dict(by_trigger),
        "by_sector":           dict(by_sector),
    }


# ── Main harness ─────────────────────────────────────────────────────────────
async def run_phase4_backtest(
    predictions: list[dict],
    *,
    lookback_days:        int   = 90,
    max_hold_days:        int   = 90,
    initial_float:        float = 200_000.0,
    buy_floor_score:      int   = 70,
    stop_multiplier:      float = 2.5,
    trail_stop_pct:       float = 0.08,
    thesis_flip_composite: float = 55.0,
    n_target_positions:   int   = 10,
    position_max_pct:     float = 0.12,
    sector_max_pct:       float = 0.30,
    portfolio_var_max_pct: float = 0.08,
    regime_check:         bool  = True,
    regime_dma_period:    int   = 200,
    regime_vix_max:       float = 25.0,
    cooldown_days:        int   = 21,
) -> dict:
    """
    Replay Phase 1-3 BUY/SELL rules over the provided prediction stream.

    Returns a dict with keys: config, trades, metrics, regime_blocked_dates,
    skipped_reasons (Counter), notes.
    """
    if not predictions:
        return {"config": {}, "trades": [], "metrics": _aggregate_metrics([], initial_float),
                "regime_blocked_dates": [], "skipped_reasons": {}, "notes": ["No predictions supplied."]}

    end_date   = date.today()
    start_date = end_date - timedelta(days=lookback_days)
    window_end = end_date + timedelta(days=5)  # buffer for exit lookups

    # Filter predictions to the window AND those with a composite signal
    in_window = []
    for p in predictions:
        d = p.get("date")
        if not isinstance(d, str):
            continue
        try:
            pd_ = date.fromisoformat(d)
        except ValueError:
            continue
        if start_date <= pd_ <= end_date:
            in_window.append({**p, "_pd": pd_})
    in_window.sort(key=lambda x: x["_pd"])

    # Pre-fetch SPY, VIX, and every ticker's history in parallel
    unique_tickers = sorted({p["ticker"] for p in in_window if p.get("ticker")})
    logger.info("backtest: fetching history for %d tickers over %d days", len(unique_tickers), lookback_days)

    async def _fetch_pack(ticker: str) -> tuple[str, Any, str]:
        hist, sector = await asyncio.gather(
            _fetch_ticker_history(ticker, start_date, window_end),
            _fetch_ticker_sector(ticker),
        )
        return ticker, hist, sector

    spy_task = _fetch_ticker_history("SPY", start_date - timedelta(days=regime_dma_period + 30), window_end)
    vix_task = _fetch_ticker_history("^VIX", start_date - timedelta(days=10), window_end)
    spy_hist, vix_hist, *packs = await asyncio.gather(
        spy_task, vix_task, *[_fetch_pack(t) for t in unique_tickers]
    )

    hist_map:   dict[str, Any] = {t: h for t, h, _ in packs if h is not None}
    sector_map: dict[str, str] = {t: s for t, _, s in packs}

    # ── Simulation state ─────────────────────────────────────────────────────
    base_portfolio_value = initial_float  # held constant for backtest reproducibility
    var_z_95             = 1.645
    month_vol_factor     = 1.0 / math.sqrt(12.0)
    var_budget           = portfolio_var_max_pct * base_portfolio_value

    trades:           list[dict]            = []
    open_positions:   dict[str, dict]       = {}     # ticker → entry record
    last_sell_date:   dict[str, date]       = {}     # ticker → cooldown anchor
    sector_exposure:  dict[str, float]      = defaultdict(float)
    existing_var_sq:  float                 = 0.0
    skipped_reasons:  dict[str, int]        = defaultdict(int)
    regime_blocked_dates: list[str]         = []

    # Process each prediction date in order
    by_date: dict[date, list[dict]] = defaultdict(list)
    for p in in_window:
        by_date[p["_pd"]].append(p)

    for trade_date in sorted(by_date.keys()):
        regime_ok = True
        regime_reason = "regime check disabled"
        if regime_check:
            regime_ok, regime_reason = _historical_regime_ok(
                trade_date, spy_hist, vix_hist, regime_dma_period, regime_vix_max
            )
        if not regime_ok:
            regime_blocked_dates.append(str(trade_date))
            skipped_reasons["regime_blocked"] += sum(1 for p in by_date[trade_date]
                                                    if (p.get("direction") or "") == "bullish")
            continue

        # Build today's candidate list — same gates as Phase 1-3
        for pred in by_date[trade_date]:
            ticker = pred.get("ticker")
            if not ticker:
                continue
            if ticker in open_positions:
                skipped_reasons["already_held"] += 1
                continue

            # Cooldown
            last_sell = last_sell_date.get(ticker)
            if last_sell and (trade_date - last_sell).days < cooldown_days:
                skipped_reasons["cooldown"] += 1
                continue

            direction  = pred.get("direction") or ""
            confidence = (pred.get("confidence") or "medium").lower()
            score      = pred.get("score") or 0
            composite  = float((pred.get("factor_scores") or {}).get("composite") or 50.0)
            vol_pct    = float(pred.get("annualised_vol_pct") or 30.0)

            if direction != "bullish":
                skipped_reasons["not_bullish"] += 1
                continue
            if confidence == "low":
                skipped_reasons["low_confidence"] += 1
                continue
            if score < buy_floor_score:
                skipped_reasons["below_floor"] += 1
                continue

            # Find entry price in the ticker's hist
            hist = hist_map.get(ticker)
            if hist is None:
                skipped_reasons["no_price_history"] += 1
                continue
            dates_h = [d.date() for d in hist.index]
            entry_idx = None
            for i, d in enumerate(dates_h):
                if d >= trade_date:
                    entry_idx = i
                    break
            if entry_idx is None:
                skipped_reasons["no_entry_bar"] += 1
                continue
            entry_price = float(hist["Close"].iloc[entry_idx])
            if entry_price <= 0:
                skipped_reasons["zero_entry_price"] += 1
                continue

            # Phase 3 sizing
            sec = sector_map.get(ticker) or "Unknown"
            edge = max(0.0, min(1.0, (score - buy_floor_score) / 30.0))
            conviction  = 0.7 + 0.3 * edge
            conf_factor = 1.0 if confidence == "high" else 0.75
            vol_clip    = max(15.0, min(60.0, vol_pct))
            vol_adj     = max(0.5, min(1.3, 30.0 / vol_clip))
            target_w    = min((1.0 / n_target_positions) * conviction * conf_factor * vol_adj, position_max_pct)
            position_value = target_w * base_portfolio_value

            sector_headroom = max(0.0, sector_max_pct * base_portfolio_value - sector_exposure[sec])
            if sector_headroom <= 0:
                skipped_reasons["sector_cap"] += 1
                continue
            position_value = min(position_value, sector_headroom)

            monthly_vol = (vol_pct / 100.0) * month_vol_factor
            if monthly_vol > 0:
                new_var = var_z_95 * monthly_vol * position_value
                projected = math.sqrt(existing_var_sq + new_var ** 2)
                if projected > var_budget:
                    headroom_sq = max(0.0, var_budget ** 2 - existing_var_sq)
                    if headroom_sq <= 0:
                        skipped_reasons["var_cap"] += 1
                        continue
                    max_ind = math.sqrt(headroom_sq)
                    position_value = max_ind / (var_z_95 * monthly_vol)

            if position_value < 500:
                skipped_reasons["below_minimum"] += 1
                continue

            sector_exposure[sec] += position_value
            actual_var = var_z_95 * monthly_vol * position_value if monthly_vol > 0 else 0.0
            existing_var_sq += actual_var ** 2

            open_positions[ticker] = {
                "ticker":             ticker,
                "sector":             sec,
                "entry_date":         str(trade_date),
                "entry_idx":          entry_idx,
                "entry_price":        entry_price,
                "entry_score":        int(score),
                "entry_composite":    composite,
                "confidence":         confidence,
                "annualised_vol_pct": vol_pct,
                "position_value":     position_value,
            }

    # Close every open position now
    for ticker, entry in open_positions.items():
        hist = hist_map.get(ticker)
        if hist is None:
            continue
        # Find later predictions on this ticker for the thesis-flip check
        later_preds = [p for p in in_window
                       if p.get("ticker") == ticker
                       and p.get("_pd") > date.fromisoformat(entry["entry_date"])]
        exit_info = _simulate_exit(
            hist,
            entry["entry_idx"],
            entry["entry_price"],
            entry["annualised_vol_pct"],
            entry["entry_composite"],
            later_preds,
            stop_multiplier=stop_multiplier,
            trail_stop_pct=trail_stop_pct,
            thesis_flip_composite=thesis_flip_composite,
            max_hold_days=max_hold_days,
        )
        pnl = entry["position_value"] * (exit_info["return_pct"] / 100.0)
        trades.append({
            **entry,
            "exit_date":    exit_info["exit_date"],
            "exit_price":   round(exit_info["exit_price"], 2),
            "exit_trigger": exit_info["exit_trigger"],
            "hold_days":    exit_info["hold_days"],
            "return_pct":   round(exit_info["return_pct"], 2),
            "pnl":          round(pnl, 2),
        })
        try:
            last_sell_date[ticker] = date.fromisoformat(exit_info["exit_date"])
        except ValueError:
            pass

    metrics = _aggregate_metrics(trades, initial_float)

    notes = []
    if len(in_window) < 30:
        notes.append(f"Only {len(in_window)} predictions in window — results have wide error bars.")
    if metrics["trade_count"] < 10:
        notes.append("Trade count < 10 — treat all metrics as indicative, not statistically meaningful.")
    if any(t["hold_days"] >= max_hold_days * 0.9 for t in trades):
        notes.append("Several trades closed at/near max_hold_days — consider extending the window.")

    return {
        "config": {
            "lookback_days":         lookback_days,
            "max_hold_days":         max_hold_days,
            "initial_float":         initial_float,
            "buy_floor_score":       buy_floor_score,
            "stop_multiplier":       stop_multiplier,
            "trail_stop_pct":        trail_stop_pct,
            "thesis_flip_composite": thesis_flip_composite,
            "n_target_positions":    n_target_positions,
            "position_max_pct":      position_max_pct,
            "sector_max_pct":        sector_max_pct,
            "portfolio_var_max_pct": portfolio_var_max_pct,
            "regime_check":          regime_check,
            "regime_dma_period":     regime_dma_period,
            "regime_vix_max":        regime_vix_max,
            "cooldown_days":         cooldown_days,
        },
        "trades":  trades,
        "metrics": metrics,
        "regime_blocked_dates": regime_blocked_dates,
        "skipped_reasons":      dict(skipped_reasons),
        "notes":                notes,
    }


# ── Threshold sweep ──────────────────────────────────────────────────────────
async def threshold_sensitivity_sweep(
    predictions: list[dict],
    *,
    lookback_days: int = 90,
    initial_float: float = 200_000.0,
    buy_floors:        Optional[list[int]]   = None,
    stop_multipliers:  Optional[list[float]] = None,
    n_targets:         Optional[list[int]]   = None,
) -> dict:
    """Run the harness across a parameter grid. Returns one summary row per config."""
    buy_floors       = buy_floors       or [65, 70, 75, 80]
    stop_multipliers = stop_multipliers or [2.0, 2.5, 3.0]
    n_targets        = n_targets        or [8, 10, 12]

    grid = [
        {"buy_floor_score": bf, "stop_multiplier": sm, "n_target_positions": nt}
        for bf in buy_floors for sm in stop_multipliers for nt in n_targets
    ]

    async def _one(cfg: dict) -> dict:
        result = await run_phase4_backtest(
            predictions, lookback_days=lookback_days, initial_float=initial_float, **cfg
        )
        m = result["metrics"]
        return {
            **cfg,
            "trade_count":       m["trade_count"],
            "win_rate":          m["win_rate"],
            "expectancy_pct":    m["expectancy_pct"],
            "sharpe_annualised": m["sharpe_annualised"],
            "max_drawdown_pct":  m["max_drawdown_pct"],
            "total_return_pct":  m["total_return_pct"],
        }

    rows = await asyncio.gather(*[_one(cfg) for cfg in grid])
    rows_sorted = sorted(
        rows,
        key=lambda r: (
            r["expectancy_pct"] if r["expectancy_pct"] is not None else -999,
            -(r["max_drawdown_pct"] if r["max_drawdown_pct"] is not None else 100),
        ),
        reverse=True,
    )
    best = rows_sorted[0] if rows_sorted else None

    return {
        "grid_size": len(grid),
        "best":      best,
        "rows":      rows_sorted,
        "notes":     ["Ranked by expectancy_pct desc, then min |max_drawdown_pct|."],
    }


# ── Score → return curve refit ───────────────────────────────────────────────
def refit_score_to_12m_return(trades: list[dict]) -> dict:
    """
    Re-fit the orchestrator's SCORE_TO_12M_RETURN anchor table using realised
    returns. This is intentionally simple: groups trades into score buckets,
    annualises each trade's return by hold_days, and reports per-bucket means
    so the orchestrator anchors can be updated by hand (the change is too
    consequential to apply automatically).
    """
    if not trades:
        return {
            "current_anchors": DEFAULT_SCORE_ANCHORS,
            "buckets":         {},
            "suggested_anchors": None,
            "n":               0,
            "notes":           ["No trades supplied — cannot refit."],
        }

    notes = []
    bucket_edges = [(0, 50), (50, 60), (60, 70), (70, 80), (80, 90), (90, 100)]
    buckets: dict[str, dict] = {}
    for lo, hi in bucket_edges:
        bucket_trades = [
            t for t in trades
            if lo <= t.get("entry_score", -1) < hi
            and t.get("hold_days", 0) > 0
        ]
        if not bucket_trades:
            buckets[f"{lo}-{hi}"] = {"n": 0, "mean_annualised_return_pct": None}
            continue
        annualised = [
            t["return_pct"] * (365.0 / max(1, t["hold_days"]))
            for t in bucket_trades
        ]
        buckets[f"{lo}-{hi}"] = {
            "n": len(bucket_trades),
            "mean_annualised_return_pct": round(statistics.mean(annualised), 2),
            "stdev_annualised":           round(statistics.stdev(annualised), 2) if len(annualised) > 1 else None,
            "mean_hold_days":             round(statistics.mean(t["hold_days"] for t in bucket_trades), 1),
        }

    populated = [(int((lo + hi) / 2), b["mean_annualised_return_pct"])
                 for (lo, hi), b in zip(bucket_edges, buckets.values())
                 if b["mean_annualised_return_pct"] is not None]

    suggested = None
    if len(populated) >= 3:
        # Always keep the [0, ...] and [100, ...] endpoints; interpolate the middle.
        # Hold the existing -30 / +28 endpoint tails as priors so a small dataset
        # can't move the anchors wildly.
        suggested = [DEFAULT_SCORE_ANCHORS[0]] + populated + [DEFAULT_SCORE_ANCHORS[-1]]
        suggested.sort(key=lambda x: x[0])
    else:
        notes.append("Fewer than 3 populated score buckets — keeping current anchors.")

    if len(trades) < 30:
        notes.append(f"Only {len(trades)} trades — suggested anchors are indicative; "
                     "do not commit to orchestrator.py until N ≥ 30 per bucket.")

    return {
        "current_anchors":   DEFAULT_SCORE_ANCHORS,
        "buckets":           buckets,
        "suggested_anchors": suggested,
        "n":                 len(trades),
        "notes":             notes,
    }
