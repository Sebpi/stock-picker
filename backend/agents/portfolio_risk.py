"""
agent.portfolio_risk — Portfolio concentration, beta, correlation and position guidance.
Works in watchlist-only mode when no real positions are available.
Scores 0-100; higher = better portfolio fit (lower risk, lower concentration).
"""
from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import yfinance as yf

import db
from agents import BaseAgent
from schemas import (
    Confidence,
    Direction,
    Evidence,
    Materiality,
    PortfolioRiskPayload,
    QualityFlag,
)

logger = logging.getLogger(__name__)

PORTFOLIO_FILE = Path(__file__).parent.parent / "portfolio.json"
CONCENTRATION_WARN_TICKER = 0.20  # >20% in one ticker → flag
CONCENTRATION_WARN_SECTOR = 0.40  # >40% in one sector → flag
CORRELATION_CROWDED = 0.85        # pairwise correlation above this → crowded trade


def _load_portfolio() -> list[dict[str, Any]]:
    try:
        if PORTFOLIO_FILE.exists():
            return json.loads(PORTFOLIO_FILE.read_text()) or []
    except Exception as exc:
        logger.debug("_load_portfolio failed: %s", exc)
    return []


def _build_holdings(transactions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Aggregate transactions into net holdings."""
    holdings: dict[str, dict[str, float]] = {}
    for tx in transactions:
        sym = tx.get("ticker", "")
        qty = float(tx.get("qty") or tx.get("quantity") or 0)
        price = float(tx.get("price") or 0)
        if not sym:
            continue
        if sym not in holdings:
            holdings[sym] = {"qty": 0.0, "cost_basis": 0.0}
        if tx.get("type", "").lower() in ("buy", "b"):
            holdings[sym]["qty"] += qty
            holdings[sym]["cost_basis"] += qty * price
        else:
            holdings[sym]["qty"] -= qty
    return {k: v for k, v in holdings.items() if v["qty"] > 0}


class PortfolioRiskAgent(BaseAgent):
    agent_id = "agent.portfolio_risk"
    signal_type = "portfolio_fit"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        transactions = _load_portfolio()
        holdings = _build_holdings(transactions)
        watchlist = self._load_watchlist()
        portfolio_mode = "positions_available" if holdings else "watchlist_only"

        # ---- Get current prices and sector for each holding ----
        universe = list(holdings.keys()) if holdings else watchlist
        if ticker not in universe:
            universe.append(ticker)

        prices: dict[str, float] = {}
        sectors: dict[str, str] = {}
        betas: dict[str, float] = {}

        for sym in universe[:20]:  # cap to avoid rate limits
            sym_ticker = yf.Ticker(sym)
            fast = self._timed_fetch(lambda st=sym_ticker: st.fast_info, f"{sym}/fast_info")
            if fast is not None:
                price = getattr(fast, "last_price", None)
                if price:
                    prices[sym] = float(price)
            try:
                full_info = db.get_ticker_info(sym) or {}
                sectors[sym] = full_info.get("sector", "")
            except Exception as exc:
                logger.debug("[%s] sector lookup failed: %s", sym, exc)
            full = self._timed_fetch(lambda st=sym_ticker: st.info, f"{sym}/info") or {}
            betas[sym] = float(full.get("beta") or 1.0)

        # ---- Portfolio value and weights ----
        port_value = 0.0
        if portfolio_mode == "positions_available":
            for sym, h in holdings.items():
                p = prices.get(sym, 0.0)
                port_value += h["qty"] * p
        else:
            # Equal-weight assumption for watchlist
            n = len(universe)
            if n > 0:
                port_value = n * 1000.0  # virtual equal weight

        ticker_value = 0.0
        if portfolio_mode == "positions_available" and ticker in holdings:
            ticker_value = holdings[ticker]["qty"] * prices.get(ticker, 0.0)
        elif portfolio_mode == "watchlist_only":
            ticker_value = 1000.0

        ticker_weight = round(ticker_value / port_value, 4) if port_value > 0 else 0.0

        # ---- Sector concentration ----
        sector_values: dict[str, float] = {}
        for sym in universe:
            sec = sectors.get(sym, "Unknown")
            val = (holdings[sym]["qty"] * prices.get(sym, 0.0)
                   if portfolio_mode == "positions_available" and sym in holdings
                   else 1000.0)
            sector_values[sec] = sector_values.get(sec, 0.0) + val

        sector_concentration = {
            s: round(v / port_value, 4) for s, v in sector_values.items()
        } if port_value > 0 else {}

        # Theme concentration (from ticker_master)
        theme_values: dict[str, float] = {}
        for sym in universe:
            sym_info = db.get_signal_for_agent(sym, "agent.industry_competition")
            themes = (sym_info.payload.get("theme_exposures", []) if sym_info else [])
            val = (holdings[sym]["qty"] * prices.get(sym, 0.0)
                   if portfolio_mode == "positions_available" and sym in holdings
                   else 1000.0)
            for th in themes:
                theme_values[th] = theme_values.get(th, 0.0) + val / max(1, len(themes))

        theme_concentration = {
            t: round(v / port_value, 4) for t, v in theme_values.items()
        } if port_value > 0 else {}

        # ---- Portfolio beta ----
        if portfolio_mode == "positions_available" and port_value > 0:
            weighted_beta = sum(
                betas.get(sym, 1.0) * (holdings[sym]["qty"] * prices.get(sym, 0.0)) / port_value
                for sym in holdings if sym in prices
            )
        else:
            weighted_beta = sum(betas.get(s, 1.0) for s in universe) / max(1, len(universe))

        # ---- NASDAQ correlation (proxy) ----
        nasdaq_corr: float | None = None
        try:
            ndx_hist = self._timed_fetch(lambda: yf.Ticker("^NDX").history(period="3mo"), "^NDX/3mo")
            tk_hist = self._timed_fetch(lambda: yf.Ticker(ticker).history(period="3mo"), f"{ticker}/3mo")
            if ndx_hist is not None and tk_hist is not None and not ndx_hist.empty and not tk_hist.empty:
                ndx_ret = ndx_hist["Close"].pct_change().dropna()
                tk_ret = tk_hist["Close"].pct_change().dropna()
                common_idx = ndx_ret.index.intersection(tk_ret.index)
                if len(common_idx) > 20:
                    import statistics
                    n_vals = [(float(ndx_ret.loc[i]), float(tk_ret.loc[i])) for i in common_idx]
                    ndx_m = statistics.mean(x[0] for x in n_vals)
                    tk_m = statistics.mean(x[1] for x in n_vals)
                    num = sum((x[0] - ndx_m) * (x[1] - tk_m) for x in n_vals)
                    denom_ndx = math.sqrt(sum((x[0] - ndx_m) ** 2 for x in n_vals))
                    denom_tk = math.sqrt(sum((x[1] - tk_m) ** 2 for x in n_vals))
                    if denom_ndx * denom_tk > 0:
                        nasdaq_corr = round(num / (denom_ndx * denom_tk), 3)
        except Exception as exc:
            logger.debug("[%s] nasdaq_corr calculation failed: %s", ticker, exc)

        # ---- Overlap flags ----
        overlap_flags: list[str] = []
        if ticker_weight > CONCENTRATION_WARN_TICKER:
            overlap_flags.append(f"High single-stock weight: {ticker_weight*100:.1f}%")
        for sec, wt in sector_concentration.items():
            if wt > CONCENTRATION_WARN_SECTOR:
                overlap_flags.append(f"Sector concentration: {sec} at {wt*100:.1f}%")

        # ---- Position guidance ----
        thesis = db.get_latest_thesis(ticker)
        thesis_score = thesis.composite_score if thesis else 50.0
        if ticker_weight > CONCENTRATION_WARN_TICKER:
            position_guidance = "avoid"
        elif any(wt > CONCENTRATION_WARN_SECTOR for wt in sector_concentration.values()):
            position_guidance = "small"
        elif thesis_score >= 70:
            position_guidance = "high_conviction_review"
        else:
            position_guidance = "moderate"

        commentary: list[str] = []
        if overlap_flags:
            commentary.extend(overlap_flags)
        if nasdaq_corr and nasdaq_corr > 0.85:
            commentary.append(f"High NASDAQ correlation ({nasdaq_corr:.2f}) — limited diversification benefit")

        # ---- Score (inverse of risk — higher = better fit) ----
        score = 100.0
        if ticker_weight > CONCENTRATION_WARN_TICKER:
            score -= 30
        if any(wt > CONCENTRATION_WARN_SECTOR for wt in sector_concentration.values()):
            score -= 20
        if nasdaq_corr and nasdaq_corr > 0.85:
            score -= 10
        if position_guidance == "avoid":
            score -= 20
        score = float(max(0, min(100, score)))

        # ---- Direction ----
        direction = (Direction.POSITIVE if score >= 65 else
                     Direction.NEGATIVE if score <= 35 else Direction.NEUTRAL)

        confidence = (Confidence.HIGH if portfolio_mode == "positions_available" else Confidence.MEDIUM)

        flags: list[QualityFlag] = []
        if portfolio_mode == "watchlist_only":
            flags.append(QualityFlag.LOW_COVERAGE)

        evidence = [
            Evidence.from_market_data(ticker, [
                f"Portfolio mode: {portfolio_mode}",
                f"Ticker weight: {ticker_weight*100:.1f}%",
                f"Portfolio beta: {weighted_beta:.2f}",
                f"NASDAQ correlation: {nasdaq_corr:.3f}" if nasdaq_corr else "NASDAQ corr: N/A",
                f"Position guidance: {position_guidance}",
            ]),
        ]

        payload = PortfolioRiskPayload(
            portfolio_mode=portfolio_mode,
            sector_concentration=sector_concentration,
            theme_concentration=theme_concentration,
            ticker_weight=ticker_weight,
            estimated_beta=round(weighted_beta, 3),
            correlation_to_nasdaq=nasdaq_corr,
            overlap_flags=overlap_flags,
            position_guidance=position_guidance,
            risk_budget_commentary=commentary,
        ).model_dump()

        return self._emit(
            ticker=ticker,
            run_id=run_id,
            as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=Materiality.HIGH if overlap_flags else Materiality.MEDIUM,
            payload=payload,
            evidence=evidence,
            quality_flags=flags,
        )

    @staticmethod
    def _load_watchlist() -> list[str]:
        try:
            wl_file = Path(__file__).parent.parent / "watchlist.json"
            if wl_file.exists():
                return json.loads(wl_file.read_text()) or []
        except Exception as exc:
            logger.debug("_load_watchlist failed: %s", exc)
        return []
