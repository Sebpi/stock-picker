"""
agent.growth_revisions — Analyst revision momentum, guidance deltas and catalyst calendar.
Scores 0-100; higher = stronger forward growth expectations and positive revision trend.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import yfinance as yf

import db
from agents import BaseAgent
from schemas import (
    Confidence,
    Direction,
    Evidence,
    GrowthRevisionPayload,
    Materiality,
    QualityFlag,
)

logger = logging.getLogger(__name__)


class GrowthRevisionsAgent(BaseAgent):
    agent_id = "agent.growth_revisions"
    signal_type = "growth_revision_momentum"

    # ------------------------------------------------------------------
    # Scoring rubric
    # ------------------------------------------------------------------

    @staticmethod
    def _score_revenue_growth(growth: float | None) -> int:
        if growth is None:
            return 8
        if growth > 0.20:
            return 30
        if growth > 0.10:
            return 22
        if growth > 0.05:
            return 15
        if growth >= 0:
            return 8
        return 0

    @staticmethod
    def _score_eps_revision_30d(rev: float | None) -> int:
        if rev is None:
            return 12
        if rev > 0.05:
            return 25
        if rev > 0.01:
            return 18
        if rev >= -0.01:
            return 12
        if rev >= -0.05:
            return 5
        return 0

    @staticmethod
    def _score_revision_breadth(breadth: float | None) -> int:
        if breadth is None:
            return 12
        if breadth > 0.60:
            return 25
        if breadth > 0.40:
            return 18
        if breadth > 0.20:
            return 12
        if breadth >= 0:
            return 5
        return 0

    @staticmethod
    def _score_guidance(guidance_delta: float | None) -> int:
        if guidance_delta is None:
            return 10
        if guidance_delta > 0.01:
            return 20
        if guidance_delta >= -0.01:
            return 10
        return 0

    # ------------------------------------------------------------------
    # Core run
    # ------------------------------------------------------------------

    def _run(self, ticker: str, run_id: str, as_of: datetime):
        t = yf.Ticker(ticker)
        info = self._timed_fetch(lambda: t.info, f"{ticker}/info") or {}

        # ---- Analyst consensus ----
        target_mean = self._safe_get(info, "targetMeanPrice")
        target_high = self._safe_get(info, "targetHighPrice")
        target_low = self._safe_get(info, "targetLowPrice")
        analyst_count = self._safe_get(info, "numberOfAnalystOpinions")
        current_price = self._safe_get(info, "currentPrice") or self._safe_get(info, "regularMarketPrice")

        target_upside: float | None = None
        if target_mean and current_price and current_price > 0:
            target_upside = (target_mean - current_price) / current_price * 100

        # ---- Forward growth estimates ----
        rev_growth_next_fy: float | None = None
        eps_growth_next_fy: float | None = None
        rev_est = self._timed_fetch(lambda: t.revenue_estimate, f"{ticker}/revenue_estimate")
        if rev_est is not None and not rev_est.empty and "+1y" in rev_est.index:
            try:
                growth_col = [c for c in rev_est.columns if "growth" in str(c).lower()]
                if growth_col:
                    val = rev_est.loc["+1y", growth_col[0]]
                    if val and val == val:
                        rev_growth_next_fy = float(val)
            except Exception as exc:
                logger.debug("[%s] revenue_estimate parse error: %s", ticker, exc)

        eps_est = self._timed_fetch(lambda: t.earnings_estimate, f"{ticker}/earnings_estimate")
        if eps_est is not None and not eps_est.empty and "+1y" in eps_est.index:
            try:
                growth_col = [c for c in eps_est.columns if "growth" in str(c).lower()]
                if growth_col:
                    val = eps_est.loc["+1y", growth_col[0]]
                    if val and val == val:
                        eps_growth_next_fy = float(val)
            except Exception as exc:
                logger.debug("[%s] earnings_estimate parse error: %s", ticker, exc)

        # Fall back to info fields
        if rev_growth_next_fy is None:
            rev_growth_next_fy = self._safe_get(info, "revenueGrowth")
        if eps_growth_next_fy is None:
            eps_growth_next_fy = self._safe_get(info, "earningsGrowth")

        # ---- Revision momentum ----
        eps_revision_30d: float | None = None
        revenue_revision_30d: float | None = None
        revision_breadth: float | None = None

        rev_hist = self._timed_fetch(lambda: t.eps_revisions, f"{ticker}/eps_revisions")
        if rev_hist is not None and not rev_hist.empty:
            try:
                row_key = "+1y" if "+1y" in rev_hist.index else (rev_hist.index[0] if not rev_hist.empty else None)
                if row_key:
                    up_30 = self._safe_get(dict(rev_hist.loc[row_key]), "upLast30days")
                    dn_30 = self._safe_get(dict(rev_hist.loc[row_key]), "downLast30days")
                    if up_30 is not None and dn_30 is not None:
                        total = up_30 + dn_30
                        if total > 0:
                            revision_breadth = (up_30 - dn_30) / total
            except Exception as exc:
                logger.debug("[%s] eps_revisions parse error: %s", ticker, exc)

        # Snapshot today's consensus for future 30d revision calc
        today_eps = eps_growth_next_fy
        today_rev = rev_growth_next_fy
        db.snapshot_consensus(ticker, today_eps, today_rev, target_mean, analyst_count)

        # Compare vs 30 days ago for revision momentum
        old_consensus = db.get_consensus_n_days_ago(ticker, 30)
        if old_consensus and today_eps is not None and old_consensus.get("eps_consensus") is not None:
            old_eps = old_consensus["eps_consensus"]
            if old_eps != 0:
                eps_revision_30d = (today_eps - old_eps) / abs(old_eps)
        if old_consensus and today_rev is not None and old_consensus.get("revenue_consensus") is not None:
            old_rev = old_consensus["revenue_consensus"]
            if old_rev != 0:
                revenue_revision_30d = (today_rev - old_rev) / abs(old_rev)

        # ---- Guidance delta ----
        guidance_delta: float | None = None
        earnings_hist = self._timed_fetch(lambda: t.earnings_history, f"{ticker}/earnings_history")
        if earnings_hist is not None and not earnings_hist.empty:
            try:
                surp_col = [c for c in earnings_hist.columns if "surprise" in str(c).lower()]
                if surp_col:
                    surprises = earnings_hist[surp_col[0]].dropna().tolist()
                    if surprises:
                        guidance_delta = float(surprises[-1]) if surprises[-1] == surprises[-1] else None
            except Exception as exc:
                logger.debug("[%s] earnings_history parse error: %s", ticker, exc)

        # ---- Catalyst calendar ----
        catalysts: list[dict[str, Any]] = []
        cal = self._timed_fetch(lambda: t.earnings_dates, f"{ticker}/earnings_dates")
        if cal is not None and not cal.empty:
            try:
                next_earnings = cal[cal.index > datetime.now(timezone.utc)].head(1)
                if not next_earnings.empty:
                    earn_date = next_earnings.index[0]
                    date_str = earn_date.strftime("%Y-%m-%d") if hasattr(earn_date, "strftime") else str(earn_date)[:10]
                    catalysts.append({
                        "name": "Earnings release",
                        "date": date_str,
                        "materiality": "high",
                    })
            except Exception as exc:
                logger.debug("[%s] earnings_dates parse error: %s", ticker, exc)

        # User-defined catalysts file (optional override per ticker)
        try:
            import json
            from pathlib import Path
            cat_file = Path(__file__).parent.parent / "catalysts.json"
            if cat_file.exists():
                all_cats = json.loads(cat_file.read_text())
                catalysts.extend(all_cats.get(ticker, []))
        except Exception:
            pass

        # ---- Score ----
        score = (
            self._score_revenue_growth(rev_growth_next_fy)
            + self._score_eps_revision_30d(eps_revision_30d)
            + self._score_revision_breadth(revision_breadth)
            + self._score_guidance(guidance_delta)
        )
        score = float(min(100, max(0, score)))

        # ---- Analyst recommendations fallback ----
        # When estimate data is sparse, use buy/sell/hold counts as a revision-breadth proxy
        if revision_breadth is None or analyst_count is None or analyst_count < 2:
            try:
                rec_summary = self._timed_fetch(lambda: t.recommendations_summary, f"{ticker}/rec_summary")
                if rec_summary is not None and not rec_summary.empty:
                    latest = rec_summary.iloc[0]
                    strong_buy = float(latest.get("strongBuy", 0) or 0)
                    buy        = float(latest.get("buy", 0) or 0)
                    hold       = float(latest.get("hold", 0) or 0)
                    sell       = float(latest.get("sell", 0) or 0)
                    strong_sell= float(latest.get("strongSell", 0) or 0)
                    total_rec  = strong_buy + buy + hold + sell + strong_sell
                    if total_rec > 0:
                        if analyst_count is None or analyst_count < 2:
                            analyst_count = int(total_rec)
                        if revision_breadth is None:
                            bullish = strong_buy + buy
                            bearish = sell + strong_sell
                            revision_breadth = (bullish - bearish) / total_rec
            except Exception as exc:
                logger.debug("[%s] recommendations_summary parse error: %s", ticker, exc)

        # ---- Flags ----
        flags: list[QualityFlag] = []
        # Only flag LOW_COVERAGE for truly uncovered stocks (0-1 analysts)
        if analyst_count is not None and analyst_count < 2:
            flags.append(QualityFlag.LOW_COVERAGE)
        if rev_growth_next_fy is None and eps_growth_next_fy is None:
            flags.append(QualityFlag.MISSING_FIELD)

        # ---- Direction ----
        if score >= 60:
            direction = Direction.POSITIVE
        elif score <= 35:
            direction = Direction.NEGATIVE
        else:
            direction = Direction.NEUTRAL

        # ---- Confidence ----
        fields = sum(v is not None for v in [rev_growth_next_fy, eps_revision_30d, revision_breadth, target_upside])
        confidence = Confidence.HIGH if fields >= 3 else (Confidence.MEDIUM if fields >= 2 else Confidence.LOW)

        evidence = [
            Evidence.from_analyst(ticker, [
                f"Analysts: {analyst_count}" if analyst_count else "Analyst count: N/A",
                f"Target price (mean): {target_mean:.2f}" if target_mean else "Target: N/A",
                f"Target upside: {target_upside:.1f}%" if target_upside else "Upside: N/A",
                f"Fwd revenue growth: {rev_growth_next_fy*100:.1f}%" if rev_growth_next_fy else "Rev growth: N/A",
                f"EPS revision 30d: {eps_revision_30d*100:.1f}%" if eps_revision_30d else "EPS revision: N/A",
                f"Revision breadth: {revision_breadth:.2f}" if revision_breadth else "Breadth: N/A",
            ]),
        ]

        payload = GrowthRevisionPayload(
            consensus_revenue_growth_next_fy=rev_growth_next_fy,
            consensus_eps_growth_next_fy=eps_growth_next_fy,
            eps_revision_30d=eps_revision_30d,
            revenue_revision_30d=revenue_revision_30d,
            guidance_delta_vs_consensus=guidance_delta,
            revision_breadth=revision_breadth,
            target_price_mean=target_mean,
            target_upside_pct=target_upside,
            analyst_count=int(analyst_count) if analyst_count else None,
            catalysts=catalysts,
        ).model_dump()

        return self._emit(
            ticker=ticker,
            run_id=run_id,
            as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=Materiality.HIGH if catalysts else Materiality.MEDIUM,
            payload=payload,
            evidence=evidence,
            quality_flags=flags,
        )
