"""
agent.orchestrator_thesis — Coordinates all agents, applies horizon-specific weighting,
generates bull/base/bear InvestmentThesis with Claude narrative.
"""
from __future__ import annotations

import json
import logging
import os
import statistics
import time as _time
import uuid
from datetime import datetime, timezone
from typing import Any

import db
from agents import run_all_agents
from schemas import (
    AgentSignal,
    Confidence,
    DecisionLogEntry,
    Direction,
    EvidenceQuality,
    HORIZON_WEIGHTS,
    HorizonForecast,
    InvestmentThesis,
    QualityFlag,
    RiskRating,
)

logger = logging.getLogger(__name__)

# Minimum agents required for a usable thesis
MIN_AGENTS_REQUIRED = 3

# Score → base 12m return mapping (linear interpolation between anchors)
SCORE_TO_12M_RETURN = [
    (0, -30.0),
    (30, -15.0),
    (50, -2.0),
    (60, 4.0),
    (70, 10.0),
    (80, 18.0),
    (100, 28.0),
]

ANTHROPIC_MODEL_THESIS = os.getenv("THESIS_MODEL", "claude-sonnet-4-6")
THESIS_MAX_TOKENS = int(os.getenv("THESIS_MAX_TOKENS", "2048"))


def _interpolate(score: float, table: list[tuple[float, float]]) -> float:
    if score <= table[0][0]:
        return table[0][1]
    if score >= table[-1][0]:
        return table[-1][1]
    for i in range(len(table) - 1):
        s0, r0 = table[i]
        s1, r1 = table[i + 1]
        if s0 <= score <= s1:
            frac = (score - s0) / (s1 - s0)
            return round(r0 + frac * (r1 - r0), 2)
    return 0.0


def _horizon_return(score: float, horizon: str) -> tuple[float, float, float]:
    """Returns (base, bull, bear) return % for a horizon given composite score."""
    scale = {"3m": 0.25, "6m": 0.5, "12m": 1.0}.get(horizon, 1.0)
    base_12m = _interpolate(score, SCORE_TO_12M_RETURN)
    base = round(base_12m * scale, 2)
    # Bull: +1 std dev in score (+12 pts typical) → asymmetric upside
    bull_score = min(100, score + 12)
    bull_12m = _interpolate(bull_score, SCORE_TO_12M_RETURN)
    bull = round(bull_12m * scale, 2)
    # Bear: -1 std dev → asymmetric downside
    bear_score = max(0, score - 15)
    bear_12m = _interpolate(bear_score, SCORE_TO_12M_RETURN)
    bear = round(bear_12m * scale, 2)
    return base, bull, bear


def _evidence_quality(signals: dict[str, AgentSignal], n_available: int) -> EvidenceQuality:
    if n_available < MIN_AGENTS_REQUIRED:
        return EvidenceQuality.INSUFFICIENT
    avg_cred = statistics.mean(s.mean_credibility() for s in signals.values()) if signals else 0.0
    n_flags = sum(len(s.quality_flags) for s in signals.values())
    if avg_cred >= 0.75 and n_flags <= 2 and n_available >= 6:
        return EvidenceQuality.STRONG
    if avg_cred >= 0.60 and n_available >= 4:
        return EvidenceQuality.MODERATE
    return EvidenceQuality.WEAK


def _risk_rating(score: float, vol_pct: float | None, dd_pct: float | None) -> RiskRating:
    vol = vol_pct or 30.0
    dd = abs(dd_pct or 15.0)
    risk_score = (100 - score) * 0.5 + min(vol, 80) * 0.3 + min(dd, 40) * 0.2
    if risk_score < 20:
        return RiskRating.LOW
    if risk_score < 35:
        return RiskRating.MEDIUM_LOW
    if risk_score < 50:
        return RiskRating.MEDIUM
    if risk_score < 65:
        return RiskRating.MEDIUM_HIGH
    return RiskRating.HIGH


def _weighted_score(signals: dict[str, AgentSignal], horizon: str) -> float:
    weights = HORIZON_WEIGHTS[horizon]
    total_weight = 0.0
    weighted_sum = 0.0
    for agent_id, weight in weights.items():
        if agent_id in signals and signals[agent_id].is_usable:
            weighted_sum += signals[agent_id].score * weight
            total_weight += weight
    if total_weight == 0:
        return 50.0
    # Rescale to account for missing agents
    return round(weighted_sum / total_weight, 2)


def _confidence_from_signals(signals: dict[str, AgentSignal], n_available: int) -> float:
    if n_available < MIN_AGENTS_REQUIRED:
        return 0.35
    scores = [s.score for s in signals.values() if s.is_usable]
    if not scores:
        return 0.40
    std = statistics.stdev(scores) if len(scores) > 1 else 0.0
    base = 0.45 + n_available * 0.04  # more agents → more confidence
    disagreement_penalty = min(0.15, std / 100)
    return round(min(0.80, base - disagreement_penalty), 3)


class OrchestratorAgent:
    """
    Not a BaseAgent subclass — runs all agents then synthesises the thesis.
    """

    def run_thesis(self, ticker: str, run_fresh: bool = False) -> InvestmentThesis:
        import observability
        run_id = str(uuid.uuid4())
        as_of = datetime.now(timezone.utc)
        t0 = _time.monotonic()
        try:
            return self._run_thesis_inner(ticker, run_fresh, run_id, as_of, t0, observability)
        except Exception as exc:
            duration = _time.monotonic() - t0
            observability.log_metric("thesis_run_duration_secs", duration,
                                     {"ticker": ticker, "status": "error"})
            logger.exception("[orchestrator] %s: thesis run failed after %.1fs: %s", ticker, duration, exc)
            raise

    def _run_thesis_inner(self, ticker: str, run_fresh: bool,
                          run_id: str, as_of: datetime, t0: float,
                          observability) -> InvestmentThesis:
        log: list[DecisionLogEntry] = []

        # ---- Step 1: Collect signals ----
        if run_fresh:
            signals = run_all_agents(ticker, run_id=run_id)
            log.append(DecisionLogEntry(action="agents_run", reason="Fresh run requested"))
        else:
            signals = db.get_latest_signals(ticker, max_age_hours=26)
            if len(signals) < MIN_AGENTS_REQUIRED:
                logger.info("[orchestrator] %s: only %d cached signals, running fresh", ticker, len(signals))
                signals = run_all_agents(ticker, run_id=run_id)
                log.append(DecisionLogEntry(action="agents_run", reason="Insufficient cached signals"))
            else:
                log.append(DecisionLogEntry(action="signals_cached", reason=f"{len(signals)} fresh signals found"))

        n_available = len([s for s in signals.values() if s.is_usable])
        logger.info("[orchestrator] %s: %d/%d usable signals", ticker, n_available, len(signals))

        # ---- Step 2: Quality flags ----
        thesis_flags: list[QualityFlag] = []
        if n_available < MIN_AGENTS_REQUIRED:
            thesis_flags.append(QualityFlag.LOW_COVERAGE)
        stale = [aid for aid, s in signals.items() if QualityFlag.STALE_SOURCE in s.quality_flags]
        if stale:
            thesis_flags.append(QualityFlag.STALE_SOURCE)
            log.append(DecisionLogEntry(action="stale_agents", reason=f"Stale: {stale}"))

        # ---- Step 3: Weighted scores per horizon ----
        weighted: dict[str, float] = {}
        forecasts: dict[str, HorizonForecast] = {}
        for horizon in ["3m", "6m", "12m"]:
            ws = _weighted_score(signals, horizon)
            weighted[horizon] = ws
            conf = _confidence_from_signals(signals, n_available)
            base, bull, bear = _horizon_return(ws, horizon)
            forecasts[horizon] = HorizonForecast(
                base_return_pct=base,
                bull_return_pct=bull,
                bear_return_pct=bear,
                confidence=conf,
            )

        composite = weighted.get("12m", 50.0)
        agent_scores = {aid: round(s.score, 1) for aid, s in signals.items()}
        agent_meta = {
            aid: {
                "direction": s.direction.value,
                "confidence": s.confidence.value,
                "flags": [f.value for f in s.quality_flags],
                "usable": s.is_usable,
            }
            for aid, s in signals.items()
        }

        # ---- Step 4: Supplementary data for risk rating ----
        tech_signal = signals.get("agent.technical_risk")
        vol_pct: float | None = None
        dd_pct: float | None = None
        current_price: float = 0.0
        if tech_signal:
            vol_pct = tech_signal.payload.get("volatility_30d")
            dd_pct = tech_signal.payload.get("drawdown_from_high")
            current_price = tech_signal.payload.get("price") or 0.0

        # ---- Step 5: Evidence quality and risk rating ----
        ev_quality = _evidence_quality(signals, n_available)
        risk = _risk_rating(composite, vol_pct, dd_pct)

        # ---- Step 6: Extract key drivers and risks from agent payloads ----
        drivers, risks = self._extract_drivers_risks(signals)

        # ---- Step 7: Claude narrative (one call, cached system prompt) ----
        narrative = self._generate_narrative(ticker, signals, weighted, forecasts, drivers, risks)
        if narrative.get("_llm"):
            thesis_flags.append(QualityFlag.LLM_UNVERIFIED)

        # ---- Assemble thesis ----
        thesis = InvestmentThesis(
            run_id=run_id,
            ticker=ticker,
            generated_at=as_of,
            current_price=current_price,
            composite_score=composite,
            risk_rating=risk,
            evidence_quality=ev_quality,
            forecast=forecasts,
            drivers=drivers,
            risks=risks,
            agent_scores=agent_scores,
            agent_meta=agent_meta,
            weighted_scores=weighted,
            narrative={k: v for k, v in narrative.items() if not k.startswith("_")},
            quality_flags=thesis_flags,
            decision_log=log,
        )

        # ---- Persist ----
        prev = db.get_latest_thesis(ticker)  # fetch before store so we get the old one
        db.store_thesis(thesis)
        duration = _time.monotonic() - t0
        logger.info("[orchestrator] %s: thesis stored id=%s composite=%.1f dur=%.1fs",
                    ticker, thesis.thesis_id, composite, duration)
        observability.log_metric("thesis_run_duration_secs", duration,
                                 {"ticker": ticker, "status": "ok"})
        observability.log_metric("thesis_composite_score", composite, {"ticker": ticker})
        observability.log_metric("thesis_agents_usable", float(n_available), {"ticker": ticker})

        try:
            import thesis_alerts
            thesis_alerts.check_and_alert(
                ticker=ticker,
                new_score=composite,
                new_risk=risk.value,
                prev_score=prev.composite_score if prev else None,
                prev_risk=prev.risk_rating.value if prev else None,
            )
        except Exception as exc:
            logger.debug("[orchestrator] thesis_alerts error: %s", exc)

        return thesis

    # ------------------------------------------------------------------
    # Driver / risk extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_drivers_risks(
        signals: dict[str, AgentSignal],
    ) -> tuple[list[str], list[str]]:
        drivers: list[str] = []
        risks: list[str] = []

        # Fundamentals
        fund = signals.get("agent.fundamentals")
        if fund:
            p = fund.payload
            rev = p.get("revenue_growth_yoy")
            if rev and rev > 0.10:
                drivers.append(f"Strong revenue growth ({rev*100:.0f}% YoY)")
            gm = p.get("gross_margin")
            if gm and gm > 0.45:
                drivers.append(f"High gross margins ({gm*100:.0f}%)")
            for flag in p.get("red_flags", []):
                risks.append(flag)

        # Valuation
        val = signals.get("agent.valuation")
        if val:
            p = val.payload
            mos = p.get("dcf_sensitivity", {}).get("base")
            price = p.get("price")
            if mos and price and mos > price * 1.15:
                drivers.append("Significant DCF upside (>15% margin of safety)")
            elif mos and price and mos < price * 0.85:
                risks.append("Stock trades above DCF intrinsic value")
            hist_pct = p.get("historical_percentile")
            if hist_pct and hist_pct > 80:
                risks.append(f"Valuation at {hist_pct:.0f}th historical percentile (stretched)")

        # Growth / revisions
        growth = signals.get("agent.growth_revisions")
        if growth:
            p = growth.payload
            rev_30d = p.get("eps_revision_30d")
            if rev_30d and rev_30d > 0.03:
                drivers.append(f"Positive EPS revision momentum (+{rev_30d*100:.1f}% last 30d)")
            elif rev_30d and rev_30d < -0.03:
                risks.append(f"Negative EPS revisions ({rev_30d*100:.1f}% last 30d)")
            for cat in p.get("catalysts", []):
                if cat.get("materiality") == "high":
                    drivers.append(f"Upcoming catalyst: {cat.get('name')} ({cat.get('date', '?')})")

        # Macro
        macro = signals.get("agent.macro_liquidity")
        if macro:
            p = macro.payload
            regime = p.get("macro_regime", "neutral")
            if regime in ("risk_on", "liquidity_supportive"):
                drivers.append(f"Supportive macro regime ({regime})")
            elif regime in ("risk_off", "recession_risk"):
                risks.append(f"Challenging macro regime ({regime})")

        # Sentiment
        sent = signals.get("agent.sentiment_news")
        if sent:
            p = sent.payload
            shift = p.get("narrative_shift", "none")
            if shift == "improving":
                drivers.append("Improving news sentiment trend")
            elif shift == "deteriorating":
                risks.append("Deteriorating news sentiment trend")

        # Technical
        tech = signals.get("agent.technical_risk")
        if tech:
            p = tech.payload
            trend = p.get("trend_label", "")
            if trend in ("uptrend", "breakout"):
                drivers.append(f"Positive price trend ({trend})")
            elif trend in ("downtrend", "breakdown"):
                risks.append(f"Negative price action ({trend})")
            for zone in p.get("risk_zones", []):
                risks.append(zone)

        return drivers[:6], risks[:6]

    # ------------------------------------------------------------------
    # Claude narrative generation
    # ------------------------------------------------------------------

    def _generate_narrative(
        self,
        ticker: str,
        signals: dict[str, AgentSignal],
        weighted: dict[str, float],
        forecasts: dict[str, HorizonForecast],
        drivers: list[str],
        risks: list[str],
    ) -> dict[str, str]:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            return self._rule_based_narrative(ticker, weighted, forecasts, drivers, risks)

        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)

            agent_summary = {
                aid: {"score": round(s.score, 1), "direction": s.direction.value,
                      "confidence": s.confidence.value}
                for aid, s in signals.items() if s.is_usable
            }
            forecast_summary = {
                h: {"base": f.base_return_pct, "bull": f.bull_return_pct, "bear": f.bear_return_pct}
                for h, f in forecasts.items()
            }

            system_prompt = (
                "You are an institutional equity analyst generating investment thesis narratives. "
                "You receive structured agent scores and evidence, and must write concise, factual "
                "bull/base/bear case narratives. Never invent data not provided. "
                "Always express uncertainty where evidence is limited. "
                "Return valid JSON only, no markdown."
            )

            user_prompt = f"""Generate a structured investment thesis for {ticker}.

Agent scores (0-100): {json.dumps(agent_summary)}
3/6/12m forecasts (%): {json.dumps(forecast_summary)}
Key drivers: {json.dumps(drivers)}
Key risks: {json.dumps(risks)}

Return JSON with exactly these keys:
{{
  "bull": "2-3 sentence bull case narrative referencing the strongest agents",
  "base": "2-3 sentence base case with balanced view",
  "bear": "2-3 sentence bear case citing primary risks"
}}"""

            response = client.messages.create(
                model=ANTHROPIC_MODEL_THESIS,
                max_tokens=THESIS_MAX_TOKENS,
                system=[{
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": user_prompt}],
            )
            text = response.content[0].text.strip()
            # Strip markdown if present
            text = text.replace("```json", "").replace("```", "").strip()
            parsed = json.loads(text)
            parsed["_llm"] = True
            return parsed

        except Exception as exc:
            logger.warning("[orchestrator] Claude narrative failed for %s: %s", ticker, exc)
            return self._rule_based_narrative(ticker, weighted, forecasts, drivers, risks)

    @staticmethod
    def _rule_based_narrative(
        ticker: str,
        weighted: dict[str, float],
        forecasts: dict[str, HorizonForecast],
        drivers: list[str],
        risks: list[str],
    ) -> dict[str, str]:
        score = weighted.get("12m", 50.0)
        f12 = forecasts.get("12m")
        base_ret = f12.base_return_pct if f12 else 0.0
        bull_ret = f12.bull_return_pct if f12 else 0.0
        bear_ret = f12.bear_return_pct if f12 else 0.0

        driver_str = "; ".join(drivers[:3]) if drivers else "mixed signals"
        risk_str = "; ".join(risks[:3]) if risks else "execution risk"

        bull = (
            f"{ticker} scores {score:.0f}/100 on a composite basis. "
            f"In the bull case, positive factors including {driver_str} could drive a "
            f"{bull_ret:.0f}% 12-month return. Upside catalysts and improving fundamentals "
            f"support a constructive outlook."
        )
        base = (
            f"The base case projects a {base_ret:.0f}% 12-month return for {ticker} based on "
            f"current evidence. Key drivers are {driver_str}. The thesis is subject to "
            f"risks including {risk_str}."
        )
        bear = (
            f"In the bear case, risks including {risk_str} could result in a {bear_ret:.0f}% "
            f"12-month return for {ticker}. A deterioration in any of the key assumptions "
            f"would pressure the thesis."
        )
        return {"bull": bull, "base": base, "bear": bear}
