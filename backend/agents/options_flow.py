"""
agent.options_flow — Options market positioning as a near-term signal.

Reads the public options chain via yfinance for the nearest 1–3
expiry dates and computes four composite metrics:

  PCR (put/call ratio by volume)
    < 0.50 → heavy call bias, bullish
    0.50–0.80 → mild call bias
    0.80–1.10 → neutral
    1.10–1.50 → mild put bias, hedging
    > 1.50 → heavy put buying, bearish / event-driven hedge

  IV skew  (avg near-ATM put IV minus avg near-ATM call IV)
    Negative (call IV > put IV) → demand skewed bullish
    Positive but small (0–0.05) → neutral
    Large positive (> 0.10) → put protection in demand, bearish

  Volume spike
    We compare call and put volumes within the chain.
    Dominant call volume spike → bullish
    Dominant put volume spike → bearish

  IV level proxy
    All-chain average IV: very low IV after a calm period often
    precedes a breakout; elevated IV signals uncertainty/fear.

Scoring ladder (0–100):
  85  PCR < 0.50, call spike, negative skew (calls bid up)
  72  PCR 0.50–0.70, call-biased, mild negative skew
  60  PCR 0.70–0.90, slight call lean
  50  Neutral (PCR 0.90–1.10, flat skew)
  43  PCR 1.10–1.30, mild put lean
  35  PCR 1.30–1.55, put spike, positive skew
  26  PCR > 1.55, heavy put buying, sharply positive skew

Horizon relevance:
  This signal is strongest at 1–4 weeks. We only include it in the
  3m horizon with meaningful weight; it contributes weakly at 6m and
  is near-noise by 12m (weights in schemas.HORIZON_WEIGHTS).

Coverage failures:
  Stocks with no listed options, illiquid chains (< 100 total
  volume across near-term expiries), or yfinance outages emit a
  LOW_COVERAGE neutral signal and do not block thesis generation.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))
from agents import BaseAgent
from schemas import Confidence, Direction, Evidence, Materiality, QualityFlag

import yfinance as yf

logger = logging.getLogger(__name__)

# Minimum total options volume to trust the signal.
MIN_VOLUME_THRESHOLD = 100

# How many near-term expiries to scan (more = more data, slower).
MAX_EXPIRIES = 3

# Strike range around spot for "near-ATM" IV skew calculation (±10%).
ATM_BAND = 0.10


class OptionsFlowAgent(BaseAgent):
    agent_id = "agent.options_flow"
    signal_type = "options_positioning"
    default_horizons = ["3m", "6m", "12m"]

    def _run(self, ticker: str, run_id: str, as_of: datetime) -> Any:
        ticker = ticker.upper()

        raw = self._timed_fetch(
            lambda: self._fetch_options(ticker),
            f"{ticker}/options_chain",
        )

        if not raw:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={"narrative": "Options data unavailable."},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        call_vol = raw["call_volume"]
        put_vol  = raw["put_volume"]
        total_vol = call_vol + put_vol
        pcr       = raw["pcr"]
        iv_skew   = raw["iv_skew"]
        avg_iv    = raw["avg_iv"]
        expiries_used = raw["expiries_used"]

        # Insufficient liquidity — emit neutral to avoid noisy signal.
        if total_vol < MIN_VOLUME_THRESHOLD:
            return self._emit(
                ticker=ticker, run_id=run_id, as_of=as_of,
                score=50.0,
                confidence=Confidence.LOW,
                direction=Direction.NEUTRAL,
                materiality=Materiality.LOW,
                payload={**raw, "narrative": f"Illiquid options chain ({total_vol} total volume); signal suppressed."},
                quality_flags=[QualityFlag.LOW_COVERAGE],
            )

        # ── Score ────────────────────────────────────────────────────
        if pcr < 0.50:
            score, direction, materiality = 85.0, Direction.POSITIVE, Materiality.HIGH
            note = f"Very bullish options positioning: PCR {pcr:.2f}, call volume dominates."
        elif pcr < 0.70:
            score, direction, materiality = 72.0, Direction.POSITIVE, Materiality.MEDIUM
            note = f"Call-biased market: PCR {pcr:.2f}, moderate call skew."
        elif pcr < 0.90:
            score, direction, materiality = 60.0, Direction.POSITIVE, Materiality.LOW
            note = f"Slight call lean: PCR {pcr:.2f}."
        elif pcr <= 1.10:
            score, direction, materiality = 50.0, Direction.NEUTRAL, Materiality.LOW
            note = f"Neutral options flow: PCR {pcr:.2f}, balanced put/call activity."
        elif pcr <= 1.30:
            score, direction, materiality = 43.0, Direction.NEUTRAL, Materiality.LOW
            note = f"Mild put bias: PCR {pcr:.2f} — possible hedging or mild caution."
        elif pcr <= 1.55:
            score, direction, materiality = 35.0, Direction.NEGATIVE, Materiality.MEDIUM
            note = f"Put-heavy activity: PCR {pcr:.2f}, elevated downside protection demand."
        else:
            score, direction, materiality = 26.0, Direction.NEGATIVE, Materiality.HIGH
            note = f"Heavy put buying: PCR {pcr:.2f} — market pricing in significant downside."

        # Adjust for IV skew: large positive skew (puts bid up) nudges score down.
        if iv_skew > 0.10:
            score = max(score - 8.0, 10.0)
            note += f" IV skew {iv_skew:.3f} — put premiums elevated."
        elif iv_skew < -0.05:
            score = min(score + 5.0, 90.0)
            note += f" Negative skew {iv_skew:.3f} — call premiums bid up."

        # ── Confidence ───────────────────────────────────────────────
        if total_vol >= 10_000:
            confidence = Confidence.HIGH
        elif total_vol >= 1_000:
            confidence = Confidence.MEDIUM
        else:
            confidence = Confidence.LOW

        evidence = [
            Evidence(
                source_type="options_positioning",
                source_name="yfinance options chain",
                url_or_ref=f"yfinance://options/{ticker}",
                credibility_weight=0.62,
                extracted_facts=[
                    f"PCR (volume): {pcr:.2f}",
                    f"Call volume: {call_vol:,}   Put volume: {put_vol:,}",
                    f"IV skew (put−call): {iv_skew:+.3f}",
                    f"Avg near-ATM IV: {avg_iv:.1%}" if avg_iv else "IV unavailable",
                    f"Expiries analysed: {expiries_used}",
                ],
            )
        ]

        return self._emit(
            ticker=ticker, run_id=run_id, as_of=as_of,
            score=score,
            confidence=confidence,
            direction=direction,
            materiality=materiality,
            payload={
                **raw,
                "score": round(score, 2),
                "narrative": note,
            },
            evidence=evidence,
        )

    # ------------------------------------------------------------------
    # Data fetch helpers
    # ------------------------------------------------------------------

    def _fetch_options(self, ticker: str) -> dict | None:
        t = yf.Ticker(ticker)
        expiry_dates = t.options
        if not expiry_dates:
            return None

        spot = None
        try:
            info = t.fast_info
            spot = float(info.last_price or info.previous_close or 0)
        except Exception:
            pass

        call_vol = put_vol = 0
        call_oi  = put_oi  = 0
        atm_call_ivs: list[float] = []
        atm_put_ivs:  list[float] = []
        all_ivs:      list[float] = []
        expiries_used = 0

        for exp in expiry_dates[:MAX_EXPIRIES]:
            try:
                chain = t.option_chain(exp)
            except Exception:
                continue

            calls = chain.calls.copy()
            puts  = chain.puts.copy()

            # Fill missing volume with 0.
            calls["volume"] = calls["volume"].fillna(0)
            puts["volume"]  = puts["volume"].fillna(0)

            call_vol += int(calls["volume"].sum())
            put_vol  += int(puts["volume"].sum())
            call_oi  += int(calls.get("openInterest", 0).fillna(0).sum())
            put_oi   += int(puts.get("openInterest", 0).fillna(0).sum())
            expiries_used += 1

            # Near-ATM IV skew.
            if spot and spot > 0:
                lo, hi = spot * (1 - ATM_BAND), spot * (1 + ATM_BAND)
                atm_c = calls[(calls["strike"] >= lo) & (calls["strike"] <= hi)]
                atm_p = puts[(puts["strike"] >= lo) & (puts["strike"] <= hi)]
                if "impliedVolatility" in atm_c.columns:
                    atm_call_ivs.extend(atm_c["impliedVolatility"].dropna().tolist())
                if "impliedVolatility" in atm_p.columns:
                    atm_put_ivs.extend(atm_p["impliedVolatility"].dropna().tolist())

            if "impliedVolatility" in calls.columns:
                all_ivs.extend(calls["impliedVolatility"].dropna().tolist())
            if "impliedVolatility" in puts.columns:
                all_ivs.extend(puts["impliedVolatility"].dropna().tolist())

        if call_vol + put_vol == 0:
            return None

        pcr = round(put_vol / max(call_vol, 1), 3)

        avg_call_iv = sum(atm_call_ivs) / len(atm_call_ivs) if atm_call_ivs else None
        avg_put_iv  = sum(atm_put_ivs)  / len(atm_put_ivs)  if atm_put_ivs  else None
        iv_skew = round((avg_put_iv - avg_call_iv), 4) if (avg_put_iv and avg_call_iv) else 0.0
        avg_iv  = round(sum(all_ivs) / len(all_ivs), 4) if all_ivs else None

        return {
            "call_volume":    call_vol,
            "put_volume":     put_vol,
            "call_oi":        call_oi,
            "put_oi":         put_oi,
            "pcr":            pcr,
            "iv_skew":        iv_skew,
            "avg_iv":         avg_iv,
            "expiries_used":  expiries_used,
            "spot_price":     spot,
        }
