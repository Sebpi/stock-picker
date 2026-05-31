"""
Canonical data model for the multi-agent stock forecasting system.
All agents emit AgentSignal; the orchestrator produces InvestmentThesis.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class QualityFlag(str, Enum):
    STALE_SOURCE = "STALE_SOURCE"
    MISSING_FIELD = "MISSING_FIELD"
    SOURCE_CONFLICT = "SOURCE_CONFLICT"
    LOW_CREDIBILITY = "LOW_CREDIBILITY"
    LOW_COVERAGE = "LOW_COVERAGE"
    OUTLIER_VALUE = "OUTLIER_VALUE"
    LLM_UNVERIFIED = "LLM_UNVERIFIED"


class Direction(str, Enum):
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"
    MIXED = "mixed"


class Materiality(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Confidence(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class MacroRegime(str, Enum):
    RISK_ON = "risk_on"
    NEUTRAL = "neutral"
    RISK_OFF = "risk_off"
    RATE_PRESSURE = "rate_pressure"
    LIQUIDITY_SUPPORTIVE = "liquidity_supportive"
    RECESSION_RISK = "recession_risk"


class RiskRating(str, Enum):
    LOW = "low"
    MEDIUM_LOW = "medium_low"
    MEDIUM = "medium"
    MEDIUM_HIGH = "medium_high"
    HIGH = "high"


class EvidenceQuality(str, Enum):
    STRONG = "strong"
    MODERATE = "moderate"
    WEAK = "weak"
    INSUFFICIENT = "insufficient"


# ---------------------------------------------------------------------------
# Source credibility weights (spec §5.3)
# ---------------------------------------------------------------------------

CREDIBILITY: dict[str, float] = {
    "sec_filing": 0.95,
    "earnings_release": 0.90,
    "earnings_call": 0.82,
    "analyst_revision": 0.75,
    "financial_press": 0.67,
    "market_data": 0.60,
    "options_positioning": 0.62,
    "social": 0.30,
    "company_guidance": 0.88,
    "macro_release": 0.92,
}

# Horizon-specific agent weights (spec §5.2)
# Horizon-specific agent weights (spec §5.2)
# Note: orchestrator normalises by sum of *usable* weights, so these
# don't have to total 1.0. Insider activity weighted heaviest at 3m
# (Form 4 cluster buying is a classic 30-90 day signal) and decays
# toward 12m where fundamentals dominate.
HORIZON_WEIGHTS: dict[str, dict[str, float]] = {
    "3m": {
        "agent.technical_risk": 0.11,
        "agent.sentiment_news": 0.11,
        "agent.growth_revisions": 0.08,
        "agent.fundamentals": 0.06,
        "agent.valuation": 0.06,
        "agent.macro_liquidity": 0.05,
        "agent.insider_activity": 0.07,
        "agent.options_flow": 0.06,
        "agent.short_interest": 0.05,
        "agent.earnings_quality": 0.03,
        "agent.credit_risk": 0.06,
        "agent.institutional_flow": 0.05,
        "agent.earnings_surprise": 0.07,
        "agent.price_momentum": 0.09,
        "agent.dividend_quality": 0.03,
        "agent.capital_allocation": 0.03,
        "agent.industry_competition": 0.01,
        "agent.portfolio_risk": 0.01,
    },
    "6m": {
        "agent.technical_risk": 0.08,
        "agent.sentiment_news": 0.08,
        "agent.growth_revisions": 0.10,
        "agent.fundamentals": 0.09,
        "agent.valuation": 0.09,
        "agent.macro_liquidity": 0.06,
        "agent.insider_activity": 0.05,
        "agent.options_flow": 0.03,
        "agent.short_interest": 0.03,
        "agent.earnings_quality": 0.07,
        "agent.credit_risk": 0.05,
        "agent.institutional_flow": 0.06,
        "agent.earnings_surprise": 0.06,
        "agent.price_momentum": 0.06,
        "agent.dividend_quality": 0.05,
        "agent.capital_allocation": 0.05,
        "agent.industry_competition": 0.03,
        "agent.portfolio_risk": 0.02,
    },
    "12m": {
        "agent.technical_risk": 0.04,
        "agent.sentiment_news": 0.04,
        "agent.growth_revisions": 0.12,
        "agent.fundamentals": 0.13,
        "agent.valuation": 0.12,
        "agent.macro_liquidity": 0.07,
        "agent.insider_activity": 0.03,
        "agent.options_flow": 0.01,
        "agent.short_interest": 0.02,
        "agent.earnings_quality": 0.09,
        "agent.credit_risk": 0.04,
        "agent.institutional_flow": 0.05,
        "agent.earnings_surprise": 0.04,
        "agent.price_momentum": 0.02,
        "agent.dividend_quality": 0.07,
        "agent.capital_allocation": 0.08,
        "agent.industry_competition": 0.05,
        "agent.portfolio_risk": 0.03,
    },
}


# ---------------------------------------------------------------------------
# Evidence object
# ---------------------------------------------------------------------------

class Evidence(BaseModel):
    source_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_type: str  # one of CREDIBILITY keys
    source_name: str
    url_or_ref: str = ""
    published_at: datetime | None = None
    retrieved_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    credibility_weight: float = Field(ge=0.0, le=1.0)
    freshness_score: float = Field(default=1.0, ge=0.0, le=1.0)
    extracted_facts: list[str] = Field(default_factory=list)
    parser_version: str = "1.0"

    @classmethod
    def from_market_data(cls, ticker: str, facts: list[str]) -> "Evidence":
        return cls(
            source_type="market_data",
            source_name="yfinance",
            url_or_ref=f"yfinance://{ticker}",
            credibility_weight=CREDIBILITY["market_data"],
            extracted_facts=facts,
        )

    @classmethod
    def from_filing(cls, ticker: str, period: str, facts: list[str]) -> "Evidence":
        return cls(
            source_type="sec_filing",
            source_name="SEC EDGAR via yfinance",
            url_or_ref=f"yfinance://financials/{ticker}/{period}",
            credibility_weight=CREDIBILITY["sec_filing"],
            extracted_facts=facts,
        )

    @classmethod
    def from_analyst(cls, ticker: str, facts: list[str]) -> "Evidence":
        return cls(
            source_type="analyst_revision",
            source_name="Analyst consensus via yfinance",
            url_or_ref=f"yfinance://analyst/{ticker}",
            credibility_weight=CREDIBILITY["analyst_revision"],
            extracted_facts=facts,
        )

    @classmethod
    def from_macro(cls, source_name: str, facts: list[str]) -> "Evidence":
        return cls(
            source_type="macro_release",
            source_name=source_name,
            credibility_weight=CREDIBILITY["macro_release"],
            extracted_facts=facts,
        )


# ---------------------------------------------------------------------------
# AgentSignal envelope
# ---------------------------------------------------------------------------

class AgentSignal(BaseModel):
    schema_version: str = "1.0"
    signal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    run_id: str
    agent_id: str
    ticker: str
    as_of: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    horizon_relevance: list[str] = Field(default_factory=lambda: ["3m", "6m", "12m"])
    signal_type: str
    score: float = Field(ge=0.0, le=100.0)
    confidence: Confidence
    direction: Direction
    materiality: Materiality
    payload: dict[str, Any] = Field(default_factory=dict)
    evidence: list[Evidence] = Field(default_factory=list)
    quality_flags: list[QualityFlag] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)

    @field_validator("score")
    @classmethod
    def round_score(cls, v: float) -> float:
        return round(v, 2)

    def add_flag(self, flag: QualityFlag) -> None:
        if flag not in self.quality_flags:
            self.quality_flags.append(flag)

    def has_flag(self, flag: QualityFlag) -> bool:
        return flag in self.quality_flags

    @property
    def is_usable(self) -> bool:
        blocking = {QualityFlag.LOW_COVERAGE, QualityFlag.MISSING_FIELD}
        return not any(f in blocking for f in self.quality_flags)

    def mean_credibility(self) -> float:
        if not self.evidence:
            return 0.0
        return sum(e.credibility_weight for e in self.evidence) / len(self.evidence)


# ---------------------------------------------------------------------------
# HorizonForecast and InvestmentThesis
# ---------------------------------------------------------------------------

class HorizonForecast(BaseModel):
    base_return_pct: float
    bull_return_pct: float
    bear_return_pct: float
    confidence: float = Field(ge=0.0, le=1.0)


class DecisionLogEntry(BaseModel):
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    action: str
    reason: str
    agent_id: str = ""
    value_before: Any = None
    value_after: Any = None


class InvestmentThesis(BaseModel):
    thesis_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    run_id: str
    ticker: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    current_price: float
    composite_score: float = Field(ge=0.0, le=100.0)
    risk_rating: RiskRating
    evidence_quality: EvidenceQuality
    forecast: dict[str, HorizonForecast]  # keys: "3m", "6m", "12m"
    drivers: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    agent_scores: dict[str, float] = Field(default_factory=dict)
    agent_meta: dict[str, dict] = Field(default_factory=dict)  # per-agent {direction, confidence, flags}
    weighted_scores: dict[str, float] = Field(default_factory=dict)  # by horizon
    narrative: dict[str, str] = Field(default_factory=dict)  # bull/base/bear
    quality_flags: list[QualityFlag] = Field(default_factory=list)
    decision_log: list[DecisionLogEntry] = Field(default_factory=list)

    def overall_direction(self) -> Direction:
        score = self.composite_score
        if score >= 60:
            return Direction.POSITIVE
        elif score <= 40:
            return Direction.NEGATIVE
        return Direction.NEUTRAL

    def add_log(self, action: str, reason: str, agent_id: str = "") -> None:
        self.decision_log.append(DecisionLogEntry(action=action, reason=reason, agent_id=agent_id))


# ---------------------------------------------------------------------------
# Payload schemas for each agent (typed dicts for documentation)
# ---------------------------------------------------------------------------

class FundamentalPayload(BaseModel):
    period: str = ""
    revenue_growth_yoy: float | None = None
    eps_growth_yoy: float | None = None
    gross_margin: float | None = None
    operating_margin: float | None = None
    fcf_margin: float | None = None
    fcf_conversion: float | None = None
    debt_to_equity: float | None = None
    net_cash_or_debt: float | None = None
    share_count_change_yoy: float | None = None
    red_flags: list[str] = Field(default_factory=list)


class ValuationPayload(BaseModel):
    price: float | None = None
    market_cap: float | None = None
    enterprise_value: float | None = None
    pe_ttm: float | None = None
    forward_pe: float | None = None
    ps_ttm: float | None = None
    ev_ebitda: float | None = None
    peg: float | None = None
    p_fcf: float | None = None
    peer_percentile: float | None = None
    historical_percentile: float | None = None
    dcf_sensitivity: dict[str, float] = Field(default_factory=dict)
    valuation_commentary: list[str] = Field(default_factory=list)


class TechnicalPayload(BaseModel):
    price: float | None = None
    trend_label: str = "unknown"
    ma_20: float | None = None
    ma_50: float | None = None
    ma_200: float | None = None
    rsi_14: float | None = None
    atr_14: float | None = None
    volatility_30d: float | None = None
    drawdown_from_high: float | None = None
    support_levels: list[float] = Field(default_factory=list)
    resistance_levels: list[float] = Field(default_factory=list)
    risk_zones: list[str] = Field(default_factory=list)


class MacroPayload(BaseModel):
    macro_regime: str = "neutral"
    fed_rate: float | None = None
    ten_year_yield: float | None = None
    yield_curve_2s10s: float | None = None
    inflation_yoy: float | None = None
    unemployment_rate: float | None = None
    pmi: float | None = None
    vix: float | None = None
    usd_pressure: str = "medium"
    sector_sensitivity: dict[str, Any] = Field(default_factory=dict)
    upcoming_events: list[dict[str, Any]] = Field(default_factory=list)


class GrowthRevisionPayload(BaseModel):
    consensus_revenue_growth_next_fy: float | None = None
    consensus_eps_growth_next_fy: float | None = None
    eps_revision_30d: float | None = None
    revenue_revision_30d: float | None = None
    guidance_delta_vs_consensus: float | None = None
    revision_breadth: float | None = None
    target_price_mean: float | None = None
    target_upside_pct: float | None = None
    analyst_count: int | None = None
    catalysts: list[dict[str, Any]] = Field(default_factory=list)


class NewsSentimentPayload(BaseModel):
    events: list[dict[str, Any]] = Field(default_factory=list)
    sentiment_score_24h: float | None = None
    sentiment_score_7d: float | None = None
    narrative_shift: str = "none"


class IndustryPayload(BaseModel):
    sector: str = ""
    industry_group: str = ""
    peer_group: list[str] = Field(default_factory=list)
    relative_growth_rank: float | None = None
    relative_margin_rank: float | None = None
    competitive_position: str = "unknown"
    theme_exposures: list[str] = Field(default_factory=list)
    read_across_events: list[dict[str, Any]] = Field(default_factory=list)
    industry_risks: list[str] = Field(default_factory=list)


class PortfolioRiskPayload(BaseModel):
    portfolio_mode: str = "watchlist_only"
    sector_concentration: dict[str, float] = Field(default_factory=dict)
    theme_concentration: dict[str, float] = Field(default_factory=dict)
    ticker_weight: float | None = None
    estimated_beta: float | None = None
    correlation_to_nasdaq: float | None = None
    overlap_flags: list[str] = Field(default_factory=list)
    position_guidance: str = "moderate"
    risk_budget_commentary: list[str] = Field(default_factory=list)
