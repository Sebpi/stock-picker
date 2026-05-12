"""
thesis_pdf.py — Render an InvestmentThesis as a clean PDF using reportlab.
"""
from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

from schemas import InvestmentThesis

# ── Palette ────────────────────────────────────────────────────────────────
_BG      = colors.HexColor("#0d1117")
_SURFACE = colors.HexColor("#161b22")
_ACCENT  = colors.HexColor("#3b82f6")
_GREEN   = colors.HexColor("#22c55e")
_RED     = colors.HexColor("#ef4444")
_YELLOW  = colors.HexColor("#eab308")
_TEXT    = colors.HexColor("#111827")
_MUTED   = colors.HexColor("#4b5563")
_WHITE   = colors.white


def _score_color(score: float) -> colors.Color:
    if score >= 65:
        return _GREEN
    if score >= 45:
        return _YELLOW
    return _RED


def _risk_color(rating: str) -> colors.Color:
    r = (rating or "").lower()
    if r in ("low", "medium_low"):
        return _GREEN
    if r in ("medium",):
        return _YELLOW
    return _RED


def _quality_color(eq: str) -> colors.Color:
    e = (eq or "").lower()
    if e == "strong":
        return _GREEN
    if e == "moderate":
        return _YELLOW
    return _RED


def build_pdf(thesis: InvestmentThesis) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )

    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("H1", parent=styles["Heading1"],
                         textColor=_TEXT, fontSize=18, spaceAfter=4)
    h2 = ParagraphStyle("H2", parent=styles["Heading2"],
                         textColor=_ACCENT, fontSize=12, spaceBefore=14, spaceAfter=4)
    body = ParagraphStyle("Body", parent=styles["Normal"],
                          textColor=_TEXT, fontSize=10, leading=14)
    muted = ParagraphStyle("Muted", parent=styles["Normal"],
                           textColor=_MUTED, fontSize=9, leading=12)
    label = ParagraphStyle("Label", parent=styles["Normal"],
                           textColor=_MUTED, fontSize=7, spaceAfter=1)

    story: list[Any] = []

    # ── Header ────────────────────────────────────────────────────────────
    generated = thesis.generated_at.strftime("%d %b %Y %H:%M UTC") if thesis.generated_at else "-"
    story.append(Paragraph(f"Investment Thesis — {thesis.ticker}", h1))
    story.append(Paragraph(f"Generated {generated}", muted))
    story.append(Spacer(1, 0.3 * cm))
    story.append(HRFlowable(width="100%", color=_ACCENT, thickness=1))
    story.append(Spacer(1, 0.4 * cm))

    # ── Summary row ───────────────────────────────────────────────────────
    score = thesis.composite_score
    summary_data = [
        [
            _cell("Composite Score", str(round(score, 1)), _score_color(score), styles),
            _cell("Risk", _fmt(thesis.risk_rating), _risk_color(str(thesis.risk_rating)), styles),
            _cell("Evidence", _fmt(thesis.evidence_quality), _quality_color(str(thesis.evidence_quality)), styles),
            _cell("Price", f"${thesis.current_price:,.2f}" if thesis.current_price else "-", _TEXT, styles),
        ]
    ]
    t = Table(summary_data, colWidths=["25%", "25%", "25%", "25%"])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f9fafb")),
        ("BOX", (0, 0), (-1, -1), 0.5, _ACCENT),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#30363d")),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWHEIGHT", (0, 0), (-1, -1), 40),
        ("ROUNDEDCORNERS", [4, 4, 4, 4]),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.5 * cm))

    # ── Forecasts ─────────────────────────────────────────────────────────
    story.append(Paragraph("Horizon Forecasts", h2))
    forecast = thesis.forecast or {}
    weighted = thesis.weighted_scores or {}
    fcast_data = [["Horizon", "Base Return", "Bull", "Bear", "Confidence", "Score"]]
    for h in ["3m", "6m", "12m"]:
        f = forecast.get(h)
        if not f:
            fcast_data.append([h.upper(), "-", "-", "-", "-", "-"])
            continue
        conf = f"{round(f.confidence * 100)}%" if f.confidence is not None else "-"
        ws = f"{weighted[h]:.1f}" if h in weighted else "-"
        base_c = _GREEN if (f.base_return_pct or 0) >= 0 else _RED
        fcast_data.append([
            Paragraph(h.upper(), ParagraphStyle("fc", textColor=_TEXT, fontSize=9, alignment=1)),
            Paragraph(f"{f.base_return_pct:+.1f}%", ParagraphStyle("fc", textColor=base_c, fontSize=9, alignment=1)),
            Paragraph(f"{f.bull_return_pct:+.1f}%", ParagraphStyle("fc", textColor=_GREEN, fontSize=9, alignment=1)),
            Paragraph(f"{f.bear_return_pct:+.1f}%", ParagraphStyle("fc", textColor=_RED, fontSize=9, alignment=1)),
            Paragraph(conf, ParagraphStyle("fc", textColor=_TEXT, fontSize=9, alignment=1)),
            Paragraph(ws, ParagraphStyle("fc", textColor=_ACCENT, fontSize=9, alignment=1)),
        ])
    ft = Table(fcast_data, colWidths=["15%", "17%", "17%", "17%", "17%", "17%"])
    ft.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), _ACCENT),
        ("TEXTCOLOR", (0, 0), (-1, 0), _WHITE),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#ffffff")),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#30363d")),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWHEIGHT", (0, 1), (-1, -1), 24),
    ]))
    story.append(ft)
    story.append(Spacer(1, 0.5 * cm))

    # ── Drivers & Risks ───────────────────────────────────────────────────
    story.append(Paragraph("Drivers &amp; Risks", h2))
    dr_data = [
        [
            _bullet_list("Drivers", thesis.drivers or [], _GREEN, styles),
            _bullet_list("Risks", thesis.risks or [], _RED, styles),
        ]
    ]
    dr_t = Table(dr_data, colWidths=["50%", "50%"])
    dr_t.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(dr_t)
    story.append(Spacer(1, 0.3 * cm))

    # ── Narrative ─────────────────────────────────────────────────────────
    narrative = thesis.narrative or {}
    if any(narrative.values()):
        story.append(Paragraph("Bull / Base / Bear Narrative", h2))
        for k, color in [("bull", _GREEN), ("base", _TEXT), ("bear", _RED)]:
            text = narrative.get(k, "")
            if text:
                story.append(Paragraph(k.upper(), ParagraphStyle(
                    f"Nar{k}", textColor=color, fontSize=8, spaceBefore=4)))
                story.append(Paragraph(text, body))
        story.append(Spacer(1, 0.3 * cm))

    # ── Agent Scores ──────────────────────────────────────────────────────
    agent_scores = thesis.agent_scores or {}
    if agent_scores:
        story.append(Paragraph("Agent Scores", h2))
        rows = [[
            Paragraph(aid.replace("agent.", ""), ParagraphStyle("as", textColor=_TEXT, fontSize=8)),
            Paragraph(f"{score:.1f}", ParagraphStyle("as", textColor=_score_color(score), fontSize=8, alignment=2)),
        ] for aid, score in sorted(agent_scores.items(), key=lambda x: -x[1])]
        at = Table(rows, colWidths=["70%", "30%"])
        at.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#30363d")),
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#ffffff")),
            ("ROWHEIGHT", (0, 0), (-1, -1), 18),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ]))
        story.append(at)
        story.append(Spacer(1, 0.3 * cm))

    # ── Quality Flags ─────────────────────────────────────────────────────
    flags = [f.value if hasattr(f, "value") else str(f) for f in (thesis.quality_flags or [])]
    if flags:
        story.append(Paragraph("Quality Flags", h2))
        story.append(Paragraph("  ".join(flags), ParagraphStyle(
            "Flags", textColor=colors.HexColor("#92400e"), fontSize=9)))
        story.append(Spacer(1, 0.2 * cm))

    # ── Footer ────────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.3 * cm))
    story.append(HRFlowable(width="100%", color=_MUTED, thickness=0.5))
    story.append(Paragraph(
        f"StockPicker Multi-Agent Thesis | thesis_id: {thesis.thesis_id} | "
        f"Generated {generated}",
        ParagraphStyle("Footer", textColor=_MUTED, fontSize=8, spaceBefore=4)))

    doc.build(story)
    return buf.getvalue()


def build_compare_pdf(theses: list[InvestmentThesis]) -> bytes:
    """Render a side-by-side compare PDF for 2+ tickers."""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4, leftMargin=1.5 * cm, rightMargin=1.5 * cm, topMargin=1.5 * cm, bottomMargin=1.5 * cm
    )
    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("CmpH1", parent=styles["Heading1"], textColor=_TEXT, fontSize=16, spaceAfter=6)
    h2 = ParagraphStyle("CmpH2", parent=styles["Heading2"], textColor=_ACCENT, fontSize=10, spaceBefore=8, spaceAfter=4)
    body = ParagraphStyle("CmpBody", parent=styles["Normal"], textColor=_TEXT, fontSize=9, leading=12)
    muted = ParagraphStyle("CmpMuted", parent=styles["Normal"], textColor=_MUTED, fontSize=8, leading=10)

    story: list[Any] = []
    names = ", ".join([t.ticker for t in theses])
    story.append(Paragraph(f"Ticker Compare — {names}", h1))
    story.append(Paragraph(f"Generated {datetime.now(timezone.utc).strftime('%d %b %Y %H:%M UTC')}", muted))
    story.append(Spacer(1, 0.2 * cm))

    headers = ["Metric"] + [t.ticker for t in theses]
    rows = [
        ["Composite Score"] + [f"{t.composite_score:.1f}" for t in theses],
        ["Risk"] + [_fmt(t.risk_rating) for t in theses],
        ["Evidence"] + [_fmt(t.evidence_quality) for t in theses],
        ["Current Price"] + [f"${t.current_price:,.2f}" if t.current_price else "-" for t in theses],
        ["3M Base"] + [f"{(t.forecast.get('3m').base_return_pct if t.forecast.get('3m') else 0):+.1f}%" if t.forecast.get("3m") else "-" for t in theses],
        ["12M Base"] + [f"{(t.forecast.get('12m').base_return_pct if t.forecast.get('12m') else 0):+.1f}%" if t.forecast.get("12m") else "-" for t in theses],
    ]
    colw = [3.2 * cm] + [((A4[0] - 3.0 * cm) - 3.2 * cm) / max(1, len(theses))] * len(theses)
    table = Table([headers] + rows, colWidths=colw)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), _ACCENT),
        ("TEXTCOLOR", (0, 0), (-1, 0), _WHITE),
        ("BACKGROUND", (0, 1), (-1, -1), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#d1d5db")),
        ("ALIGN", (1, 1), (-1, -1), "CENTER"),
        ("TEXTCOLOR", (0, 1), (0, -1), colors.HexColor("#374151")),
    ]))
    story.append(table)

    story.append(Spacer(1, 0.25 * cm))
    story.append(Paragraph("Narrative Snapshot", h2))
    for t in theses:
        story.append(Paragraph(f"<b>{t.ticker}</b>: {(t.narrative or {}).get('base', 'No base narrative.')}", body))
        story.append(Spacer(1, 0.1 * cm))

    doc.build(story)
    return buf.getvalue()


# ── Helpers ───────────────────────────────────────────────────────────────

def _fmt(val: Any) -> str:
    if val is None:
        return "-"
    s = str(val.value) if hasattr(val, "value") else str(val)
    return s.replace("_", " ").title()


def _cell(label: str, value: str, value_color: colors.Color, styles: Any) -> Table:
    inner = Table(
        [[Paragraph(label, ParagraphStyle("CL", textColor=colors.HexColor("#374151"), fontSize=8, alignment=1))],
         [Paragraph(value, ParagraphStyle("CV", textColor=value_color, fontSize=13,
                                          fontName="Helvetica-Bold", alignment=1))]],
        colWidths=["100%"],
    )
    inner.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 4),
                                ("RIGHTPADDING", (0, 0), (-1, -1), 4)]))
    return inner


def _bullet_list(title: str, items: list[str], bullet_color: colors.Color, styles: Any) -> Table:
    title_p = Paragraph(title, ParagraphStyle(
        "BT", textColor=colors.HexColor("#374151"), fontSize=9, spaceBefore=0, spaceAfter=4))
    rows: list[list[Any]] = [[title_p]]
    for item in items[:8]:
        rows.append([Paragraph(
            f"• {item}",
            ParagraphStyle("BI", textColor=colors.HexColor("#111827"),
                           fontSize=9, leading=13, leftIndent=4))])
    if not items:
        rows.append([Paragraph("None recorded.", ParagraphStyle(
            "BN", textColor=colors.HexColor("#4b5563"), fontSize=9))])
    t = Table(rows, colWidths=["100%"])
    t.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 0),
                             ("TOPPADDING", (0, 0), (-1, -1), 1),
                             ("BOTTOMPADDING", (0, 0), (-1, -1), 1)]))
    return t
