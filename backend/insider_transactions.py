"""
SEC Form 4 (insider transactions) ingestion + summary.

What this gives you that's missing today:
- Director / officer / 10%-owner trades, with code (P=purchase, S=sale,
  A=grant, etc.) — one of the highest signal/noise data sources for
  short-to-medium-term equity prediction.
- 30-day rollup per ticker (net $ purchased − sold, transaction counts,
  unique-insider counts) ready to feed into the 9-agent pipeline as a
  new agent input or as a standalone API.

Compliance: SEC requires a real `User-Agent` for programmatic access
(set `EDGAR_USER_AGENT` env var, default falls back to a courtesy
identifier) and capped to 10 requests/sec — we sleep 0.12s between
HTTP calls.

Storage: `insider_transactions` table in the existing stockpicker.db.
DDL added to db.py; we just use `get_conn()` here.

CLI:
    python3 -m insider_transactions --ticker NVDA --refresh
    python3 -m insider_transactions --tickers NVDA MSFT GOOGL --refresh
    python3 -m insider_transactions --ticker NVDA --summary --days 30
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from xml.etree import ElementTree as ET

import httpx

# Local imports — works whether run as `python3 -m insider_transactions`
# or imported from main.py.
sys.path.insert(0, str(Path(__file__).parent))
from db import get_conn  # noqa: E402

logger = logging.getLogger("stockpicker.insider")

# ── SEC EDGAR config ─────────────────────────────────────────────────────────
SEC_USER_AGENT = os.getenv(
    "EDGAR_USER_AGENT",
    "StockPicker subhas.patel@gmail.com",  # SEC requires identifying contact
)
SEC_HEADERS = {"User-Agent": SEC_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
SEC_RATE_DELAY_SEC = 0.12  # ≈ 8 req/sec, well under SEC's 10/s cap

# Transaction codes per Form 4 instructions. We treat P/S as the high-signal
# pair (open-market purchases and sales); everything else is informational.
TXN_CODE_LABELS = {
    "P": "purchase",
    "S": "sale",
    "A": "grant",
    "M": "exercise",
    "F": "tax-withhold",
    "D": "disposition-to-issuer",
    "G": "gift",
    "X": "exercise-out-of-money",
    "V": "voluntary-report",
}
HIGH_SIGNAL_CODES = {"P", "S"}  # codes that carry directional information


# ── HTTP helpers ─────────────────────────────────────────────────────────────
def _http_get(url: str, *, accept_404: bool = False) -> str | None:
    """SEC-compliant GET. Returns body text or None on 404 (if accept_404)."""
    time.sleep(SEC_RATE_DELAY_SEC)
    try:
        r = httpx.get(url, headers=SEC_HEADERS, timeout=20.0, follow_redirects=True)
    except httpx.HTTPError as exc:
        logger.warning("SEC GET failed for %s: %s", url, exc)
        return None
    if r.status_code == 404 and accept_404:
        return None
    if r.status_code != 200:
        logger.warning("SEC GET %s → HTTP %s", url, r.status_code)
        return None
    return r.text


# ── ticker → CIK lookup (cached in-process) ──────────────────────────────────
_CIK_CACHE: dict[str, str] = {}


def _load_cik_map() -> dict[str, str]:
    """Pull SEC's full ticker→CIK mapping (cached on first call)."""
    if _CIK_CACHE:
        return _CIK_CACHE
    body = _http_get("https://www.sec.gov/files/company_tickers.json")
    if not body:
        return {}
    import json
    raw = json.loads(body)
    for _, entry in raw.items():
        ticker = (entry.get("ticker") or "").upper()
        cik    = str(entry.get("cik_str") or "")
        if ticker and cik:
            _CIK_CACHE[ticker] = cik.zfill(10)
    return _CIK_CACHE


def cik_for_ticker(ticker: str) -> str | None:
    """Return SEC CIK (zero-padded to 10 digits) for a ticker, or None."""
    return _load_cik_map().get(ticker.upper())


# ── Filings index + Form 4 fetch ─────────────────────────────────────────────
def _list_form4_filings(cik: str, limit: int = 40) -> list[dict]:
    """Return list of {accession, accession_no_dashes, filed, primaryDocument}."""
    body = _http_get(f"https://data.sec.gov/submissions/CIK{cik}.json")
    if not body:
        return []
    import json
    data = json.loads(body)
    recent = (data.get("filings") or {}).get("recent") or {}
    forms       = recent.get("form") or []
    accessions  = recent.get("accessionNumber") or []
    filed_dates = recent.get("filingDate") or []
    primary_doc = recent.get("primaryDocument") or []
    out = []
    for f, acc, filed, doc in zip(forms, accessions, filed_dates, primary_doc):
        if f != "4":
            continue
        out.append({
            "accession":         acc,
            "accession_no_dash": acc.replace("-", ""),
            "filed":             filed,
            "primary_doc":       doc,
        })
        if len(out) >= limit:
            break
    return out


def _fetch_form4_xml(cik: str, filing: dict) -> str | None:
    """Form 4 XML lives at the accession folder root.

    Subtlety: SEC's submissions JSON returns primary_doc as
    `xslF345X06/wk-form4_NNN.xml` — that path is the **XSL-rendered HTML view**
    of the form, not the raw XML. The raw XML lives at the accession root
    with the same filename minus the xsl/ prefix.
    """
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{filing['accession_no_dash']}"

    # 1. Primary document — strip any xsl/ prefix so we hit raw XML, not HTML.
    doc = filing.get("primary_doc") or ""
    if doc.endswith(".xml"):
        if "/" in doc and doc.lower().startswith("xsl"):
            doc = doc.split("/", 1)[1]
        body = _http_get(f"{base}/{doc}", accept_404=True)
        if body and "<ownershipDocument" in body:
            return body

    # 2. Walk the accession's index page for any non-xsl *.xml — same defence.
    index = _http_get(f"{base}/", accept_404=True)
    if index:
        import re
        for m in re.finditer(r'href="([^"]+\.xml)"', index, flags=re.IGNORECASE):
            xml_url = m.group(1)
            # Skip the rendered-view URLs; only fetch raw XML.
            if "/xsl" in xml_url.lower():
                continue
            if xml_url.startswith("/"):
                xml_url = "https://www.sec.gov" + xml_url
            elif not xml_url.startswith("http"):
                xml_url = f"{base}/{xml_url}"
            body = _http_get(xml_url, accept_404=True)
            if body and "<ownershipDocument" in body:
                return body
    return None


# ── XML parsing ──────────────────────────────────────────────────────────────
def _text(el: ET.Element | None, path: str) -> str | None:
    if el is None:
        return None
    node = el.find(path)
    return (node.text or "").strip() if node is not None and node.text else None


def _float(el: ET.Element | None, path: str) -> float | None:
    raw = _text(el, path)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def parse_form4(xml_text: str, ticker: str, accession: str, filed_at: str) -> list[dict]:
    """Extract one row per non-derivative transaction.

    Skips derivative-table transactions (option grants, conversions) — they're
    relevant but noisier; can be added later if we want full coverage.
    """
    try:
        root = ET.fromstring(xml_text)  # nosec B314 — SEC EDGAR XML, read-only, no entity expansion
    except ET.ParseError as exc:
        logger.warning("Form 4 parse error (%s): %s", accession, exc)
        return []

    # Reporting owner (insider) details
    owner       = root.find("reportingOwner")
    owner_name  = _text(owner, "reportingOwnerId/rptOwnerName")
    rel         = owner.find("reportingOwnerRelationship") if owner is not None else None
    is_director = (_text(rel, "isDirector") or "").lower() in ("1", "true")
    is_officer  = (_text(rel, "isOfficer")  or "").lower() in ("1", "true")
    is_ten_pct  = (_text(rel, "isTenPercentOwner") or "").lower() in ("1", "true")
    officer_title = _text(rel, "officerTitle") or ""
    title = officer_title or (
        "Director" if is_director else "10% Owner" if is_ten_pct else "Insider"
    )

    rows: list[dict] = []
    nd_table = root.find("nonDerivativeTable")
    if nd_table is None:
        return rows

    for idx, tx in enumerate(nd_table.findall("nonDerivativeTransaction")):
        tx_date   = _text(tx, "transactionDate/value")
        tx_code   = _text(tx, "transactionCoding/transactionCode")
        shares    = _float(tx, "transactionAmounts/transactionShares/value")
        price     = _float(tx, "transactionAmounts/transactionPricePerShare/value")
        ad_code   = _text(tx, "transactionAmounts/transactionAcquiredDisposedCode/value")
        post_amt  = _float(tx, "postTransactionAmounts/sharesOwnedFollowingTransaction/value")

        if shares is None:
            continue

        # Signed share count: positive = acquired, negative = disposed.
        signed_shares = shares if (ad_code or "A").upper() == "A" else -shares
        total_value   = None
        if price is not None:
            total_value = round(signed_shares * price, 2)

        rows.append({
            "txn_id":               f"{accession}-{idx}",
            "ticker":               ticker.upper(),
            "filed_at":             filed_at,
            "transaction_date":     tx_date,
            "insider_name":         owner_name,
            "insider_title":        title,
            "transaction_code":     (tx_code or "").upper(),
            "transaction_label":    TXN_CODE_LABELS.get((tx_code or "").upper(), tx_code),
            "shares":               signed_shares,
            "price":                price,
            "total_value":          total_value,
            "shares_after":         post_amt,
            "is_director":          1 if is_director else 0,
            "is_officer":           1 if is_officer else 0,
            "is_ten_percent_owner": 1 if is_ten_pct else 0,
            "accession":            accession,
        })
    return rows


# ── Storage ──────────────────────────────────────────────────────────────────
INSIDER_DDL = """
CREATE TABLE IF NOT EXISTS insider_transactions (
    txn_id              TEXT PRIMARY KEY,
    ticker              TEXT NOT NULL,
    filed_at            TEXT,
    transaction_date    TEXT,
    insider_name        TEXT,
    insider_title       TEXT,
    transaction_code    TEXT,
    shares              REAL,
    price               REAL,
    total_value         REAL,
    shares_after        REAL,
    is_director         INTEGER DEFAULT 0,
    is_officer          INTEGER DEFAULT 0,
    is_ten_percent_owner INTEGER DEFAULT 0,
    accession           TEXT,
    fetched_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_insider_ticker_date
    ON insider_transactions(ticker, transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_code
    ON insider_transactions(transaction_code);
"""


def ensure_schema() -> None:
    with get_conn() as conn:
        conn.executescript(INSIDER_DDL)


def store_transactions(rows: Iterable[dict]) -> int:
    """Upsert insider transaction rows. Returns count of new rows inserted."""
    ensure_schema()
    rows = list(rows)
    if not rows:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    with get_conn() as conn:
        for r in rows:
            cur = conn.execute(
                """INSERT OR IGNORE INTO insider_transactions
                   (txn_id, ticker, filed_at, transaction_date, insider_name,
                    insider_title, transaction_code, shares, price, total_value,
                    shares_after, is_director, is_officer, is_ten_percent_owner,
                    accession, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    r["txn_id"], r["ticker"], r["filed_at"], r["transaction_date"],
                    r["insider_name"], r["insider_title"], r["transaction_code"],
                    r["shares"], r["price"], r["total_value"], r["shares_after"],
                    r["is_director"], r["is_officer"], r["is_ten_percent_owner"],
                    r.get("accession"), now,
                ),
            )
            if cur.rowcount:
                inserted += 1
    return inserted


def list_transactions(ticker: str, days: int = 90, limit: int = 100) -> list[dict]:
    """Most recent transactions for a ticker, ordered by transaction_date desc."""
    ensure_schema()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT * FROM insider_transactions
               WHERE ticker = ? AND transaction_date >= ?
               ORDER BY transaction_date DESC, filed_at DESC
               LIMIT ?""",
            (ticker.upper(), cutoff, limit),
        ).fetchall()
    return [dict(r) for r in rows]


# ── Orchestration ───────────────────────────────────────────────────────────
def refresh_ticker(ticker: str, limit: int = 40) -> dict:
    """Pull recent Form 4 filings for a ticker, parse, store. Returns counts."""
    ticker = ticker.upper()
    cik = cik_for_ticker(ticker)
    if not cik:
        return {"ticker": ticker, "error": "no CIK found"}

    filings = _list_form4_filings(cik, limit=limit)
    if not filings:
        return {"ticker": ticker, "filings_seen": 0, "transactions_added": 0}

    all_rows: list[dict] = []
    for filing in filings:
        xml = _fetch_form4_xml(cik, filing)
        if not xml:
            continue
        rows = parse_form4(xml, ticker, filing["accession"], filing["filed"])
        all_rows.extend(rows)

    inserted = store_transactions(all_rows)
    return {
        "ticker":             ticker,
        "cik":                cik,
        "filings_seen":       len(filings),
        "transactions_added": inserted,
        "transactions_total": len(all_rows),
    }


def summarize_ticker(ticker: str, days: int = 30) -> dict:
    """Net buying $ / count / unique insiders / role breakdown for last N days.

    Net buying = sum(P transactions $) − sum(S transactions $). Positive
    = insiders net-buying — classically bullish signal.
    """
    txns = list_transactions(ticker, days=days, limit=500)
    high_signal = [t for t in txns if t["transaction_code"] in HIGH_SIGNAL_CODES]

    # total_value is signed (positive = acquired, negative = disposed) per
    # parse_form4. Net dollar flow = sum over P+S transactions.
    net_value     = round(sum(t["total_value"] or 0 for t in high_signal), 2)
    purchase_val  = round(sum((t["total_value"] or 0) for t in high_signal
                              if t["transaction_code"] == "P"), 2)
    sale_val      = round(sum(abs(t["total_value"] or 0) for t in high_signal
                              if t["transaction_code"] == "S"), 2)

    insiders = {t["insider_name"] for t in high_signal if t["insider_name"]}
    director_txns = sum(1 for t in high_signal if t["is_director"])
    officer_txns  = sum(1 for t in high_signal if t["is_officer"])

    # Cluster buying = ≥3 insiders making P transactions in the window
    cluster = (
        len({t["insider_name"] for t in high_signal
             if t["transaction_code"] == "P" and t["insider_name"]})
        >= 3
    )

    return {
        "ticker":               ticker.upper(),
        "window_days":          days,
        "transaction_count":    len(high_signal),
        "purchase_count":       sum(1 for t in high_signal if t["transaction_code"] == "P"),
        "sale_count":           sum(1 for t in high_signal if t["transaction_code"] == "S"),
        "net_value_usd":        net_value,
        "purchase_value_usd":   purchase_val,
        "sale_value_usd":       sale_val,
        "unique_insiders":      len(insiders),
        "director_transactions": director_txns,
        "officer_transactions":  officer_txns,
        "cluster_buying":       cluster,
    }


# ── CLI ──────────────────────────────────────────────────────────────────────
def _cli() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ticker", help="single ticker (e.g. NVDA)")
    ap.add_argument("--tickers", nargs="+", help="multiple tickers (overrides --ticker)")
    ap.add_argument("--refresh", action="store_true", help="pull fresh data from SEC EDGAR")
    ap.add_argument("--summary", action="store_true", help="print N-day rollup")
    ap.add_argument("--days", type=int, default=30, help="window for --summary (default 30)")
    ap.add_argument("--limit", type=int, default=40, help="max filings per ticker (default 40)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    tickers = args.tickers or ([args.ticker] if args.ticker else [])
    if not tickers:
        ap.error("provide --ticker or --tickers")

    for t in tickers:
        if args.refresh:
            r = refresh_ticker(t, limit=args.limit)
            print(f"[{t}] {r}")
        if args.summary or not args.refresh:
            s = summarize_ticker(t, days=args.days)
            print(f"[{t}] {s}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
