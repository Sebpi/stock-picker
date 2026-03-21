const API = "";

let priceChart = null;
let watchlist = [];

const TAB_LOADERS = { watchlist: () => loadWatchlist(), predictions: () => loadPredictions(), alerts: () => loadAlerts() };
const SIGNAL_LABELS = { daily_swing: "Daily Swing", momentum: "Momentum", volume_surge: "Vol Surge" };

document.querySelectorAll(".tab-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById(`tab-${btn.dataset.tab}`).classList.add("active");
    TAB_LOADERS[btn.dataset.tab]?.();
  });
});

// ── Helpers ──────────────────────────────────────────────────
const n2  = v => v != null ? v.toFixed(2) : "—";
const pct = v => v != null ? (v * 100).toFixed(1) + "%" : "—";
function destroyChart() { if (priceChart) { priceChart.destroy(); priceChart = null; } }

function fmt(n) {
  if (n == null) return "—";
  if (n >= 1e12) return "$" + (n / 1e12).toFixed(2) + "T";
  if (n >= 1e9)  return "$" + (n / 1e9).toFixed(2) + "B";
  if (n >= 1e6)  return "$" + (n / 1e6).toFixed(2) + "M";
  return "$" + n.toLocaleString();
}

function changeHtml(pct) {
  if (pct == null) return "<span>—</span>";
  const sign = pct >= 0 ? "+" : "";
  const cls = pct >= 0 ? "change-pos" : "change-neg";
  return `<span class="${cls}">${sign}${pct.toFixed(2)}%</span>`;
}

// ── Screener ──────────────────────────────────────────────────
document.getElementById("btn-screen").addEventListener("click", runScreen);

async function runScreen() {
  const btn = document.getElementById("btn-screen");
  const status = document.getElementById("screen-status");
  const body = document.getElementById("screen-body");

  const sector   = document.getElementById("filter-sector").value;
  const maxPe    = document.getElementById("filter-pe").value;
  const maxPeg   = document.getElementById("filter-peg").value;
  const maxPb    = document.getElementById("filter-pb").value;
  const maxEv    = document.getElementById("filter-ev").value;
  const minFcf   = document.getElementById("filter-fcf").value;
  const minCap   = document.getElementById("filter-cap").value;
  const minVol   = document.getElementById("filter-vol").value;

  const params = new URLSearchParams();
  if (sector) params.set("sector", sector);
  if (maxPe)  params.set("max_pe", maxPe);
  if (maxPeg) params.set("max_peg", maxPeg);
  if (maxPb)  params.set("max_pb", maxPb);
  if (maxEv)  params.set("max_ev_ebitda", maxEv);
  if (minFcf) params.set("min_fcf_yield", minFcf);
  if (minCap) params.set("min_market_cap", parseFloat(minCap) * 1e9);
  if (minVol) params.set("min_volume", parseFloat(minVol) * 1e6);

  btn.disabled = true;
  status.textContent = "Screening stocks… this may take a moment.";
  body.innerHTML = "";

  try {
    const res = await fetch(`${API}/api/screen?${params}`);
    const data = await res.json();

    if (data.length === 0) {
      status.textContent = "No stocks matched your criteria.";
      return;
    }

    status.textContent = `${data.length} stock${data.length !== 1 ? "s" : ""} found.`;

    body.innerHTML = data.map(s => `
      <tr data-ticker="${s.ticker}">
        <td><strong>${s.ticker}</strong></td>
        <td>${s.name}</td>
        <td>${s.sector || "—"}</td>
        <td>${s.price != null ? "$" + s.price : "—"}</td>
        <td>${s.pe ?? "—"}</td>
        <td>${s.peg ?? "—"}</td>
        <td>${s.pb ?? "—"}</td>
        <td>${s.ev_ebitda ?? "—"}</td>
        <td>${s.fcf_yield != null ? s.fcf_yield + "%" : "—"}</td>
        <td>${fmt(s.market_cap)}</td>
        <td><button class="btn-icon" onclick="addToWatchlist(event,'${s.ticker}')">+ Watch</button></td>
      </tr>
    `).join("");

    body.querySelectorAll("tr").forEach(row => {
      row.addEventListener("click", e => {
        if (e.target.tagName === "BUTTON") return;
        openDetail(row.dataset.ticker);
      });
    });

    // Mark already-watched tickers
    refreshWatchButtons();

  } catch (err) {
    status.textContent = "Error: " + err.message + ". Is the backend running?";
  } finally {
    btn.disabled = false;
  }
}

// ── Watchlist ────────────────────────────────────────────────
async function loadWatchlist() {
  const status = document.getElementById("watchlist-status");
  const body = document.getElementById("watchlist-body");
  const empty = document.getElementById("watchlist-empty");

  status.textContent = "Loading…";
  try {
    const res = await fetch(`${API}/api/watchlist`);
    watchlist = await res.json();
    status.textContent = "";

    if (watchlist.length === 0) {
      body.innerHTML = "";
      empty.classList.add("visible");
      return;
    }

    empty.classList.remove("visible");
    body.innerHTML = watchlist.map(s => `
      <tr data-ticker="${s.ticker}">
        <td><strong>${s.ticker}</strong></td>
        <td>${s.name}</td>
        <td>${s.price != null ? "$" + s.price : "—"}</td>
        <td>${changeHtml(s.change_pct)}</td>
        <td><button class="btn-remove" onclick="removeFromWatchlist(event,'${s.ticker}')">Remove</button></td>
      </tr>
    `).join("");

    body.querySelectorAll("tr").forEach(row => {
      row.addEventListener("click", e => {
        if (e.target.tagName === "BUTTON") return;
        openDetail(row.dataset.ticker);
      });
    });

  } catch (err) {
    status.textContent = "Error loading watchlist. Is the backend running?";
  }
}

document.getElementById("btn-refresh-watchlist").addEventListener("click", loadWatchlist);

async function addToWatchlist(e, ticker) {
  e.stopPropagation();
  const btn = e.target;
  btn.disabled = true;
  try {
    await fetch(`${API}/api/watchlist/${ticker}`, { method: "POST" });
    btn.textContent = "✓ Added";
    btn.classList.add("added");
  } catch (err) {
    btn.disabled = false;
  }
}

async function removeFromWatchlist(e, ticker) {
  e.stopPropagation();
  await fetch(`${API}/api/watchlist/${ticker}`, { method: "DELETE" });
  loadWatchlist();
}

function refreshWatchButtons() {
  const watched = new Set(watchlist.map(s => s.ticker));
  document.querySelectorAll("#screen-body .btn-icon").forEach(btn => {
    const ticker = btn.closest("tr")?.dataset.ticker;
    if (ticker && watched.has(ticker)) {
      btn.textContent = "✓ Added";
      btn.classList.add("added");
    }
  });
}

// ── Detail Panel ─────────────────────────────────────────────
function setValArrow(arrowId, comparison) {
  const el = document.getElementById(arrowId);
  if (!el) return;
  if (comparison === "undervalued") {
    el.textContent = "▲";
    el.className = "val-arrow arrow-undervalued";
    el.title = "Undervalued vs sector peers";
  } else if (comparison === "overvalued") {
    el.textContent = "▼";
    el.className = "val-arrow arrow-overvalued";
    el.title = "Overvalued vs sector peers";
  } else {
    el.textContent = "";
    el.className = "val-arrow";
    el.title = "";
  }
}

async function openDetail(ticker) {
  const overlay = document.getElementById("detail-overlay");
  overlay.classList.remove("hidden");

  document.getElementById("detail-name").textContent = "Loading…";
  document.getElementById("detail-ticker").textContent = ticker;
  document.getElementById("detail-sector").textContent = "";
  document.getElementById("detail-price").textContent = "";
  document.getElementById("detail-change").innerHTML = "";
  document.getElementById("stat-pe").textContent = "—";
  document.getElementById("stat-cap").textContent = "—";
  document.getElementById("stat-high").textContent = "—";
  document.getElementById("stat-low").textContent = "—";
  document.getElementById("detail-desc").textContent = "";
  ["arrow-pe", "arrow-peg", "arrow-pb", "arrow-ev", "arrow-fcf"].forEach(id => setValArrow(id, null));

  destroyChart();

  try {
    const [stockRes, peersRes] = await Promise.all([
      fetch(`${API}/api/stock/${ticker}`),
      fetch(`${API}/api/stock/${ticker}/peers`),
    ]);
    const d = await stockRes.json();
    const peers = peersRes.ok ? await peersRes.json() : null;

    document.getElementById("detail-name").textContent = d.name;
    document.getElementById("detail-ticker").textContent = d.ticker;
    document.getElementById("detail-sector").textContent = d.sector || "";
    document.getElementById("detail-price").textContent = d.price != null ? "$" + d.price.toFixed(2) : "—";
    document.getElementById("detail-change").innerHTML = changeHtml(d.change_pct);
    document.getElementById("stat-pe").textContent     = n2(d.pe);
    document.getElementById("stat-peg").textContent    = n2(d.peg);
    document.getElementById("stat-pb").textContent     = n2(d.pb);
    document.getElementById("stat-ev").textContent     = n2(d.ev_ebitda);
    document.getElementById("stat-fcf").textContent    = d.fcf_yield != null ? d.fcf_yield + "%" : "—";
    document.getElementById("stat-margin").textContent = pct(d.profit_margin);
    document.getElementById("stat-cap").textContent    = fmt(d.market_cap);
    document.getElementById("stat-beta").textContent   = n2(d.beta);
    document.getElementById("stat-eps").textContent    = pct(d.eps_growth);
    document.getElementById("stat-rev").textContent    = pct(d.revenue_growth);
    document.getElementById("stat-high").textContent   = d.week_52_high != null ? "$" + d.week_52_high.toFixed(2) : "—";
    document.getElementById("stat-low").textContent    = d.week_52_low != null ? "$" + d.week_52_low.toFixed(2) : "—";
    document.getElementById("detail-desc").textContent = d.description || "";

    if (peers && peers.comparison) {
      setValArrow("arrow-pe",  peers.comparison.pe);
      setValArrow("arrow-peg", peers.comparison.peg);
      setValArrow("arrow-pb",  peers.comparison.pb);
      setValArrow("arrow-ev",  peers.comparison.ev_ebitda);
      setValArrow("arrow-fcf", peers.comparison.fcf_yield);
    }

    // Chart
    const labels = d.history.map(h => h.date);
    const prices = d.history.map(h => h.close);

    const ctx = document.getElementById("price-chart").getContext("2d");
    priceChart = new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [{
          label: d.ticker,
          data: prices,
          borderColor: "#4f8ef7",
          backgroundColor: "rgba(79,142,247,0.08)",
          borderWidth: 2,
          pointRadius: 0,
          fill: true,
          tension: 0.3,
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: {
            ticks: {
              color: "#7b82a0",
              maxTicksLimit: 6,
              maxRotation: 0,
            },
            grid: { color: "#2e3350" },
          },
          y: {
            ticks: { color: "#7b82a0", callback: v => "$" + v },
            grid: { color: "#2e3350" },
          }
        }
      }
    });

    const wlBtn = document.getElementById("detail-watchlist-btn");
    const inWatchlist = watchlist.some(s => s.ticker === ticker.toUpperCase());

    wlBtn.textContent = inWatchlist ? "✓ In Watchlist" : "+ Add to Watchlist";
    wlBtn.onclick = async () => {
      if (inWatchlist) return;
      await fetch(`${API}/api/watchlist/${ticker}`, { method: "POST" });
      wlBtn.textContent = "✓ In Watchlist";
    };

  } catch (err) {
    document.getElementById("detail-name").textContent = "Failed to load stock data.";
  }
}

document.getElementById("detail-close").addEventListener("click", () => {
  document.getElementById("detail-overlay").classList.add("hidden");
  destroyChart();
});

document.getElementById("detail-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("detail-overlay")) {
    document.getElementById("detail-overlay").classList.add("hidden");
    destroyChart();
  }
});

// ── AI Advisor ───────────────────────────────────────────────
document.getElementById("btn-ask").addEventListener("click", async () => {
  const query = document.getElementById("ai-query").value.trim();
  if (!query) return;

  const btn = document.getElementById("btn-ask");
  const status = document.getElementById("ai-status");
  const response = document.getElementById("ai-response");

  btn.disabled = true;
  status.textContent = "Asking Claude…";
  response.classList.remove("visible");

  try {
    const res = await fetch(`${API}/api/recommend`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query }),
    });
    const data = await res.json();

    if (data.detail) {
      status.textContent = "Error: " + data.detail;
    } else {
      status.textContent = "";
      response.textContent = data.response;
      response.classList.add("visible");
    }
  } catch (err) {
    status.textContent = "Error: " + err.message + ". Is the backend running?";
  } finally {
    btn.disabled = false;
  }
});

// ── Predictions ───────────────────────────────────────────────

async function loadPredictions() {
  const status = document.getElementById("pred-status");
  const body = document.getElementById("pred-body");
  const empty = document.getElementById("pred-empty");
  const bar = document.getElementById("accuracy-bar");

  status.textContent = "Loading predictions…";
  try {
    const res = await fetch(`${API}/api/predictions`);
    const preds = await res.json();
    status.textContent = "";

    if (preds.length === 0) {
      body.innerHTML = "";
      empty.classList.add("visible");
      bar.classList.add("hidden");
      return;
    }

    empty.classList.remove("visible");
    renderPredictionsTable(preds);
    renderAccuracyBar(preds);
  } catch (err) {
    status.textContent = "Error loading predictions. Is the backend running?";
  }
}

function renderPredictionsTable(preds) {
  const body = document.getElementById("pred-body");
  body.innerHTML = preds.map(p => {
    const predCls = p.predicted_pct >= 0 ? "change-pos" : "change-neg";
    const predStr = p.predicted_pct != null ? `<span class="${predCls}">${p.predicted_pct >= 0 ? "+" : ""}${p.predicted_pct.toFixed(2)}%</span>` : "—";

    let actualStr = '<span class="result-pending">Pending</span>';
    let resultStr = '<span class="result-pending">—</span>';
    if (p.actual_pct != null) {
      const actCls = p.actual_pct >= 0 ? "change-pos" : "change-neg";
      actualStr = `<span class="${actCls}">${p.actual_pct >= 0 ? "+" : ""}${p.actual_pct.toFixed(2)}%</span>`;
      const correct = (p.predicted_pct > 0) === (p.actual_pct > 0);
      resultStr = correct
        ? '<span class="result-correct">✓ Correct</span>'
        : '<span class="result-wrong">✗ Wrong</span>';
    }

    const confBadge = `<span class="badge-${p.confidence || 'medium'}">${(p.confidence || 'medium').toUpperCase()}</span>`;

    return `
      <tr>
        <td>${p.date}</td>
        <td><strong>${p.ticker}</strong></td>
        <td>${predStr}</td>
        <td>${actualStr}</td>
        <td>${resultStr}</td>
        <td>${confBadge}</td>
        <td class="reasoning-cell">${p.reasoning || "—"}</td>
      </tr>
    `;
  }).join("");
}

function renderAccuracyBar(preds) {
  const bar = document.getElementById("accuracy-bar");
  const completed = preds.filter(p => p.actual_pct != null);

  if (completed.length === 0) {
    bar.classList.add("hidden");
    return;
  }

  bar.classList.remove("hidden");

  const correct = completed.filter(p => (p.predicted_pct > 0) === (p.actual_pct > 0)).length;
  const accPct = (correct / completed.length * 100).toFixed(0);
  const avgPred = (completed.reduce((s, p) => s + p.predicted_pct, 0) / completed.length).toFixed(2);
  const avgActual = (completed.reduce((s, p) => s + p.actual_pct, 0) / completed.length).toFixed(2);

  document.getElementById("acc-pct").textContent = accPct + "%";
  document.getElementById("acc-total").textContent = completed.length + " resolved";
  document.getElementById("acc-avg-pred").textContent = (avgPred >= 0 ? "+" : "") + avgPred + "%";
  document.getElementById("acc-avg-actual").textContent = (avgActual >= 0 ? "+" : "") + avgActual + "%";

  const accEl = document.getElementById("acc-pct");
  accEl.style.color = accPct >= 60 ? "var(--green)" : accPct >= 40 ? "var(--accent)" : "var(--red)";
}

document.getElementById("btn-generate").addEventListener("click", async () => {
  const btn = document.getElementById("btn-generate");
  const status = document.getElementById("pred-status");

  btn.disabled = true;
  status.textContent = "Fetching macro data, news and fundamentals… this takes ~30 seconds.";

  try {
    const res = await fetch(`${API}/api/predictions/generate`, { method: "POST" });
    const data = await res.json();

    if (data.detail) {
      status.textContent = "Error: " + data.detail;
      return;
    }

    if (data.message) {
      status.textContent = data.message;
    } else {
      status.textContent = `Generated ${data.predictions.length} prediction(s) for today.`;
    }

    loadPredictions();
  } catch (err) {
    status.textContent = "Error: " + err.message + ". Is the backend running?";
  } finally {
    btn.disabled = false;
  }
});

document.getElementById("btn-refresh-preds").addEventListener("click", loadPredictions);

// ── Alerts ────────────────────────────────────────────────────

async function loadAlerts() {
  const status = document.getElementById("alerts-status");
  status.textContent = "Loading…";
  try {
    const [alertsRes, statusRes] = await Promise.all([
      fetch(`${API}/api/alerts`),
      fetch(`${API}/api/alerts/status`),
    ]);
    const alerts = await alertsRes.json();
    const monStatus = await statusRes.json();
    status.textContent = "";
    renderMonitorBar(monStatus);
    renderAlertsTable(alerts);
  } catch (err) {
    status.textContent = "Error loading alerts. Is the backend running?";
  }
}

function renderMonitorBar(s) {
  document.getElementById("mon-active").innerHTML =
    s.active ? '<span class="mon-ok">● Running</span>' : '<span class="mon-off">○ Starting…</span>';

  let lastCheck = "—";
  if (s.last_check) {
    const d = new Date(s.last_check);
    lastCheck = d.toLocaleTimeString();
  }
  document.getElementById("mon-last").textContent = lastCheck;
  document.getElementById("mon-watching").textContent = s.watching + " stock" + (s.watching !== 1 ? "s" : "");
  document.getElementById("mon-email").innerHTML =
    s.notifications.email ? '<span class="mon-ok">✓ On</span>' : '<span class="mon-off">✗ Off</span>';
  document.getElementById("mon-sms").innerHTML =
    s.notifications.sms ? '<span class="mon-ok">✓ On</span>' : '<span class="mon-off">✗ Off</span>';
  document.getElementById("mon-swing").textContent = s.thresholds.daily_swing_pct + "%";
}

function renderAlertsTable(alerts) {
  const body = document.getElementById("alerts-body");
  const empty = document.getElementById("alerts-empty");

  if (alerts.length === 0) {
    body.innerHTML = "";
    empty.classList.add("visible");
    return;
  }
  empty.classList.remove("visible");

  body.innerHTML = alerts.map(a => {
    const time = new Date(a.timestamp).toLocaleString();
    const signals = a.signals || [];
    const primarySignal = signals[0] || {};
    const allSignals = signals.map(s =>
      `<span class="signal-tag signal-${s.type}">${s.signal}</span>`
    ).join(" ");
    const type = signals.map(s => SIGNAL_LABELS[s.type] || s.type).join(", ");

    const changePct = primarySignal.change_pct;
    const priceHtml = `$${a.price?.toFixed(2) ?? "—"}` +
      (changePct != null ? ` <span class="${changePct >= 0 ? "change-pos" : "change-neg"}">${changePct >= 0 ? "+" : ""}${changePct.toFixed(2)}%</span>` : "");

    return `
      <tr>
        <td style="white-space:nowrap;font-size:0.82rem">${time}</td>
        <td><strong>${a.ticker}</strong><br><span style="color:var(--text-muted);font-size:0.78rem">${a.name || ""}</span></td>
        <td>${priceHtml}</td>
        <td>${allSignals}</td>
        <td style="font-size:0.82rem;color:var(--text-muted)">${type}</td>
        <td class="${a.notified_email ? "notif-yes" : "notif-no"}">${a.notified_email ? "✓ Sent" : "—"}</td>
        <td class="${a.notified_sms ? "notif-yes" : "notif-no"}">${a.notified_sms ? "✓ Sent" : "—"}</td>
      </tr>
    `;
  }).join("");
}

document.getElementById("btn-refresh-alerts").addEventListener("click", loadAlerts);

document.getElementById("btn-test-alert").addEventListener("click", async () => {
  const btn = document.getElementById("btn-test-alert");
  const status = document.getElementById("alerts-status");
  btn.disabled = true;
  status.textContent = "Sending test alert…";
  try {
    const res = await fetch(`${API}/api/alerts/test`, { method: "POST" });
    const data = await res.json();
    const parts = [];
    if (data.email_sent) parts.push("Email sent ✓");
    else parts.push("Email not configured");
    if (data.sms_sent) parts.push("SMS sent ✓");
    else parts.push("SMS not configured");
    status.textContent = parts.join(" · ");
  } catch (err) {
    status.textContent = "Error: " + err.message;
  } finally {
    btn.disabled = false;
  }
});

document.getElementById("btn-clear-alerts").addEventListener("click", async () => {
  if (!confirm("Clear all alert history?")) return;
  await fetch(`${API}/api/alerts`, { method: "DELETE" });
  loadAlerts();
});
