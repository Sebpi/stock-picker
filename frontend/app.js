const API = "";

// ── XSS protection ────────────────────────────────────────────
const safe = html => (typeof DOMPurify !== "undefined" ? DOMPurify.sanitize(String(html ?? "")) : String(html ?? ""));

// ── Auth helpers ──────────────────────────────────────────────
const TOKEN_KEY = "sp_token";
const getToken = () => localStorage.getItem(TOKEN_KEY);
const setToken = t => localStorage.setItem(TOKEN_KEY, t);
const clearToken = () => localStorage.removeItem(TOKEN_KEY);

async function authFetch(url, opts = {}) {
  const token = getToken();
  opts.headers = { ...(opts.headers || {}), "Authorization": `Bearer ${token}` };
  const res = await fetch(url, opts);
  if (res.status === 401) {
    clearToken();
    showLogin();
    throw new Error("Session expired. Please sign in again.");
  }
  return res;
}

function showLogin() {
  document.getElementById("login-overlay").style.display = "flex";
  document.body.classList.add("login-active");
}
function hideLogin() {
  document.getElementById("login-overlay").style.display = "none";
  document.body.classList.remove("login-active");
}

// ── Login / Reset password flow ───────────────────────────────
(function initAuth() {
  const loginWrap   = document.getElementById("login-form-wrap");
  const forgotWrap  = document.getElementById("forgot-form-wrap");
  const resetWrap   = document.getElementById("reset-form-wrap");

  function showPanel(panel) {
    [loginWrap, forgotWrap, resetWrap].forEach(p => p.style.display = "none");
    panel.style.display = "block";
  }

  // Check for reset token in URL
  const urlToken = new URLSearchParams(window.location.search).get("reset_token");
  if (urlToken) {
    showLogin();
    showPanel(resetWrap);
    document.getElementById("btn-reset").onclick = async () => {
      const np = document.getElementById("reset-password").value;
      const cp = document.getElementById("reset-confirm").value;
      const errEl = document.getElementById("reset-error");
      errEl.style.display = "none";
      if (np !== cp) { errEl.textContent = "Passwords do not match."; errEl.style.display = "block"; return; }
      if (np.length < 12) { errEl.textContent = "Password must be at least 12 characters."; errEl.style.display = "block"; return; }
      const res = await fetch(`${API}/api/auth/reset-password`, {
        method: "POST", headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ token: urlToken, new_password: np })
      });
      if (res.ok) {
        history.replaceState(null, "", "/");
        showPanel(loginWrap);
        document.getElementById("login-error").style.display = "none";
        alert("Password updated. Please sign in.");
      } else {
        const d = await res.json();
        errEl.textContent = d.detail || "Reset failed.";
        errEl.style.display = "block";
      }
    };
  }

  // Check if already logged in
  const token = getToken();
  if (token) {
    fetch(`${API}/api/auth/me`, { headers: { "Authorization": `Bearer ${token}` } })
      .then(r => r.ok ? hideLogin() : showLogin())
      .catch(() => showLogin());
  } else {
    showLogin();
  }

  // Sign in
  document.getElementById("btn-login").onclick = async () => {
    const u = document.getElementById("login-username").value.trim();
    const p = document.getElementById("login-password").value;
    const errEl = document.getElementById("login-error");
    errEl.style.display = "none";
    try {
      const res = await fetch(`${API}/api/auth/login`, {
        method: "POST", headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ username: u, password: p })
      });
      if (res.ok) {
        const d = await res.json();
        setToken(d.access_token);
        hideLogin();
      } else {
        const d = await res.json();
        errEl.textContent = d.detail || "Invalid credentials.";
        errEl.style.display = "block";
      }
    } catch { errEl.textContent = "Cannot connect to server."; errEl.style.display = "block"; }
  };

  document.getElementById("login-password").addEventListener("keydown", e => {
    if (e.key === "Enter") document.getElementById("btn-login").click();
  });

  // Forgot password
  document.getElementById("link-forgot").onclick = e => { e.preventDefault(); showPanel(forgotWrap); };
  document.getElementById("link-back-login").onclick = e => { e.preventDefault(); showPanel(loginWrap); };
  document.getElementById("btn-forgot").onclick = async () => {
    const u = document.getElementById("forgot-username").value.trim();
    const msgEl = document.getElementById("forgot-msg");
    const res = await fetch(`${API}/api/auth/forgot-password`, {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ username: u })
    });
    const d = await res.json();
    msgEl.textContent = d.message || "Reset link sent if the username exists.";
    msgEl.style.display = "block";
  };
})();

// ── Logout ────────────────────────────────────────────────────
document.getElementById("btn-logout").onclick = () => { clearToken(); showLogin(); };

// ── Change Password Modal ─────────────────────────────────────
document.getElementById("btn-logout").addEventListener("contextmenu", e => {
  e.preventDefault();
  document.getElementById("change-pw-modal").style.display = "flex";
});
document.getElementById("btn-cp-cancel").onclick = () => {
  document.getElementById("change-pw-modal").style.display = "none";
  ["cp-current","cp-new","cp-confirm"].forEach(id => document.getElementById(id).value = "");
};
document.getElementById("btn-cp-save").onclick = async () => {
  const cur = document.getElementById("cp-current").value;
  const np  = document.getElementById("cp-new").value;
  const cp  = document.getElementById("cp-confirm").value;
  const errEl = document.getElementById("change-pw-error");
  const okEl  = document.getElementById("change-pw-ok");
  errEl.style.display = "none"; okEl.style.display = "none";
  if (np !== cp) { errEl.textContent = "New passwords do not match."; errEl.style.display = "block"; return; }
  if (np.length < 12) { errEl.textContent = "Password must be at least 12 characters."; errEl.style.display = "block"; return; }
  try {
    const res = await fetch(`${API}/api/auth/change-password`, {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ current_password: cur, new_password: np })
    });
    if (res.ok) {
      okEl.textContent = "Password updated successfully.";
      okEl.style.display = "block";
      ["cp-current","cp-new","cp-confirm"].forEach(id => document.getElementById(id).value = "");
    } else {
      const d = await res.json();
      errEl.textContent = d.detail || "Failed to update password.";
      errEl.style.display = "block";
    }
  } catch (err) { errEl.textContent = err.message; errEl.style.display = "block"; }
};

let priceChart = null;
let watchlist = [];

const TAB_LOADERS = { watchlist: () => loadWatchlist(), predictions: () => loadPredictions(), recommendations: () => loadRecommendations(), alerts: () => loadAlerts(), portfolio: () => loadPortfolio(), backtest: () => {} };
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
    const res = await authFetch(`${API}/api/screen?${params}`);
    const data = await res.json();

    if (data.length === 0) {
      status.textContent = "No stocks matched your criteria.";
      return;
    }

    status.textContent = `${data.length} stock${data.length !== 1 ? "s" : ""} found.`;

    // Compute sector medians from screener results for inline arrows
    const sectorGroups = {};
    for (const s of data) {
      const sec = s.sector || "Unknown";
      if (!sectorGroups[sec]) sectorGroups[sec] = [];
      sectorGroups[sec].push(s);
    }
    function calcMedian(stocks, key) {
      const vals = stocks.map(s => s[key]).filter(v => v != null).sort((a, b) => a - b);
      if (!vals.length) return null;
      const mid = Math.floor(vals.length / 2);
      return vals.length % 2 === 0 ? (vals[mid - 1] + vals[mid]) / 2 : vals[mid];
    }
    const sectorMedians = {};
    for (const [sec, stocks] of Object.entries(sectorGroups)) {
      if (stocks.length < 2) continue; // need peers to compare
      sectorMedians[sec] = {
        pe:        calcMedian(stocks, 'pe'),
        peg:       calcMedian(stocks, 'peg'),
        pb:        calcMedian(stocks, 'pb'),
        ev_ebitda: calcMedian(stocks, 'ev_ebitda'),
        fcf_yield: calcMedian(stocks, 'fcf_yield'),
      };
    }
    function sArrow(val, median, higherIsBetter = false) {
      if (val == null || median == null) return '';
      const under = higherIsBetter ? val > median : val < median;
      return under
        ? '<span class="val-arrow arrow-undervalued" title="Undervalued vs sector peers">▲</span>'
        : '<span class="val-arrow arrow-overvalued" title="Overvalued vs sector peers">▼</span>';
    }

    body.innerHTML = data.map(s => {
      const m = sectorMedians[s.sector] || {};
      return `
      <tr data-ticker="${s.ticker}">
        <td><strong>${s.ticker}</strong></td>
        <td>${s.name}</td>
        <td>${s.sector || "—"}</td>
        <td>${s.price != null ? "$" + s.price : "—"}</td>
        <td>${s.pe ?? "—"}${sArrow(s.pe, m.pe)}</td>
        <td>${s.peg ?? "—"}${sArrow(s.peg, m.peg)}</td>
        <td>${s.pb ?? "—"}${sArrow(s.pb, m.pb)}</td>
        <td>${s.ev_ebitda ?? "—"}${sArrow(s.ev_ebitda, m.ev_ebitda)}</td>
        <td>${s.fcf_yield != null ? s.fcf_yield + "%" : "—"}${sArrow(s.fcf_yield, m.fcf_yield, true)}</td>
        <td>${fmt(s.market_cap)}</td>
        <td><button class="btn-icon" onclick="addToWatchlist(event,'${s.ticker}')">+ Watch</button></td>
      </tr>
    `}).join("");

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
    const res = await authFetch(`${API}/api/watchlist`);
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
    await authFetch(`${API}/api/watchlist/${ticker}`, { method: "POST" });
    btn.textContent = "✓ Added";
    btn.classList.add("added");
  } catch (err) {
    btn.disabled = false;
  }
}

async function removeFromWatchlist(e, ticker) {
  e.stopPropagation();
  await authFetch(`${API}/api/watchlist/${ticker}`, { method: "DELETE" });
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
    // Start peers fetch in background — don't block stock panel from rendering
    authFetch(`${API}/api/stock/${ticker}/peers`)
      .then(r => r.ok ? r.json() : null)
      .then(peers => {
        if (peers && peers.comparison) {
          setValArrow("arrow-pe",  peers.comparison.pe);
          setValArrow("arrow-peg", peers.comparison.peg);
          setValArrow("arrow-pb",  peers.comparison.pb);
          setValArrow("arrow-ev",  peers.comparison.ev_ebitda);
          setValArrow("arrow-fcf", peers.comparison.fcf_yield);
        }
      })
      .catch(() => {});

    const res = await authFetch(`${API}/api/stock/${ticker}`);
    const d = await res.json();

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
      await authFetch(`${API}/api/watchlist/${ticker}`, { method: "POST" });
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
    const res = await authFetch(`${API}/api/recommend`, {
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
    const res = await authFetch(`${API}/api/predictions`);
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
    let varianceStr = '<span class="result-pending">—</span>';
    let resultStr = '<span class="result-pending">—</span>';
    if (p.actual_pct != null) {
      const actCls = p.actual_pct >= 0 ? "change-pos" : "change-neg";
      actualStr = `<span class="${actCls}">${p.actual_pct >= 0 ? "+" : ""}${p.actual_pct.toFixed(2)}%</span>`;
      const variance = p.actual_pct - p.predicted_pct;
      const varCls = variance >= 0 ? "change-pos" : "change-neg";
      varianceStr = `<span class="${varCls}">${variance >= 0 ? "+" : ""}${variance.toFixed(2)}%</span>`;
      const correct = (p.predicted_pct > 0) === (p.actual_pct > 0);
      resultStr = correct
        ? '<span class="result-correct">✓ Correct</span>'
        : '<span class="result-wrong">✗ Wrong</span>';
    }

    const isPending = p.confidence === "pending" || p.predicted_pct == null;
    const direction = isPending ? "" : (p.predicted_pct >= 0 ? "▲ BULLISH" : "▼ BEARISH");
    const dirClass  = isPending ? "" : (p.predicted_pct >= 0 ? "dir-bull" : "dir-bear");
    const confBadge = isPending
      ? `<span class="badge-pending">NOT ANALYSED</span>`
      : `<span class="badge-${p.confidence || 'medium'}">${(p.confidence || 'medium').toUpperCase()}</span> <span class="${dirClass}">${direction}</span>`;

    return `
      <tr>
        <td>${p.date}</td>
        <td><strong>${p.ticker}</strong></td>
        <td style="color:var(--text-muted);font-size:0.85rem">${safe(p.name || "—")}</td>
        <td>${predStr}</td>
        <td>${actualStr}</td>
        <td>${varianceStr}</td>
        <td>${resultStr}</td>
        <td>${confBadge}</td>
        <td class="reasoning-cell">${safe(p.reasoning || "—")}</td>
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
    const res = await authFetch(`${API}/api/predictions/generate`, { method: "POST" });
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

// ── Backtest ───────────────────────────────────────────────────

document.getElementById("btn-backtest").addEventListener("click", async () => {
  const btn    = document.getElementById("btn-backtest");
  const status = document.getElementById("backtest-status");
  const summary  = document.getElementById("backtest-summary");
  const byTicker = document.getElementById("backtest-by-ticker");
  const tableWrap = document.getElementById("backtest-table-wrap");

  btn.disabled = true;
  status.textContent = "Running backtest… fetching 4 weeks of historical data (this may take 30–60 seconds)…";
  summary.classList.add("hidden");
  byTicker.classList.add("hidden");
  tableWrap.classList.add("hidden");

  try {
    const res  = await authFetch(`${API}/api/predictions/backtest`);
    const data = await res.json();
    status.textContent = "";

    const s = data.summary;
    const accCls = s.accuracy_pct >= 60 ? "change-pos" : s.accuracy_pct >= 50 ? "" : "change-neg";
    summary.innerHTML = `
      <div class="acc-item"><span class="acc-label">Directional Accuracy</span><span class="acc-value ${accCls}">${s.accuracy_pct}%</span></div>
      <div class="acc-item"><span class="acc-label">Total Days Tested</span><span class="acc-value">${s.total}</span></div>
      <div class="acc-item"><span class="acc-label">Correct Direction</span><span class="acc-value">${s.correct}</span></div>
      <div class="acc-item"><span class="acc-label">Avg Abs Variance</span><span class="acc-value">±${s.avg_abs_variance}%</span></div>
      <div class="acc-item"><span class="acc-label">Avg Predicted</span><span class="acc-value">${s.avg_predicted >= 0 ? "+" : ""}${s.avg_predicted}%</span></div>
      <div class="acc-item"><span class="acc-label">Avg Actual</span><span class="acc-value">${s.avg_actual >= 0 ? "+" : ""}${s.avg_actual}%</span></div>
    `;
    summary.classList.remove("hidden");

    // Per-ticker breakdown
    const tickers = Object.entries(data.by_ticker).sort((a, b) => b[1].accuracy_pct - a[1].accuracy_pct);
    byTicker.innerHTML = `<h4 style="margin:1rem 0 0.5rem">Accuracy by Stock</h4>
      <div class="backtest-ticker-grid">` +
      tickers.map(([ticker, t]) => {
        const cls = t.accuracy_pct >= 60 ? "change-pos" : t.accuracy_pct >= 50 ? "" : "change-neg";
        return `<div class="backtest-ticker-card">
          <strong>${ticker}</strong>
          <span class="${cls}">${t.accuracy_pct}%</span>
          <small>${t.correct}/${t.total} · ±${t.avg_abs_variance}% var</small>
        </div>`;
      }).join("") + `</div>`;
    byTicker.classList.remove("hidden");

    // Detail table
    const body = document.getElementById("backtest-body");
    body.innerHTML = data.results.map(r => {
      const pCls = r.predicted_pct >= 0 ? "change-pos" : "change-neg";
      const aCls = r.actual_pct    >= 0 ? "change-pos" : "change-neg";
      const vCls = r.variance      >= 0 ? "change-pos" : "change-neg";
      const sp5Cls = r.sp_5d_chg  >= 0 ? "change-pos" : "change-neg";
      const resCls = r.correct ? "result-correct" : "result-wrong";
      return `<tr>
        <td>${r.date}</td>
        <td><strong>${r.ticker}</strong></td>
        <td>${r.vix}</td>
        <td><span class="${sp5Cls}">${r.sp_5d_chg >= 0 ? "+" : ""}${r.sp_5d_chg}%</span></td>
        <td><span class="${r.sentiment_score >= 0 ? "change-pos" : "change-neg"}">${r.sentiment_score >= 0 ? "+" : ""}${r.sentiment_score}%</span></td>
        <td><span class="${r.fund_adj >= 0 ? "change-pos" : "change-neg"}">${r.fund_adj >= 0 ? "+" : ""}${r.fund_adj}%</span></td>
        <td><span class="${pCls}">${r.predicted_pct >= 0 ? "+" : ""}${r.predicted_pct}%</span></td>
        <td><span class="${aCls}">${r.actual_pct >= 0 ? "+" : ""}${r.actual_pct}%</span></td>
        <td><span class="${vCls}">${r.variance >= 0 ? "+" : ""}${r.variance}%</span></td>
        <td><span class="${resCls}">${r.correct ? "✓" : "✗"}</span></td>
      </tr>`;
    }).join("");
    tableWrap.classList.remove("hidden");

  } catch (err) {
    status.textContent = "Error running backtest: " + err.message;
  } finally {
    btn.disabled = false;
  }
});

// ── P&L Simulator ─────────────────────────────────────────────

let simChart = null;

document.getElementById("btn-simulate").addEventListener("click", async () => {
  const btn    = document.getElementById("btn-simulate");
  const status = document.getElementById("sim-status");
  const results = document.getElementById("sim-results");

  btn.disabled = true;
  status.textContent = "Running simulator… fetching 4 weeks of price data and running 1,000 Monte Carlo simulations (30–60 seconds)…";
  results.classList.add("hidden");

  try {
    const res  = await authFetch(`${API}/api/predictions/simulate`);
    const data = await res.json();

    if (data.error) { status.textContent = data.error; return; }
    status.textContent = "";

    const s  = data.stats;
    const mc = data.monte_carlo;
    const fmt = (n) => "£" + Math.abs(n).toLocaleString("en-GB", { minimumFractionDigits: 0, maximumFractionDigits: 0 });
    const fmtPct = (n) => (n >= 0 ? "+" : "") + n.toFixed(1) + "%";

    // Stats bar
    const retCls = s.hist_return_pct >= 0 ? "change-pos" : "change-neg";
    document.getElementById("sim-stats").innerHTML = `
      <div class="acc-item"><span class="acc-label">Start Capital</span><span class="acc-value">${fmt(s.initial_float)}</span></div>
      <div class="acc-item"><span class="acc-label">After ${s.hist_weeks}wk (real)</span><span class="acc-value ${retCls}">${fmt(s.hist_final_value)} (${fmtPct(s.hist_return_pct)})</span></div>
      <div class="acc-item"><span class="acc-label">Win Rate</span><span class="acc-value">${s.win_rate_pct}%</span></div>
      <div class="acc-item"><span class="acc-label">Avg Win</span><span class="acc-value change-pos">+${s.avg_win_pct}%</span></div>
      <div class="acc-item"><span class="acc-label">Avg Loss</span><span class="acc-value change-neg">-${s.avg_loss_pct}%</span></div>
      <div class="acc-item"><span class="acc-label">Trades/Day</span><span class="acc-value">${s.avg_trades_per_day}</span></div>
    `;

    // Monte Carlo outcome cards
    const probCls = mc.prob_target_pct >= 50 ? "change-pos" : mc.prob_target_pct >= 25 ? "" : "change-neg";
    document.getElementById("sim-mc-cards").innerHTML = `
      <div class="sim-mc-card sim-mc-pessimist"><span class="sim-mc-label">Pessimistic (10th %ile)</span><span class="sim-mc-val change-neg">${fmt(mc.p10)}</span><small>${fmtPct((mc.p10 - s.initial_float) / s.initial_float * 100)}</small></div>
      <div class="sim-mc-card sim-mc-median"><span class="sim-mc-label">Median (50th %ile)</span><span class="sim-mc-val">${fmt(mc.p50)}</span><small>${fmtPct((mc.p50 - s.initial_float) / s.initial_float * 100)}</small></div>
      <div class="sim-mc-card sim-mc-optimist"><span class="sim-mc-label">Optimistic (90th %ile)</span><span class="sim-mc-val change-pos">${fmt(mc.p90)}</span><small>${fmtPct((mc.p90 - s.initial_float) / s.initial_float * 100)}</small></div>
      <div class="sim-mc-card sim-mc-prob"><span class="sim-mc-label">Prob. of hitting ${fmt(s.target)}</span><span class="sim-mc-val ${probCls}">${mc.prob_target_pct}%</span><small>across 1,000 sims</small></div>
    `;

    // Build chart datasets — Monte Carlo projection paths
    // Project median path (sample_paths[0] approximates median-ish)
    const projDays   = Array.from({length: mc.n_days + 1}, (_, i) => `Day ${i}`);
    const p10Path    = mc.sample_paths[0] || [];
    const p50Path    = mc.sample_paths[4] || [];
    const p90Path    = mc.sample_paths[9] || [];

    if (simChart) simChart.destroy();
    const ctx = document.getElementById("sim-chart").getContext("2d");
    simChart = new Chart(ctx, {
      type: "line",
      data: {
        labels: projDays,
        datasets: [
          { label: "Optimistic (90th)", data: p90Path, borderColor: "rgba(34,197,94,0.7)", backgroundColor: "rgba(34,197,94,0.05)", borderWidth: 1.5, pointRadius: 0, fill: false },
          { label: "Sample median", data: p50Path, borderColor: "rgba(79,142,247,0.9)", backgroundColor: "rgba(79,142,247,0.08)", borderWidth: 2, pointRadius: 0, fill: false },
          { label: "Pessimistic (10th)", data: p10Path, borderColor: "rgba(239,68,68,0.7)", backgroundColor: "rgba(239,68,68,0.05)", borderWidth: 1.5, pointRadius: 0, fill: false },
        ],
      },
      options: {
        responsive: true,
        plugins: {
          legend: { labels: { color: "#e8eaf0" } },
          tooltip: {
            callbacks: { label: ctx => "£" + Math.round(ctx.raw).toLocaleString("en-GB") },
          },
          annotation: {
            annotations: {
              target: {
                type: "line", yMin: s.target, yMax: s.target,
                borderColor: "rgba(250,204,21,0.8)", borderWidth: 1.5, borderDash: [6, 4],
                label: { content: `Target £${s.target.toLocaleString("en-GB")}`, enabled: true, color: "#fbbf24", backgroundColor: "transparent" },
              },
            },
          },
        },
        scales: {
          x: { ticks: { color: "#7b82a0", maxTicksLimit: 12 }, grid: { color: "#2e3350" } },
          y: {
            ticks: { color: "#7b82a0", callback: v => "£" + Math.round(v / 1000) + "k" },
            grid: { color: "#2e3350" },
          },
        },
      },
    });

    results.classList.remove("hidden");
  } catch (err) {
    status.textContent = "Simulator error: " + err.message;
  } finally {
    btn.disabled = false;
  }
});

// ── Recommendations ───────────────────────────────────────────

async function loadRecommendations() {
  const status = document.getElementById("rec-status");
  status.textContent = "Loading recommendations…";
  try {
    const res  = await authFetch(`${API}/api/recommendations`);
    const data = await res.json();
    status.textContent = "";
    renderRecommendations(data);
  } catch (err) {
    status.textContent = "Error: " + err.message;
  }
}

function fmt(n, prefix = "£") {
  if (n == null) return "—";
  return prefix + Math.abs(n).toLocaleString("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function renderRecommendations(data) {
  const s = data.summary || {};
  const buys  = data.buys  || [];
  const sells = data.sells || [];
  const empty = document.getElementById("rec-empty");

  // ── Progress card ─────────────────────────────────────────────
  if (s.initial_float) {
    const card = document.getElementById("rec-progress-card");
    card.classList.remove("hidden");

    const pnlCls = s.total_pnl >= 0 ? "change-pos" : "change-neg";
    document.getElementById("rec-total-value").textContent  = fmt(s.total_portfolio_value);
    document.getElementById("rec-invested").textContent     = fmt(s.total_invested);
    document.getElementById("rec-cash").textContent         = fmt(s.available_cash);
    document.getElementById("rec-pnl").innerHTML            = `<span class="${pnlCls}">${s.total_pnl >= 0 ? "+" : ""}${fmt(s.total_pnl)}</span>`;
    document.getElementById("rec-target").textContent       = fmt(s.target) + ` (${s.target_months}mo)`;
    document.getElementById("rec-remaining").textContent    = fmt(s.remaining_to_target);
    document.getElementById("rec-progress-label").textContent = `${s.progress_pct}% of target reached`;
    document.getElementById("rec-progress-date").textContent  = data.prediction_date ? `Based on predictions: ${data.prediction_date}` : "";

    const fillPct = Math.min(s.progress_pct, 100);
    const fillEl  = document.getElementById("rec-progress-fill");
    fillEl.style.width = fillPct + "%";
    fillEl.className   = "rec-progress-fill" + (fillPct >= 100 ? " rec-progress-complete" : fillPct >= 75 ? " rec-progress-good" : "");
  }

  // ── Sells ─────────────────────────────────────────────────────
  const sellsWrap = document.getElementById("rec-sells-wrap");
  const sellsBody = document.getElementById("rec-sells-body");
  if (sells.length > 0) {
    sellsWrap.classList.remove("hidden");
    sellsBody.innerHTML = sells.map(s => {
      const pnlCls   = s.unrealised_pnl >= 0 ? "change-pos" : "change-neg";
      const predStr  = s.predicted_pct != null ? `<span class="${s.predicted_pct >= 0 ? "change-pos" : "change-neg"}">${s.predicted_pct >= 0 ? "+" : ""}${s.predicted_pct.toFixed(2)}%</span>` : "—";
      const trigCls  = s.trigger === "STOP LOSS" ? "badge-low" : s.trigger === "TAKE PROFIT" ? "badge-high" : "badge-medium";
      return `<tr>
        <td><strong>${safe(s.ticker)}</strong></td>
        <td style="color:var(--text-muted);font-size:0.85rem">${safe(s.name)}</td>
        <td><span class="${trigCls}">${safe(s.trigger)}</span></td>
        <td>${s.qty}</td>
        <td>${fmt(s.current_price)}</td>
        <td><strong>${fmt(s.estimated_proceeds)}</strong></td>
        <td><span class="${pnlCls}">${s.unrealised_pnl >= 0 ? "+" : ""}${fmt(s.unrealised_pnl)} (${s.unrealised_pct >= 0 ? "+" : ""}${s.unrealised_pct.toFixed(1)}%)</span></td>
        <td>${predStr}</td>
        <td class="reasoning-cell">${safe(s.reasoning)}</td>
      </tr>`;
    }).join("");
  } else {
    sellsWrap.classList.add("hidden");
  }

  // ── Buys ──────────────────────────────────────────────────────
  const buysWrap = document.getElementById("rec-buys-wrap");
  const buysBody = document.getElementById("rec-buys-body");
  if (buys.length > 0) {
    buysWrap.classList.remove("hidden");
    buysBody.innerHTML = buys.map((b, i) => {
      const accStr = b.accuracy_pct != null ? `${b.accuracy_pct}%` : "<span style='color:var(--text-muted)'>No data</span>";
      return `<tr>
        <td style="color:var(--text-muted)">#${i + 1}</td>
        <td><strong>${safe(b.ticker)}</strong></td>
        <td style="color:var(--text-muted);font-size:0.85rem">${safe(b.name)}</td>
        <td><span class="badge-${b.confidence}">${b.confidence.toUpperCase()}</span></td>
        <td>${accStr}</td>
        <td><span class="change-pos">+${b.predicted_pct.toFixed(2)}%</span></td>
        <td>${fmt(b.current_price)}</td>
        <td><strong>${b.qty}</strong></td>
        <td><strong>${fmt(b.estimated_cost)}</strong></td>
        <td class="reasoning-cell">${safe(b.reasoning)}</td>
      </tr>`;
    }).join("");
  } else {
    buysWrap.classList.add("hidden");
  }

  if (buys.length === 0 && sells.length === 0) {
    empty.classList.remove("hidden");
  } else {
    empty.classList.add("hidden");
  }
}

document.getElementById("btn-load-recs").addEventListener("click", loadRecommendations);

// ── Alerts ────────────────────────────────────────────────────

async function loadAlerts() {
  const status = document.getElementById("alerts-status");
  status.textContent = "Loading…";
  try {
    const [alertsRes, statusRes] = await Promise.all([
      authFetch(`${API}/api/alerts`),
      authFetch(`${API}/api/alerts/status`),
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
      `<span class="signal-tag signal-${s.type}">${safe(s.signal)}</span>`
    ).join(" ");
    const type = signals.map(s => SIGNAL_LABELS[s.type] || s.type).join(", ");

    const changePct = primarySignal.change_pct;
    const priceHtml = `$${a.price?.toFixed(2) ?? "—"}` +
      (changePct != null ? ` <span class="${changePct >= 0 ? "change-pos" : "change-neg"}">${changePct >= 0 ? "+" : ""}${changePct.toFixed(2)}%</span>` : "");

    return `
      <tr>
        <td style="white-space:nowrap;font-size:0.82rem">${time}</td>
        <td><strong>${safe(a.ticker)}</strong><br><span style="color:var(--text-muted);font-size:0.78rem">${safe(a.name || "")}</span></td>
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
    const res = await authFetch(`${API}/api/alerts/test`, { method: "POST" });
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
  await authFetch(`${API}/api/alerts`, { method: "DELETE" });
  loadAlerts();
});

// ── Portfolio ─────────────────────────────────────────────────

function pnlHtml(val) {
  if (val == null) return "<span>—</span>";
  const sign = val >= 0 ? "+" : "";
  const cls  = val >= 0 ? "change-pos" : "change-neg";
  return `<span class="${cls}">${sign}$${Math.abs(val).toLocaleString("en-US", {minimumFractionDigits: 2, maximumFractionDigits: 2})}</span>`;
}

async function loadPortfolio() {
  const status  = document.getElementById("portfolio-status");
  const body    = document.getElementById("portfolio-body");
  const empty   = document.getElementById("portfolio-empty");
  const summary = document.getElementById("portfolio-summary");

  status.textContent = "Loading…";
  try {
    const res  = await authFetch(`${API}/api/portfolio`);
    const data = await res.json();
    status.textContent = "";

    const { positions, summary: s } = data;

    if (!positions || positions.length === 0) {
      body.innerHTML = "";
      empty.classList.add("visible");
      summary.classList.add("hidden");
      return;
    }

    empty.classList.remove("visible");
    summary.classList.remove("hidden");

    document.getElementById("port-invested").textContent    = "$" + s.total_invested.toLocaleString("en-US", {minimumFractionDigits: 2, maximumFractionDigits: 2});
    document.getElementById("port-current").textContent     = "$" + s.total_current_value.toLocaleString("en-US", {minimumFractionDigits: 2, maximumFractionDigits: 2});
    document.getElementById("port-unrealised").innerHTML    = pnlHtml(s.total_unrealised_pnl);
    document.getElementById("port-realised").innerHTML      = pnlHtml(s.total_realised_pnl);
    const totalEl = document.getElementById("port-total");
    totalEl.innerHTML = pnlHtml(s.total_pnl);

    body.innerHTML = positions.map(p => `
      <tr data-ticker="${p.ticker}">
        <td><strong>${p.ticker}</strong></td>
        <td style="color:var(--text-muted);font-size:0.85rem">${p.name}</td>
        <td>${p.shares}</td>
        <td>$${p.avg_cost.toFixed(2)}</td>
        <td>$${p.current_price.toFixed(2)}</td>
        <td>$${p.cost_basis.toLocaleString("en-US", {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
        <td>$${p.current_value.toLocaleString("en-US", {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
        <td>${pnlHtml(p.unrealised_pnl)} <span style="color:var(--text-muted);font-size:0.78rem">(${p.unrealised_pct >= 0 ? "+" : ""}${p.unrealised_pct}%)</span></td>
        <td>${pnlHtml(p.realised_pnl)}</td>
      </tr>
    `).join("");

    body.querySelectorAll("tr").forEach(row => {
      row.addEventListener("click", () => openDetail(row.dataset.ticker));
    });

  } catch (err) {
    status.textContent = "Error loading portfolio. Is the backend running?";
  }
}

async function submitTrade(type) {
  const ticker = document.getElementById("trade-ticker").value.trim().toUpperCase();
  const qty    = parseFloat(document.getElementById("trade-qty").value);
  const price  = parseFloat(document.getElementById("trade-price").value);
  const date   = document.getElementById("trade-date").value;
  const status = document.getElementById("portfolio-status");

  if (!ticker || !qty || !price) {
    status.textContent = "Please enter ticker, quantity and price.";
    return;
  }

  const btnBuy  = document.getElementById("btn-buy");
  const btnSell = document.getElementById("btn-sell");
  btnBuy.disabled = btnSell.disabled = true;
  status.textContent = `Recording ${type}…`;

  try {
    const res = await authFetch(`${API}/api/portfolio/${type}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ticker, qty, price, date: date || null }),
    });
    const data = await res.json();
    if (data.detail) {
      status.textContent = "Error: " + data.detail;
    } else {
      status.textContent = `${type === "buy" ? "Buy" : "Sell"} recorded for ${ticker}.`;
      document.getElementById("trade-ticker").value = "";
      document.getElementById("trade-qty").value    = "";
      document.getElementById("trade-price").value  = "";
      loadPortfolio();
    }
  } catch (err) {
    status.textContent = "Error: " + err.message;
  } finally {
    btnBuy.disabled = btnSell.disabled = false;
  }
}

document.getElementById("btn-buy").addEventListener("click",  () => submitTrade("buy"));
document.getElementById("btn-sell").addEventListener("click", () => submitTrade("sell"));
document.getElementById("btn-refresh-portfolio").addEventListener("click", loadPortfolio);

document.getElementById("btn-import-portfolio").addEventListener("click", () => {
  document.getElementById("import-file-input").click();
});

// ── Saxo PDF import ───────────────────────────────────────────
document.getElementById("btn-import-pdf").addEventListener("click", () => {
  document.getElementById("import-pdf-input").click();
});

document.getElementById("import-pdf-input").addEventListener("change", async (e) => {
  const file   = e.target.files[0];
  const status = document.getElementById("portfolio-status");
  if (!file) return;

  status.textContent = "Reading PDF… this may take 15–30 seconds while Claude parses it…";

  const form = new FormData();
  form.append("file", file);

  try {
    const res  = await authFetch(`${API}/api/portfolio/import-pdf`, { method: "POST", body: form });
    const data = await res.json();
    status.textContent = "";

    if (!res.ok) {
      status.textContent = "PDF import failed: " + (data.detail || "Unknown error");
      return;
    }

    if (data.imported === 0 && data.skipped === 0) {
      status.textContent = data.message || "No transactions found in this PDF.";
      return;
    }

    // Show preview modal
    const overlay  = document.getElementById("pdf-import-overlay");
    const tbody    = document.getElementById("pdf-preview-body");
    const summary  = document.getElementById("pdf-import-summary");
    const errorsEl = document.getElementById("pdf-import-errors");

    summary.textContent = `${data.imported} transaction(s) imported successfully.` +
      (data.skipped > 0 ? ` ${data.skipped} skipped.` : "");

    tbody.innerHTML = (data.preview || []).map(r => {
      const typeCls = r.type === "buy" ? "change-pos" : "change-neg";
      return `<tr>
        <td><span class="${typeCls}">${safe(r.type.toUpperCase())}</span></td>
        <td><strong>${safe(r.ticker)}</strong></td>
        <td style="color:var(--text-muted);font-size:0.85rem">${safe(r.name)}</td>
        <td>${r.qty}</td>
        <td>$${r.price.toFixed(2)}</td>
        <td>${safe(r.date)}</td>
      </tr>`;
    }).join("");

    errorsEl.innerHTML = data.errors.length > 0
      ? safe("<strong>Skipped rows:</strong><br>" + data.errors.map(e => String(e)).join("<br>"))
      : "";

    overlay.classList.remove("hidden");
    loadPortfolio();
  } catch (err) {
    status.textContent = "PDF import error: " + err.message;
  } finally {
    e.target.value = "";
  }
});

document.getElementById("btn-pdf-cancel").addEventListener("click", () => {
  document.getElementById("pdf-import-overlay").classList.add("hidden");
});
document.getElementById("btn-pdf-confirm").addEventListener("click", () => {
  document.getElementById("pdf-import-overlay").classList.add("hidden");
  document.getElementById("portfolio-status").textContent = "Portfolio updated from PDF import.";
});

document.getElementById("import-file-input").addEventListener("change", async (e) => {
  const file   = e.target.files[0];
  const status = document.getElementById("portfolio-status");
  if (!file) return;

  status.textContent = "Importing…";
  const form = new FormData();
  form.append("file", file);

  try {
    const res  = await authFetch(`${API}/api/portfolio/import`, { method: "POST", body: form });
    const data = await res.json();
    if (!res.ok) {
      status.textContent = "Import failed: " + (data.detail || "Unknown error");
      return;
    }
    let msg = `Imported ${data.imported} transaction(s).`;
    if (data.skipped > 0) msg += ` ${data.skipped} row(s) skipped: ${data.errors.join("; ")}`;
    status.textContent = msg;
    loadPortfolio();
  } catch (err) {
    status.textContent = "Import error: " + err.message;
  } finally {
    e.target.value = "";  // reset so same file can be re-imported if needed
  }
});
