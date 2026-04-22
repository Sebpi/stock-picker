const API = "";

// ── Tooltip system (fixed-position, never clipped) ────────────
(function () {
  let popup = null;
  document.addEventListener("mouseover", e => {
    const tip = e.target.closest(".tip");
    if (!tip) return;
    const text = tip.getAttribute("data-tip");
    if (!text) return;
    if (!popup) {
      popup = document.createElement("div");
      popup.className = "tip-popup";
      document.body.appendChild(popup);
    }
    popup.textContent = text;
    popup.classList.remove("visible");
    const rect = tip.getBoundingClientRect();
    const pw = 300;
    let left = rect.left + rect.width / 2 - pw / 2;
    let top  = rect.bottom + 10;
    left = Math.max(10, Math.min(left, window.innerWidth - pw - 10));
    popup.style.left = left + "px";
    popup.style.top  = top + "px";
    requestAnimationFrame(() => {
      const ph = popup.offsetHeight;
      if (top + ph > window.innerHeight - 10) {
        popup.style.top = Math.max(10, rect.top - ph - 10) + "px";
      }
      popup.classList.add("visible");
    });
  });
  document.addEventListener("mouseout", e => {
    const tip = e.target.closest(".tip");
    if (!tip || !popup) return;
    popup.classList.remove("visible");
  });
})();

// ── Loading overlay ───────────────────────────────────────────
const _loaderMsgs = [
  "Crunching the numbers…",
  "Consulting the oracle…",
  "Bribing the market makers…",
  "Asking the bull and the bear…",
  "Reading the tea leaves…",
  "Scanning 10-Ks at warp speed…",
  "Waking up the quants…",
  "Calculating alpha…",
  "Checking under the sofa for returns…",
  "Decoding Fed speak…",
  "Negotiating with yfinance…",
  "Running the DCF… again…",
  "Trying to time the market (irresponsibly)…",
  "Praying to the chart gods…",
];
let _loaderTimer = null;
let _loaderMsgTimer = null;

function showLoader(msg) {
  const overlay = document.getElementById("loader-overlay");
  const msgEl   = document.getElementById("loader-msg");
  if (!overlay) return;
  overlay.classList.add("active");
  msgEl.textContent = msg || _loaderMsgs[Math.floor(Math.random() * _loaderMsgs.length)];
  // Rotate messages every 3s
  _loaderMsgTimer = setInterval(() => {
    msgEl.style.animation = "none";
    requestAnimationFrame(() => {
      msgEl.style.animation = "";
      msgEl.textContent = _loaderMsgs[Math.floor(Math.random() * _loaderMsgs.length)];
    });
  }, 3000);
}

function hideLoader() {
  const overlay = document.getElementById("loader-overlay");
  if (overlay) overlay.classList.remove("active");
  clearInterval(_loaderMsgTimer);
}

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

function describeRequestError(err, fallback) {
  const msg = err && err.message ? err.message : "";
  if (!msg) return fallback;
  if (msg.includes("Session expired")) return msg;
  return `${msg}. ${fallback}`;
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
    msgEl.style.display = "none";
    if (!u) {
      msgEl.textContent = "Enter your username first.";
      msgEl.style.display = "block";
      return;
    }
    try {
      const res = await fetch(`${API}/api/auth/forgot-password`, {
        method: "POST", headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ username: u })
      });
      const d = await res.json().catch(() => ({}));
      if (!res.ok) {
        msgEl.textContent = d.detail || d.message || `Reset request failed (${res.status}).`;
        msgEl.style.display = "block";
        return;
      }
      msgEl.textContent = d.message || "Reset link sent if the username exists.";
      msgEl.style.display = "block";
    } catch (err) {
      msgEl.textContent = err?.message || "Cannot connect to the password reset service.";
      msgEl.style.display = "block";
    }
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
let sentimentWatchlist = [];
let predictionReasoningMap = {};
let recReasoningMap = {};

const TAB_LOADERS = {
  watchlist: () => loadWatchlist(),
  predictions: () => loadPredictions(),
  recommendations: () => loadRecommendations(),
  alerts: () => loadAlerts(),
  portfolio: () => loadPortfolio(),
  backtest: () => {},
  sentiment: () => loadSentiment(),
  paper: () => loadPaperPortfolio(),
};
const SIGNAL_LABELS = { buy_opportunity: "BUY Opportunity", sell_signal: "SELL Signal", daily_swing: "Daily Swing", momentum: "Momentum", volume_surge: "Vol Surge" };

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

function fmtScreenerMarketCap(n) {
  if (n == null) return "â€”";
  const bn = n / 1e9;
  if (bn >= 100) return `${Math.round(bn)}BN`;
  if (bn >= 10) return `${bn.toFixed(1)}BN`;
  return `${bn.toFixed(2)}BN`;
}

function changeHtml(pct) {
  if (pct == null) return "<span>—</span>";
  const sign = pct >= 0 ? "+" : "";
  const cls = pct >= 0 ? "change-pos" : "change-neg";
  return `<span class="${cls}">${sign}${pct.toFixed(2)}%</span>`;
}

// ── Screener ──────────────────────────────────────────────────
function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatInlineMarkdown(text) {
  return escapeHtml(text)
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/`([^`]+)`/g, "<code>$1</code>");
}

function slugify(text) {
  return String(text || "section")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "section";
}

function looksLikeTableSeparator(line) {
  return /^\s*\|?[\s:-]+\|[\s|:-]*$/.test(line || "");
}

function parseMarkdownTable(lines, startIndex) {
  const tableLines = [];
  let i = startIndex;
  while (i < lines.length && /^\s*\|.+\|\s*$/.test(lines[i])) {
    tableLines.push(lines[i].trim());
    i += 1;
  }
  if (tableLines.length < 2 || !looksLikeTableSeparator(tableLines[1])) return null;

  const splitRow = line => line.split("|").slice(1, -1).map(cell => cell.trim());
  const headers = splitRow(tableLines[0]);
  const rows = tableLines.slice(2).map(splitRow).filter(row => row.some(Boolean));
  return { table: { headers, rows }, nextIndex: i };
}

function getSectionIcon(title = "") {
  const label = title.toLowerCase();
  if (label.includes("overview")) return "OV";
  if (label.includes("news") || label.includes("catalyst")) return "NW";
  if (label.includes("financial")) return "FN";
  if (label.includes("technical")) return "TA";
  if (label.includes("bull")) return "UP";
  if (label.includes("bear") || label.includes("risk")) return "RS";
  if (label.includes("verdict") || label.includes("outlook")) return "VD";
  if (label.includes("summary")) return "SM";
  return "RD";
}

const _NON_TICKER_WORDS = new Set([
  // Financial/accounting terms
  "TTM","YOY","QOQ","EPS","PE","PEG","ROE","ROA","EBIT","EBITDA","FCF","DCF","NAV",
  "IPO","ETF","REIT","SPV","AUM","AUM","MOM","YTD","WTD","MTD","CAGR","WACC","IRR",
  "NII","NIM","LTV","CET","RWA","NPL","NOI","FFO","AFFO","BPS","DPS","SPS","OCF",
  "CFO","CTO","CEO","COO","CFO","CIO","EVP","SVP","VP","MD","GM",
  // Common English words often uppercased in financial text
  "AI","ML","US","UK","EU","UN","GDP","CPI","PPI","PMI","ISM","FED","ECB","BOE","BOJ",
  "USD","GBP","EUR","JPY","CAD","AUD","CHF","HKD","CNY","INR",
  "Q1","Q2","Q3","Q4","H1","H2","FY","NY","LA","SF","DC",
  "S","P","A","B","C","E","R","T","I","N","M","F",
  "AND","FOR","THE","BUT","NOT","ARE","WAS","HAS","ITS","MAY","CAN","YET","NEW","NOW",
  "HIGH","LOW","BUY","SELL","HOLD","LONG","SHORT","CALL","PUT","OTC",
  "NOTE","NOTES","RISK","RISKS","DATA","RATE","RATES","DEBT","CASH","NET","GROSS",
]);

function extractTickersFromText(text) {
  const matches = String(text || "").match(/\b[A-Z]{1,5}(?:\.[A-Z])?\b/g) || [];
  return [...new Set(matches)].filter(t => !_NON_TICKER_WORDS.has(t) && t.length >= 2).slice(0, 6);
}

function extractEntityCards(rawText, sections) {
  const entities = [];
  const seen = new Set();

  sections.forEach(section => {
    const source = section.title || section.lines.join(" ");
    const ticker = (source.match(/\(([A-Z]{1,5}(?:\.[A-Z])?)\)/) || [])[1]
      || (section.title.match(/\b[A-Z]{1,5}(?:\.[A-Z])?\b/) || [])[0];
    if (!ticker || seen.has(ticker)) return;

    seen.add(ticker);
    const joined = section.lines.join("\n");
    const outlookMatch = joined.match(/\*\*Outlook\*\*:\s*([^\n]+)/i) || joined.match(/Outlook:\s*([^\n]+)/i);
    const catalystMatch = joined.match(/\*\*Key (?:thing to watch|catalyst)\*\*:\s*([^\n]+)/i)
      || joined.match(/Key (?:thing to watch|catalyst):\s*([^\n]+)/i);
    const priceMatch = joined.match(/\*\*Current Price\*\*:\s*([^\n]+)/i) || joined.match(/Current Price:\s*([^\n]+)/i);
    const marketCapMatch = joined.match(/\*\*Market Cap\*\*:\s*([^\n]+)/i) || joined.match(/Market Cap:\s*([^\n]+)/i);

    entities.push({
      ticker,
      title: section.title,
      outlook: outlookMatch?.[1]?.trim() || "Under review",
      catalyst: catalystMatch?.[1]?.trim() || "Watch upcoming catalysts and guidance.",
      price: priceMatch?.[1]?.trim() || "Price not parsed",
      marketCap: marketCapMatch?.[1]?.trim() || "Market cap not parsed",
    });
  });

  if (entities.length > 0) return entities.slice(0, 4);

  return extractTickersFromText(rawText).map(ticker => ({
    ticker,
    title: ticker,
    outlook: "Research report loaded",
    catalyst: "Scan the sections below for catalysts and risks.",
    price: "See report",
    marketCap: "See report",
  }));
}

function buildTickerArt(ticker, outlook = "") {
  const key = ticker.split("").reduce((sum, ch) => sum + ch.charCodeAt(0), 0);
  const hue = key % 360;
  const tone = /bull/i.test(outlook) ? "#22c55e" : /bear/i.test(outlook) ? "#ef4444" : "#4f8ef7";
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="320" height="180" viewBox="0 0 320 180">
      <defs>
        <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stop-color="hsl(${hue} 75% 18%)" />
          <stop offset="100%" stop-color="#111727" />
        </linearGradient>
        <linearGradient id="line" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stop-color="#ffffff" stop-opacity="0.25" />
          <stop offset="100%" stop-color="${tone}" stop-opacity="0.95" />
        </linearGradient>
      </defs>
      <rect width="320" height="180" rx="28" fill="url(#bg)" />
      <circle cx="252" cy="42" r="34" fill="${tone}" opacity="0.18" />
      <circle cx="280" cy="18" r="54" fill="#ffffff" opacity="0.05" />
      <path d="M26 132 C62 130, 78 66, 118 84 S180 154, 224 102 S270 60, 294 72" fill="none" stroke="url(#line)" stroke-width="8" stroke-linecap="round"/>
      <text x="26" y="52" fill="#f8fafc" font-family="Arial, sans-serif" font-size="42" font-weight="700">${escapeHtml(ticker)}</text>
      <text x="28" y="154" fill="#cbd5e1" font-family="Arial, sans-serif" font-size="18">Research snapshot</text>
    </svg>
  `.trim();
  return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`;
}

function parseResearchResponse(rawText) {
  const lines = String(rawText || "").replace(/\r/g, "").split("\n");
  const sections = [];
  let current = { title: "Executive Summary", level: 2, lines: [] };

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    // Skip horizontal rules (--- or ***)
    if (/^\s*[-*]{3,}\s*$/.test(line)) continue;
    // Match h1-h4 headings (# through ####)
    const heading = line.match(/^(#{1,4})\s+(.+)/);
    if (heading) {
      if (current.lines.length || current.title) sections.push(current);
      current = { title: heading[2].trim(), level: Math.max(2, heading[1].length), lines: [] };
      continue;
    }
    current.lines.push(line);
  }
  if (current.lines.length || current.title) sections.push(current);

  sections.forEach(section => {
    const blocks = [];
    for (let i = 0; i < section.lines.length; i += 1) {
      const line = section.lines[i];
      if (!line.trim()) continue;

      const table = parseMarkdownTable(section.lines, i);
      if (table) {
        blocks.push({ type: "table", ...table.table });
        i = table.nextIndex - 1;
        continue;
      }

      const bulletMatch = line.match(/^\s*[-*]\s+(.+)/);
      if (bulletMatch) {
        const items = [bulletMatch[1]];
        while (i + 1 < section.lines.length) {
          const next = section.lines[i + 1].match(/^\s*[-*]\s+(.+)/);
          if (!next) break;
          items.push(next[1]);
          i += 1;
        }
        blocks.push({ type: "list", items });
        continue;
      }

      const keyValMatch = line.match(/^\s*\*\*(.+?)\*\*:\s*(.+)$/);
      if (keyValMatch) {
        blocks.push({ type: "metric", label: keyValMatch[1].trim(), value: keyValMatch[2].trim() });
        continue;
      }

      blocks.push({ type: "paragraph", text: line.trim() });
    }
    section.blocks = blocks;
  });

  return {
    sections,
    entities: extractEntityCards(rawText, sections),
  };
}

function renderResearchResponse(rawText, query) {
  const { sections } = parseResearchResponse(rawText);

  // Filter out sections with no real content (e.g. empty "Executive Summary" default)
  const visibleSections = sections.filter(s => s.blocks && s.blocks.length > 0);

  const sectionHtml = visibleSections.map(section => `
    <section class="research-block research-block-level-${section.level}">
      <div class="research-block-head">
        <span class="research-block-icon">${getSectionIcon(section.title)}</span>
        <h3 id="${slugify(section.title)}">${formatInlineMarkdown(section.title)}</h3>
      </div>
      <div class="research-block-body">
        ${section.blocks.map(block => {
          if (block.type === "metric") {
            return `<div class="research-metric"><span class="research-metric-label">${formatInlineMarkdown(block.label)}</span><span class="research-metric-value">${formatInlineMarkdown(block.value)}</span></div>`;
          }
          if (block.type === "list") {
            return `<ul class="research-list">${block.items.map(item => `<li>${formatInlineMarkdown(item)}</li>`).join("")}</ul>`;
          }
          if (block.type === "table") {
            return `<div class="research-table-wrap"><table class="research-table"><thead><tr>${block.headers.map(h => `<th>${formatInlineMarkdown(h)}</th>`).join("")}</tr></thead><tbody>${block.rows.map(row => `<tr>${row.map(cell => `<td>${formatInlineMarkdown(cell)}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
          }
          return `<p class="research-paragraph">${formatInlineMarkdown(block.text)}</p>`;
        }).join("")}
      </div>
    </section>
  `).join("");

  const toc = visibleSections.length > 2 ? `
    <div class="research-toc">
      ${visibleSections.map(s => `<a href="#${slugify(s.title)}">${formatInlineMarkdown(s.title)}</a>`).join("")}
    </div>
  ` : "";

  return safe(`
    <div class="research-rendered">
      <div class="research-banner">
        <div>
          <div class="research-banner-label">AI Research Report</div>
          <h2>${formatInlineMarkdown(query || "Stock research")}</h2>
        </div>
      </div>
      ${toc}
      <div class="research-sections">
        ${sectionHtml || `<p class="research-paragraph">${formatInlineMarkdown(rawText)}</p>`}
      </div>
    </div>
  `);
}

// Toggle ≤ / ≥ on filter operator buttons
document.querySelectorAll(".filter-op-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    if (btn.dataset.op === "max") {
      btn.dataset.op = "min";
      btn.textContent = "≥";
    } else {
      btn.dataset.op = "max";
      btn.textContent = "≤";
    }
  });
});

document.getElementById("btn-screen").addEventListener("click", runScreen);

document.getElementById("screener-search").addEventListener("input", function () {
  const q = this.value.trim().toLowerCase();
  const rows = document.querySelectorAll("#screen-body tr");
  if (rows.length === 0) return; // nothing loaded yet — wait for Enter
  let visible = 0;
  rows.forEach(row => {
    const ticker = (row.dataset.ticker || "").toLowerCase();
    const name   = row.cells[1]?.textContent.toLowerCase() || "";
    const match  = !q || ticker.includes(q) || name.includes(q);
    row.style.display = match ? "" : "none";
    if (match) visible++;
  });
  const status = document.getElementById("screen-status");
  status.textContent = q
    ? `${visible} of ${rows.length} stock${rows.length !== 1 ? "s" : ""} shown.`
    : `${rows.length} stock${rows.length !== 1 ? "s" : ""} found.`;
});

document.getElementById("screener-search").addEventListener("keydown", async function (e) {
  if (e.key !== "Enter") return;
  const q = this.value.trim();
  if (!q) return;
  const status = document.getElementById("screen-status");
  const body   = document.getElementById("screen-body");
  status.textContent = `Searching for "${q}"…`;
  body.innerHTML = "";
  showLoader(`Searching for "${q}"…`);
  try {
    const res  = await authFetch(`${API}/api/search?q=${encodeURIComponent(q)}`);
    const data = await res.json();
    if (data.length === 0) {
      status.textContent = `No stocks found matching "${q}".`;
      return;
    }
    status.textContent = `${data.length} result${data.length !== 1 ? "s" : ""} for "${q}".`;
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
        <td>${fmtScreenerMarketCap(s.market_cap)}</td>
        <td><button class="btn-icon" onclick="addToWatchlist(event,'${s.ticker}')">+ Watch</button></td>
      </tr>`).join("");
  } catch (err) {
    status.textContent = "Search failed. Please try again.";
  } finally {
    hideLoader();
  }
});

async function runScreen() {
  const btn = document.getElementById("btn-screen");
  const status = document.getElementById("screen-status");
  const body = document.getElementById("screen-body");

  const index  = document.getElementById("filter-index").value;
  const sector = document.getElementById("filter-sector").value;
  const query  = document.getElementById("screener-search").value.trim();

  function opParam(filterId, backendKey, scale) {
    const val = document.getElementById("filter-" + filterId).value;
    if (val === "") return null;
    const btn = document.querySelector(`.filter-op-btn[data-filter="${filterId}"]`);
    const op  = btn ? btn.dataset.op : "min";
    const num = parseFloat(val) * (scale || 1);
    return [op + "_" + backendKey, num];
  }

  const params = new URLSearchParams();
  if (index)  params.set("index", index);
  if (query)  params.set("q", query);
  if (sector) params.set("sector", sector);
  [
    opParam("pe",         "pe"),
    opParam("peg",        "peg"),
    opParam("pb",         "pb"),
    opParam("ev",         "ev_ebitda"),
    opParam("fcf",        "fcf_yield"),
    opParam("cap",        "market_cap", 1e9),
    opParam("vol",        "volume",     1e6),
    opParam("rev-growth", "rev_growth"),
  ].forEach(p => { if (p) params.set(p[0], p[1]); });

  btn.disabled = true;
  status.textContent = query ? `Screening stocks for "${query}"…` : "Screening stocks… this may take a moment.";
  body.innerHTML = "";
  showLoader(query ? `Screening for "${query}"…` : "Screening stocks…");

  try {
    const res = await authFetch(`${API}/api/screen?${params}`);
    const data = await res.json();

    if (data.length === 0) {
      status.textContent = query ? `No stocks matched "${query}" and your criteria.` : "No stocks matched your criteria.";
      return;
    }

    status.textContent = query
      ? `${data.length} stock${data.length !== 1 ? "s" : ""} found for "${query}".`
      : `${data.length} stock${data.length !== 1 ? "s" : ""} found.`;

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
        pe:         calcMedian(stocks, 'pe'),
        peg:        calcMedian(stocks, 'peg'),
        pb:         calcMedian(stocks, 'pb'),
        ev_ebitda:  calcMedian(stocks, 'ev_ebitda'),
        fcf_yield:  calcMedian(stocks, 'fcf_yield'),
        rev_growth: calcMedian(stocks, 'rev_growth'),
      };
    }
    function fmtMedian(val, isPct = false) {
      if (val == null) return "n/a";
      return isPct ? `${val}%` : `${val}`;
    }
    function escapeAttr(str) {
      return String(str)
        .replace(/&/g, "&amp;")
        .replace(/"/g, "&quot;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
    }
    function sArrow(val, median, higherIsBetter = false, label = "metric", sectorName = "sector") {
      if (val == null || median == null) return '';
      const under = higherIsBetter ? val > median : val < median;
      const title = `${under ? "Stronger" : "Weaker"} vs ${sectorName} median ${label}: ${fmtMedian(median, higherIsBetter)}`;
      return under
        ? `<span class="val-arrow arrow-undervalued" title="${escapeAttr(title)}">▲</span>`
        : `<span class="val-arrow arrow-overvalued" title="${escapeAttr(title)}">▼</span>`;
    }

    body.innerHTML = data.map(s => {
      const sectorName = s.sector || "Unknown";
      const m = sectorMedians[sectorName] || {};
      return `
      <tr data-ticker="${s.ticker}">
        <td><strong>${s.ticker}</strong></td>
        <td>${s.name}</td>
        <td>${sectorName || "—"}</td>
        <td>${s.price != null ? "$" + s.price : "—"}</td>
        <td>${s.pe ?? "—"}${sArrow(s.pe, m.pe, false, "P/E", sectorName)}</td>
        <td>${s.peg ?? "—"}${sArrow(s.peg, m.peg, false, "PEG", sectorName)}</td>
        <td>${s.pb ?? "—"}${sArrow(s.pb, m.pb, false, "P/B", sectorName)}</td>
        <td>${s.ev_ebitda ?? "—"}${sArrow(s.ev_ebitda, m.ev_ebitda, false, "EV/EBITDA", sectorName)}</td>
        <td>${s.fcf_yield != null ? s.fcf_yield + "%" : "—"}${sArrow(s.fcf_yield, m.fcf_yield, true, "FCF yield", sectorName)}</td>
        <td>${s.rev_growth != null ? s.rev_growth + "%" : "—"}${sArrow(s.rev_growth, m.rev_growth, true, "Revenue growth", sectorName)}</td>
        <td>${fmtScreenerMarketCap(s.market_cap)}</td>
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
    hideLoader();
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

async function loadSentiment() {
  const status = document.getElementById("sentiment-status");
  const result = document.getElementById("sentiment-result");
  status.textContent = "Loading watchlist...";
  result.innerHTML = '<div class="sentiment-empty-state">Loading your watchlist for sentiment analysis...</div>';
  try {
    const res = await authFetch(`${API}/api/sentiment?watchlist=true`);
    const data = await res.json();
    if (data.detail) {
      status.textContent = "Error: " + data.detail;
      result.innerHTML = '<div class="sentiment-empty-state">Watchlist could not be loaded.</div>';
      return;
    }
    sentimentWatchlist = Array.isArray(data.watchlist) ? data.watchlist : [];
    status.textContent = sentimentWatchlist.length > 0
      ? `Ready. ${sentimentWatchlist.length} watchlist stock${sentimentWatchlist.length !== 1 ? "s" : ""} available for scanning.`
      : "Your watchlist is empty.";
    result.innerHTML = renderSentimentWatchlist(data);
  } catch (err) {
    status.textContent = "Error: " + err.message;
    result.innerHTML = '<div class="sentiment-empty-state">Watchlist could not be loaded.</div>';
  }
}

function sentimentToneClass(sentiment, score = 0) {
  const label = String(sentiment || "").toLowerCase();
  if (label === "bullish") return "sentiment-positive";
  if (label === "bearish") return "sentiment-negative";
  if (label === "error") return "sentiment-negative";
  return "sentiment-neutral";
}

function sentimentScoreHtml(score) {
  const cls = score > 0 ? "change-pos" : score < 0 ? "change-neg" : "";
  const sign = score > 0 ? "+" : "";
  return `<span class="${cls}">${sign}${score}</span>`;
}

function renderSentimentWatchlist(data) {
  const watchlist = Array.isArray(data?.watchlist) ? data.watchlist : [];
  sentimentWatchlist = watchlist;
  if (watchlist.length === 0) {
    return '<div class="sentiment-empty-state">Your watchlist is empty. Add stocks first, then run a scan.</div>';
  }

  return `
    <div class="sentiment-list-wrap">
      <div class="sentiment-summary-card">
        <div class="sentiment-summary-label">Tracked symbols</div>
        <div class="sentiment-summary-value">${watchlist.length}</div>
      </div>
      <div class="sentiment-chip-row">
        ${watchlist.map(ticker => `<span class="sentiment-chip">${safe(ticker)}</span>`).join("")}
      </div>
    </div>
  `;
}

function renderSentimentResults(data, ticker) {
  const results = Array.isArray(data?.results) ? data.results : (data?.ticker ? [data] : []);
  if (results.length === 0) {
    return '<div class="sentiment-empty-state">No sentiment results returned yet.</div>';
  }

  const positive = results.filter(item => (item.sentiment_score || 0) > 0).length;
  const negative = results.filter(item => (item.sentiment_score || 0) < 0).length;
  const neutral = results.length - positive - negative;

  return `
    <div class="sentiment-dashboard">
      <div class="sentiment-summary-grid">
        <div class="sentiment-summary-card">
          <div class="sentiment-summary-label">${ticker ? "Ticker" : "Scanned"}</div>
          <div class="sentiment-summary-value">${ticker ? safe(ticker) : results.length}</div>
        </div>
        <div class="sentiment-summary-card">
          <div class="sentiment-summary-label">Bullish</div>
          <div class="sentiment-summary-value change-pos">${positive}</div>
        </div>
        <div class="sentiment-summary-card">
          <div class="sentiment-summary-label">Neutral</div>
          <div class="sentiment-summary-value">${neutral}</div>
        </div>
        <div class="sentiment-summary-card">
          <div class="sentiment-summary-label">Bearish</div>
          <div class="sentiment-summary-value change-neg">${negative}</div>
        </div>
      </div>
      <div class="sentiment-card-grid">
        ${results.map(item => `
          <article class="sentiment-card ${sentimentToneClass(item.sentiment, item.sentiment_score)}">
            <div class="sentiment-card-head">
              <div>
                <div class="sentiment-card-ticker">${safe(item.ticker || "N/A")}</div>
                <div class="sentiment-card-name">${safe(item.name || item.ticker || "Unknown")}</div>
              </div>
              <div class="sentiment-badge ${sentimentToneClass(item.sentiment, item.sentiment_score)}">${safe(item.sentiment || "neutral")}</div>
            </div>
            <div class="sentiment-metrics">
              <div class="sentiment-metric">
                <span class="sentiment-metric-label">Score</span>
                <strong>${sentimentScoreHtml(item.sentiment_score || 0)}</strong>
              </div>
              <div class="sentiment-metric">
                <span class="sentiment-metric-label">Price</span>
                <strong>${item.price != null ? `$${item.price}` : "—"}</strong>
              </div>
              <div class="sentiment-metric">
                <span class="sentiment-metric-label">Change</span>
                <strong>${item.change_pct != null ? changeHtml(item.change_pct) : "—"}</strong>
              </div>
              <div class="sentiment-metric">
                <span class="sentiment-metric-label">Analyst</span>
                <strong>${safe(item.recommendation || "n/a")}</strong>
              </div>
            </div>
            ${(item.headlines || []).length > 0 ? `
              <div class="sentiment-headlines">
                <div class="sentiment-headlines-title">Recent headlines</div>
                <ul>
                  ${(item.headlines || []).map(headline => `<li>${safe(headline)}</li>`).join("")}
                </ul>
              </div>
            ` : ""}
            ${item.error ? `<div class="sentiment-error">${safe(item.error)}</div>` : ""}
          </article>
        `).join("")}
      </div>
    </div>
  `;
}

document.getElementById("btn-sentiment-list").addEventListener("click", async () => {
  const status = document.getElementById("sentiment-status");
  const result = document.getElementById("sentiment-result");
  status.textContent = "Loading watchlist…";
  try {
    const res = await authFetch(`${API}/api/sentiment?watchlist=true`);
    const data = await res.json();
    if (data.detail) {
      status.textContent = "Error: " + data.detail;
      return;
    }
    status.textContent = "Watchlist loaded.";
    result.innerHTML = renderSentimentWatchlist(data);
  } catch (err) {
    status.textContent = "Error: " + err.message;
  }
});

async function runSentimentScan(ticker) {
  const status = document.getElementById("sentiment-status");
  const result = document.getElementById("sentiment-result");
  status.textContent = ticker ? `Scanning ticker ${ticker}…` : "Scanning watchlist…";
  result.innerHTML = '<div class="sentiment-empty-state">Scanning live sentiment data...</div>';
  if (!ticker && sentimentWatchlist.length === 0) {
    status.textContent = "Your watchlist is empty.";
    result.innerHTML = '<div class="sentiment-empty-state">Add stocks to the watchlist first, then run a watchlist scan.</div>';
    return;
  }
  if (!ticker) {
    status.textContent = `Scanning ${sentimentWatchlist.length} watchlist stock${sentimentWatchlist.length !== 1 ? "s" : ""}...`;
  }
  showLoader(status.textContent);
  try {
    const url = ticker
      ? `${API}/api/sentiment?ticker=${encodeURIComponent(ticker)}`
      : `${API}/api/sentiment`;
    const res = await authFetch(url);
    const data = await res.json();
    status.textContent = "Done.";
    result.innerHTML = renderSentimentResults(data, ticker);
  } catch (err) {
    status.textContent = "Error: " + err.message;
    result.innerHTML = '<div class="sentiment-empty-state">Sentiment data could not be loaded.</div>';
  } finally {
    hideLoader();
  }
}

document.getElementById("btn-sentiment-scan").addEventListener("click", () => runSentimentScan());

document.getElementById("btn-sentiment-ticker").addEventListener("click", () => {
  const ticker = document.getElementById("sentiment-ticker").value.trim().toUpperCase();
  if (!ticker) {
    document.getElementById("sentiment-status").textContent = "Enter a ticker symbol first.";
    return;
  }
  runSentimentScan(ticker);
});

async function addToWatchlist(e, ticker) {
  e.stopPropagation();
  const btn = e.target;
  btn.disabled = true;
  try {
    await authFetch(`${API}/api/watchlist/${ticker}`, { method: "POST" });
    btn.textContent = "✓ Added";
    btn.classList.add("added");
    if (document.getElementById("tab-sentiment")?.classList.contains("active")) {
      loadSentiment();
    }
  } catch (err) {
    btn.disabled = false;
  }
}

async function removeFromWatchlist(e, ticker) {
  e.stopPropagation();
  await authFetch(`${API}/api/watchlist/${ticker}`, { method: "DELETE" });
  loadWatchlist();
  if (document.getElementById("tab-sentiment")?.classList.contains("active")) {
    loadSentiment();
  }
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

  showLoader("Loading stock data…");
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
  } finally {
    hideLoader();
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
document.getElementById("btn-research").addEventListener("click", async () => {
  const query = document.getElementById("research-query").value.trim();
  if (!query) return;

  const btn = document.getElementById("btn-research");
  const status = document.getElementById("research-status");
  const response = document.getElementById("research-response");
  const loader = document.getElementById("research-loader");

  let stageTimer = null;
  const stages = [
    "Building research query from input...",
    "Collecting live fundamentals (yfinance)...",
    "Gathering live news, analyst and technical signals...",
    "Sending only the live data package to Claude...",
    "Compiling final narrative and risk summary...",
  ];
  let currentStage = 0;

  function advanceStage() {
    if (currentStage < stages.length) {
      status.textContent = stages[currentStage];
      currentStage += 1;
      stageTimer = setTimeout(advanceStage, 2400);
    }
  }

  btn.disabled = true;
  response.classList.remove("visible");
  loader.classList.remove("hidden");
  status.textContent = "Starting research flow…";
  currentStage = 0;
  advanceStage();

  try {
    const res = await authFetch(`${API}/api/stock-research`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query }),
    });
    let data;
    try {
      data = await res.json();
    } catch {
      const text = await res.text().catch(() => "Unknown error");
      throw new Error(res.status === 500 ? (text.includes("API key") ? "Invalid Anthropic API key — check your .env file" : "Backend error: " + text.slice(0, 120)) : text.slice(0, 120));
    }

    clearTimeout(stageTimer);
    loader.classList.add("hidden");

    if (data.detail) {
      status.textContent = "Error: " + data.detail;
      response.textContent = "";
    } else {
      status.textContent = "Research complete. Insights ready below.";
      response.innerHTML = renderResearchResponse(data.response, query);
      response.classList.add("visible");
    }
  } catch (err) {
    clearTimeout(stageTimer);
    loader.classList.add("hidden");
    status.textContent = "Error: " + err.message + ". Is the backend running?";
    response.textContent = "";
  } finally {
    btn.disabled = false;
  }
});

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

let predictionsSnapshotCache = null;
let predictionsRenderedCache = null;
const PREDICTIONS_RENDER_VERSION = 2;

function invalidatePredictionsSnapshotCache() {
  predictionsSnapshotCache = null;
  predictionsRenderedCache = null;
}

async function loadPredictions(forceRefresh = false) {
  const status = document.getElementById("pred-status");
  const body = document.getElementById("pred-body");
  const empty = document.getElementById("pred-empty");
  const bar = document.getElementById("accuracy-bar");

  if (!forceRefresh && predictionsRenderedCache && predictionsRenderedCache.version === PREDICTIONS_RENDER_VERSION) {
    status.textContent = "";
    body.innerHTML = predictionsRenderedCache.bodyHtml;
    empty.classList.toggle("visible", !!predictionsRenderedCache.emptyVisible);
    bar.classList.toggle("hidden", !!predictionsRenderedCache.barHidden);
    if (predictionsRenderedCache.barHtml != null) {
      bar.innerHTML = predictionsRenderedCache.barHtml;
    }
    return;
  }

  if (!forceRefresh && Array.isArray(predictionsSnapshotCache)) {
    status.textContent = "";
    if (predictionsSnapshotCache.length === 0) {
      body.innerHTML = "";
      empty.classList.add("visible");
      bar.classList.add("hidden");
      predictionsRenderedCache = {
        version: PREDICTIONS_RENDER_VERSION,
        bodyHtml: "",
        emptyVisible: true,
        barHidden: true,
        barHtml: bar.innerHTML,
      };
      return;
    }
    empty.classList.remove("visible");
    const bodyHtml = renderPredictionsTable(predictionsSnapshotCache);
    const barState = renderAccuracyBar(predictionsSnapshotCache);
    predictionsRenderedCache = {
      version: PREDICTIONS_RENDER_VERSION,
      bodyHtml,
      emptyVisible: false,
      barHidden: !!barState?.hidden,
      barHtml: bar.innerHTML,
    };
    return;
  }

  status.textContent = "Loading predictions…";
  showLoader("Loading predictions…");
  try {
    const res = await authFetch(`${API}/api/predictions`);
    const preds = await res.json();
    predictionsSnapshotCache = Array.isArray(preds) ? preds : [];
    status.textContent = "";

    if (predictionsSnapshotCache.length === 0) {
      body.innerHTML = "";
      empty.classList.add("visible");
      bar.classList.add("hidden");
      predictionsRenderedCache = {
        version: PREDICTIONS_RENDER_VERSION,
        bodyHtml: "",
        emptyVisible: true,
        barHidden: true,
        barHtml: bar.innerHTML,
      };
      return;
    }

    empty.classList.remove("visible");
    const bodyHtml = renderPredictionsTable(predictionsSnapshotCache);
    const barState = renderAccuracyBar(predictionsSnapshotCache);
    predictionsRenderedCache = {
      version: PREDICTIONS_RENDER_VERSION,
      bodyHtml,
      emptyVisible: false,
      barHidden: !!barState?.hidden,
      barHtml: bar.innerHTML,
    };
  } catch (err) {
    status.textContent = "Error: " + describeRequestError(err, "Please refresh or try again.");
  } finally {
    hideLoader();
  }
}

function renderPredictionsTableLegacy(preds) {
  const body = document.getElementById("pred-body");
  predictionReasoningMap = {};
  body.innerHTML = preds.map(p => {
    const basePrice = Number(p.price_at_prediction);
    const hasBasePrice = Number.isFinite(basePrice) && basePrice > 0;
    const currentPrice = Number(p.current_price);
    const hasCurrentPrice = Number.isFinite(currentPrice) && currentPrice > 0;

    const fmtProjectedPct = val => {
      if (val == null) return "—";
      const cls = val >= 0 ? "change-pos" : "change-neg";
      return `<span class="${cls}">${val >= 0 ? "+" : ""}${Number(val).toFixed(2)}%</span>`;
    };
    const fmtTargetPrice = val => {
      if (val == null || !hasBasePrice) return "";
      const targetPrice = basePrice * (1 + Number(val) / 100);
      if (!Number.isFinite(targetPrice)) return "";
      return `<div class="pred-target-price">Target $${targetPrice.toFixed(2)}</div>`;
    };
    const renderProjectedCell = val => `
      <div class="pred-horizon-cell">
        ${fmtProjectedPct(val)}
        ${fmtTargetPrice(val)}
      </div>
    `;

    const scoreValue = p.score != null ? p.score : (p.predicted_pct != null ? Math.max(0, Math.min(100, Math.round(50 + p.predicted_pct * 14))) : null);
    const directionValue = p.direction || (p.predicted_pct == null ? "pending" : (p.predicted_pct >= 0.35 ? "bullish" : p.predicted_pct <= -0.35 ? "bearish" : "neutral"));
    const rowKey = `${p.date || "unknown"}__${p.ticker || "unknown"}`;
    predictionReasoningMap[rowKey] = {
      ticker: p.ticker || "—",
      name: p.name || p.ticker || "—",
      date: p.date || "—",
      price_at_prediction: hasBasePrice ? basePrice : null,
      current_price: hasCurrentPrice ? currentPrice : null,
      confidence: p.confidence || "pending",
      direction: directionValue,
      score: scoreValue,
      predicted_pct: p.predicted_pct,
      predicted_3m_pct: p.predicted_3m_pct,
      predicted_6m_pct: p.predicted_6m_pct,
      predicted_12m_pct: p.predicted_12m_pct,
      predicted_24m_pct: p.predicted_24m_pct,
      predicted_36m_pct: p.predicted_36m_pct,
      actual_pct: p.actual_pct,
      reasoning: p.reasoning || "No reasoning available.",
    };
    const scoreCls = scoreValue >= 61 ? "change-pos" : scoreValue <= 39 ? "change-neg" : "";
    const scoreStr = scoreValue != null ? `<span class="${scoreCls}">${scoreValue}/100</span>` : "—";

    let actualStr = '<span class="result-pending">Pending</span>';
    let varianceStr = '<span class="result-pending">—</span>';
    let resultStr = '<span class="result-pending">—</span>';
    if (p.actual_pct != null) {
      const actCls = p.actual_pct >= 0 ? "change-pos" : "change-neg";
      actualStr = `<span class="${actCls}">${p.actual_pct >= 0 ? "+" : ""}${p.actual_pct.toFixed(2)}%</span>`;
      const actualDirection = p.actual_pct >= 0.35 ? "bullish" : p.actual_pct <= -0.35 ? "bearish" : "neutral";
      varianceStr = `<span>${actualDirection.toUpperCase()}</span>`;
      const correct = directionValue === actualDirection || (directionValue === "neutral" && Math.abs(p.actual_pct) < 0.35);
      resultStr = correct
        ? '<span class="result-correct">✓ Correct</span>'
        : '<span class="result-wrong">✗ Wrong</span>';
    }

    const isPending = p.confidence === "pending" || scoreValue == null;
    const direction = isPending ? "" : (directionValue === "bullish" ? "▲ BULLISH" : directionValue === "bearish" ? "▼ BEARISH" : "• NEUTRAL");
    const dirClass  = isPending ? "" : (directionValue === "bullish" ? "dir-bull" : directionValue === "bearish" ? "dir-bear" : "");
    const confBadge = isPending
      ? `<span class="badge-pending">NOT ANALYSED</span>`
      : `<span class="badge-${p.confidence || 'medium'}">${(p.confidence || 'medium').toUpperCase()}</span> <span class="${dirClass}">${direction}</span>`;

    // Factor badges
    const fs = p.factor_scores || {};
    const factorBadge = (label, key, title) => {
      const val = fs[key];
      if (val == null) return `<span class="factor-badge factor-na" title="${title}">—</span>`;
      const cls = val >= 70 ? "factor-green" : val >= 45 ? "factor-amber" : "factor-red";
      return `<span class="factor-badge ${cls}" title="${title}: ${val}/100">${label}</span>`;
    };
    const dcfMoS = p.dcf && p.dcf.margin_of_safety_pct != null
      ? (() => { const v = p.dcf.margin_of_safety_pct; const cls = v >= 0 ? "change-pos" : "change-neg"; return `<span class="${cls}" title="DCF Margin of Safety">${v >= 0 ? "▲" : "▼"}${Math.abs(v).toFixed(0)}%</span>`; })()
      : "";
    const currentPriceStr = hasCurrentPrice
      ? `<span>${fmtUsd(currentPrice)}</span>`
      : '<span class="result-pending">—</span>';

    return `
      <tr>
        <td>${p.date}</td>
        <td><strong>${p.ticker}</strong></td>
        <td style="color:var(--text-muted);font-size:0.85rem">${safe(p.name || "—")}</td>
        <td>${currentPriceStr}</td>
        <td>${scoreStr}</td>
        <td class="factor-cell">${factorBadge("V","value","Value")}${factorBadge("M","momentum","Momentum")}${factorBadge("Q","quality","Quality")}${factorBadge("G","growth","Growth")}${factorBadge("⊕","composite","Composite")}${dcfMoS ? `<br><span style="font-size:0.75rem">${dcfMoS} MoS</span>` : ""}</td>
        <td>${renderProjectedCell(p.predicted_3m_pct)}</td>
        <td>${renderProjectedCell(p.predicted_6m_pct)}</td>
        <td>${renderProjectedCell(p.predicted_12m_pct)}</td>
        <td>${renderProjectedCell(p.predicted_24m_pct)}</td>
        <td>${renderProjectedCell(p.predicted_36m_pct)}</td>
        <td>${actualStr}</td>
        <td>${varianceStr}</td>
        <td>${resultStr}</td>
        <td>${confBadge}</td>
        <td class="reasoning-cell">${safe(p.reasoning || "—")}</td>
      </tr>
    `;
  }).join("");

  body.querySelectorAll("tr").forEach((row, index) => {
    const p = preds[index];
    if (!p) return;
    const rowKey = `${p.date || "unknown"}__${p.ticker || "unknown"}`;
    const cell = row.lastElementChild;
    if (!cell) return;
    cell.className = "pred-reasoning-col";
    cell.innerHTML = `<button class="btn-reasoning" onclick="openPredictionReasoning('${rowKey}')">View</button>`;
  });
  return body.innerHTML;
}

function openPredictionReasoning(rowKey) {
  const item = predictionReasoningMap[rowKey];
  if (!item) return;

  const overlay = document.getElementById("pred-reasoning-overlay");
  document.getElementById("pred-reasoning-title").textContent = `${item.ticker} thesis`;
  document.getElementById("pred-reasoning-company").textContent = item.name || item.ticker;
  document.getElementById("pred-reasoning-meta").innerHTML = `
    <span>${safe(item.date)}</span>
    <span>${safe((item.confidence || "pending").toUpperCase())}</span>
    <span>${item.score != null ? `${item.score}/100 ${safe((item.direction || "pending").toUpperCase())}` : "Pending"}</span>
    <span>${item.current_price != null ? `Current ${fmtUsd(item.current_price)}` : "Current price unavailable"}</span>
    <span>${item.price_at_prediction != null ? `Base $${Number(item.price_at_prediction).toFixed(2)}` : "Base price unavailable"}</span>
    <span>${item.predicted_12m_pct != null ? `${item.predicted_12m_pct >= 0 ? "+" : ""}${Number(item.predicted_12m_pct).toFixed(2)}% 12M` : "12M pending"}</span>
    <span>${item.actual_pct != null ? `${item.actual_pct >= 0 ? "+" : ""}${item.actual_pct.toFixed(2)}% actual` : "Actual pending"}</span>
  `;
  document.getElementById("pred-reasoning-body").textContent = item.reasoning || "No reasoning available.";
  overlay.classList.remove("hidden");
}

document.getElementById("pred-reasoning-close").addEventListener("click", () => {
  document.getElementById("pred-reasoning-overlay").classList.add("hidden");
});

document.getElementById("pred-reasoning-overlay").addEventListener("click", e => {
  if (e.target === document.getElementById("pred-reasoning-overlay")) {
    document.getElementById("pred-reasoning-overlay").classList.add("hidden");
  }
});

function renderPredictionsTable(preds) {
  const body = document.getElementById("pred-body");
  predictionReasoningMap = {};

  const today = new Date();
  const todayIso = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;
  const startOfWeek = new Date(today);
  startOfWeek.setHours(0, 0, 0, 0);
  const dayOfWeek = startOfWeek.getDay();
  const mondayOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek;
  startOfWeek.setDate(startOfWeek.getDate() + mondayOffset);
  const weekIso = `${startOfWeek.getFullYear()}-${String(startOfWeek.getMonth() + 1).padStart(2, "0")}-${String(startOfWeek.getDate()).padStart(2, "0")}`;
  const monthIso = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-01`;

  const groupedPreds = { today: [], week: [], month: [], ytd: [] };
  preds.forEach(p => {
    const predictionDate = String(p.date || "");
    if (predictionDate === todayIso) groupedPreds.today.push(p);
    else if (predictionDate >= weekIso) groupedPreds.week.push(p);
    else if (predictionDate >= monthIso) groupedPreds.month.push(p);
    else groupedPreds.ytd.push(p);
  });

  const renderPredictionRow = p => {
    const basePrice = Number(p.price_at_prediction);
    const hasBasePrice = Number.isFinite(basePrice) && basePrice > 0;
    const currentPrice = Number(p.current_price);
    const hasCurrentPrice = Number.isFinite(currentPrice) && currentPrice > 0;

    const fmtProjectedPct = val => {
      if (val == null) return "—";
      const cls = val >= 0 ? "change-pos" : "change-neg";
      return `<span class="${cls}">${val >= 0 ? "+" : ""}${Number(val).toFixed(2)}%</span>`;
    };
    const fmtTargetPrice = val => {
      if (val == null || !hasBasePrice) return "";
      const targetPrice = basePrice * (1 + Number(val) / 100);
      if (!Number.isFinite(targetPrice)) return "";
      return `<div class="pred-target-price">Target $${targetPrice.toFixed(2)}</div>`;
    };
    const renderProjectedCell = val => `
      <div class="pred-horizon-cell">
        ${fmtProjectedPct(val)}
        ${fmtTargetPrice(val)}
      </div>
    `;

    const scoreValue = p.score != null ? p.score : (p.predicted_pct != null ? Math.max(0, Math.min(100, Math.round(50 + p.predicted_pct * 14))) : null);
    const directionValue = p.direction || (p.predicted_pct == null ? "pending" : (p.predicted_pct >= 0.35 ? "bullish" : p.predicted_pct <= -0.35 ? "bearish" : "neutral"));
    const rowKey = `${p.date || "unknown"}__${p.ticker || "unknown"}`;
    predictionReasoningMap[rowKey] = {
      ticker: p.ticker || "—",
      name: p.name || p.ticker || "—",
      date: p.date || "—",
      price_at_prediction: hasBasePrice ? basePrice : null,
      current_price: hasCurrentPrice ? currentPrice : null,
      confidence: p.confidence || "pending",
      direction: directionValue,
      score: scoreValue,
      predicted_pct: p.predicted_pct,
      predicted_3m_pct: p.predicted_3m_pct,
      predicted_6m_pct: p.predicted_6m_pct,
      predicted_12m_pct: p.predicted_12m_pct,
      predicted_24m_pct: p.predicted_24m_pct,
      predicted_36m_pct: p.predicted_36m_pct,
      actual_pct: p.actual_pct,
      reasoning: p.reasoning || "No reasoning available.",
    };

    const scoreCls = scoreValue >= 61 ? "change-pos" : scoreValue <= 39 ? "change-neg" : "";
    const scoreStr = scoreValue != null ? `<span class="${scoreCls}">${scoreValue}/100</span>` : "—";

    let actualStr = '<span class="result-pending">Pending</span>';
    let varianceStr = '<span class="result-pending">—</span>';
    let resultStr = '<span class="result-pending">—</span>';
    if (p.actual_pct != null) {
      const actCls = p.actual_pct >= 0 ? "change-pos" : "change-neg";
      actualStr = `<span class="${actCls}">${p.actual_pct >= 0 ? "+" : ""}${p.actual_pct.toFixed(2)}%</span>`;
      const actualDirection = p.actual_pct >= 0.35 ? "bullish" : p.actual_pct <= -0.35 ? "bearish" : "neutral";
      varianceStr = `<span>${actualDirection.toUpperCase()}</span>`;
      const correct = directionValue === actualDirection || (directionValue === "neutral" && Math.abs(p.actual_pct) < 0.35);
      resultStr = correct
        ? '<span class="result-correct">✓ Correct</span>'
        : '<span class="result-wrong">✕ Wrong</span>';
    }

    const isPending = p.confidence === "pending" || scoreValue == null;
    const direction = isPending ? "" : (directionValue === "bullish" ? "▲ BULLISH" : directionValue === "bearish" ? "▼ BEARISH" : "• NEUTRAL");
    const dirClass = isPending ? "" : (directionValue === "bullish" ? "dir-bull" : directionValue === "bearish" ? "dir-bear" : "");
    const confBadge = isPending
      ? `<span class="badge-pending">NOT ANALYSED</span>`
      : `<span class="badge-${p.confidence || "medium"}">${(p.confidence || "medium").toUpperCase()}</span> <span class="${dirClass}">${direction}</span>`;

    const fs = p.factor_scores || {};
    const factorBadge = (label, key, title) => {
      const val = fs[key];
      if (val == null) return `<span class="factor-badge factor-na" title="${title}">—</span>`;
      const cls = val >= 70 ? "factor-green" : val >= 45 ? "factor-amber" : "factor-red";
      return `<span class="factor-badge ${cls}" title="${title}: ${val}/100">${label}</span>`;
    };
    const dcfMoS = p.dcf && p.dcf.margin_of_safety_pct != null
      ? (() => {
          const v = p.dcf.margin_of_safety_pct;
          const cls = v >= 0 ? "change-pos" : "change-neg";
          return `<span class="${cls}" title="DCF Margin of Safety">${v >= 0 ? "▲" : "▼"}${Math.abs(v).toFixed(0)}%</span>`;
        })()
      : "";
    const currentPriceStr = hasCurrentPrice ? `<span>${fmtUsd(currentPrice)}</span>` : '<span class="result-pending">—</span>';

    return `
      <tr>
        <td>${p.date}</td>
        <td><strong>${p.ticker}</strong></td>
        <td style="color:var(--text-muted);font-size:0.85rem">${safe(p.name || "—")}</td>
        <td>${currentPriceStr}</td>
        <td>${scoreStr}</td>
        <td class="factor-cell">${factorBadge("V","value","Value")}${factorBadge("M","momentum","Momentum")}${factorBadge("Q","quality","Quality")}${factorBadge("G","growth","Growth")}${factorBadge("⊕","composite","Composite")}${dcfMoS ? `<br><span style="font-size:0.75rem">${dcfMoS} MoS</span>` : ""}</td>
        <td>${renderProjectedCell(p.predicted_3m_pct)}</td>
        <td>${renderProjectedCell(p.predicted_6m_pct)}</td>
        <td>${renderProjectedCell(p.predicted_12m_pct)}</td>
        <td>${renderProjectedCell(p.predicted_24m_pct)}</td>
        <td>${renderProjectedCell(p.predicted_36m_pct)}</td>
        <td>${actualStr}</td>
        <td>${varianceStr}</td>
        <td>${resultStr}</td>
        <td>${confBadge}</td>
        <td class="reasoning-cell">${safe(p.reasoning || "—")}</td>
      </tr>
    `;
  };

  const sections = [
    ["today", "Today"],
    ["week", "This Week"],
    ["month", "This Month"],
    ["ytd", "YTD"],
  ];

  body.innerHTML = sections
    .filter(([key]) => groupedPreds[key].length > 0)
    .map(([key, label]) => `
      <tr class="pred-group-row">
        <td colspan="16">
          <div class="pred-group-heading">
            <span>${label}</span>
            <span class="pred-group-count">${groupedPreds[key].length}</span>
          </div>
        </td>
      </tr>
      ${groupedPreds[key].map(renderPredictionRow).join("")}
    `)
    .join("");

  Array.from(body.querySelectorAll("tr"))
    .filter(row => !row.classList.contains("pred-group-row"))
    .forEach((row, index) => {
      const p = preds[index];
      if (!p) return;
      const rowKey = `${p.date || "unknown"}__${p.ticker || "unknown"}`;
      const cell = row.lastElementChild;
      if (!cell) return;
      cell.className = "pred-reasoning-col";
      cell.innerHTML = `<button class="btn-reasoning" onclick="openPredictionReasoning('${rowKey}')">View</button>`;
    });

  return body.innerHTML;
}

function applyPredictionPeriodFilter(period) {
  const body = document.getElementById("pred-body");
  if (!body) return;
  const groups = body.querySelectorAll("tr.pred-group-row");
  groups.forEach(groupRow => {
    const heading = groupRow.querySelector(".pred-group-heading span")?.textContent?.trim() || "";
    const periodMap = { today: "Today", week: "This Week", month: "This Month", ytd: "YTD" };
    const match = period === "all" || periodMap[period] === heading;
    const siblings = [];
    let next = groupRow.nextElementSibling;
    while (next && !next.classList.contains("pred-group-row")) {
      siblings.push(next);
      next = next.nextElementSibling;
    }
    groupRow.style.display = match ? "" : "none";
    siblings.forEach(r => { r.style.display = match ? "" : "none"; });
  });
}

document.querySelectorAll(".pred-period-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".pred-period-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    applyPredictionPeriodFilter(btn.dataset.period);
  });
});

function openRecReasoning(key) {
  const item = recReasoningMap[key];
  if (!item) return;
  const overlay = document.getElementById("pred-reasoning-overlay");
  document.getElementById("pred-reasoning-title").textContent = `${item.ticker} reasoning`;
  document.getElementById("pred-reasoning-company").textContent = item.name || item.ticker;
  document.getElementById("pred-reasoning-meta").innerHTML = `<span>${safe(item.type)}</span>`;
  document.getElementById("pred-reasoning-body").textContent = item.reasoning || "No reasoning available.";
  overlay.classList.remove("hidden");
}

function renderAccuracyBar(preds) {
  const bar = document.getElementById("accuracy-bar");
  const completed = preds.filter(p => p.actual_pct != null);

  if (completed.length === 0) {
    bar.classList.add("hidden");
    return { hidden: true, html: bar.innerHTML };
  }

  bar.classList.remove("hidden");

  const classifyDirection = item => item.direction || (item.predicted_pct == null ? "pending" : (item.predicted_pct >= 0.35 ? "bullish" : item.predicted_pct <= -0.35 ? "bearish" : "neutral"));
  const correct = completed.filter(p => {
    const predictedDirection = classifyDirection(p);
    const actualDirection = p.actual_pct >= 0.35 ? "bullish" : p.actual_pct <= -0.35 ? "bearish" : "neutral";
    return predictedDirection === actualDirection;
  }).length;
  const accPct = (correct / completed.length * 100).toFixed(0);
  const avgPred = (completed.reduce((s, p) => s + (p.score ?? (p.predicted_pct != null ? (50 + p.predicted_pct * 14) : 50)), 0) / completed.length).toFixed(0);
  const avgActual = (completed.reduce((s, p) => s + p.actual_pct, 0) / completed.length).toFixed(2);

  document.getElementById("acc-pct").textContent = accPct + "%";
  document.getElementById("acc-total").textContent = completed.length + " resolved";
  document.getElementById("acc-avg-pred").textContent = avgPred + "/100";
  document.getElementById("acc-avg-actual").textContent = (avgActual >= 0 ? "+" : "") + avgActual + "%";

  const accEl = document.getElementById("acc-pct");
  accEl.style.color = accPct >= 60 ? "var(--green)" : accPct >= 40 ? "var(--accent)" : "var(--red)";
  return { hidden: false, html: bar.innerHTML };
}

document.getElementById("btn-generate").addEventListener("click", async () => {
  const btn = document.getElementById("btn-generate");
  const status = document.getElementById("pred-status");

  btn.disabled = true;
  status.textContent = "Generating predictions from market data and fundamentals…";
  showLoader("Consulting the oracle…");

  try {
    const res = await authFetch(`${API}/api/predictions/generate`, { method: "POST" });
    const rawText = await res.text();
    let data = {};
    try {
      data = rawText ? JSON.parse(rawText) : {};
    } catch {
      throw new Error(rawText || `Predictions request failed (${res.status})`);
    }

    if (!res.ok) {
      status.textContent = "Error: " + (data.detail || data.error || "Predictions request failed.");
      return;
    }

    if (data.detail) {
      status.textContent = "Error: " + data.detail;
      return;
    }

    if (data.message) {
      status.textContent = data.message;
    } else {
      status.textContent = `Generated ${data.predictions.length} prediction(s) for today.`;
    }

    invalidatePredictionsSnapshotCache();
    loadPredictions(true);
  } catch (err) {
    status.textContent = "Error: " + describeRequestError(err, "Please refresh or try again.");
  } finally {
    btn.disabled = false;
    hideLoader();
  }
});

document.getElementById("btn-refresh-preds").addEventListener("click", () => {
  invalidatePredictionsSnapshotCache();
  loadPredictions(true);
});

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
    const rawText = await res.text();
    let data = {};
    try {
      data = rawText ? JSON.parse(rawText) : {};
    } catch {
      throw new Error(rawText || `Backtest request failed (${res.status})`);
    }
    if (!res.ok) {
      status.textContent = data.detail || data.error || "Backtest request failed.";
      return;
    }
    status.textContent = "";

    const s = data.summary;
    if (!s || typeof s.accuracy_pct !== "number") {
      status.textContent = data.error || "Backtest could not produce results with the currently available market data.";
      return;
    }
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

function setSimChartSize(size = "default") {
  const wrap = document.getElementById("sim-chart-wrap");
  if (!wrap) return;
  wrap.classList.remove("sim-chart-wrap-compact", "sim-chart-wrap-default", "sim-chart-wrap-large");
  wrap.classList.add(`sim-chart-wrap-${size}`);

  [["sim-size-compact", "compact"], ["sim-size-default", "default"], ["sim-size-large", "large"]].forEach(([id, key]) => {
    const btn = document.getElementById(id);
    if (!btn) return;
    btn.classList.toggle("active", key === size);
  });

  if (simChart) simChart.resize();
}

document.getElementById("sim-size-compact")?.addEventListener("click", () => setSimChartSize("compact"));
document.getElementById("sim-size-default")?.addEventListener("click", () => setSimChartSize("default"));
document.getElementById("sim-size-large")?.addEventListener("click", () => setSimChartSize("large"));

document.getElementById("btn-simulate")?.addEventListener("click", async () => {
  const btn    = document.getElementById("btn-simulate");
  const status = document.getElementById("sim-status");
  const results = document.getElementById("sim-results");

  btn.disabled = true;
  status.textContent = "Running simulator… fetching 4 weeks of price data and running 1,000 Monte Carlo simulations (30–60 seconds)…";
  results.classList.add("hidden");

  try {
    const res  = await authFetch(`${API}/api/predictions/simulate`);
    const rawText = await res.text();
    let data = {};
    try {
      data = rawText ? JSON.parse(rawText) : {};
    } catch {
      throw new Error(rawText || `Simulator request failed (${res.status})`);
    }

    if (!res.ok) {
      status.textContent = data.detail || data.error || "Simulator request failed.";
      return;
    }

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

    if (simChart) { simChart.destroy(); simChart = null; }
    setSimChartSize("default");
    const simCanvas = document.getElementById("sim-chart");
    if (!simCanvas) { status.textContent = "Chart canvas not found — please open the Backtest tab and try again."; return; }
    const ctx = simCanvas.getContext("2d");
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
        maintainAspectRatio: false,
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
  const fmtEta = ms => {
    if (ms <= 0) return "0s";
    if (ms < 1000) return "<1s";
    return `${Math.round(ms / 1000)}s`;
  };
  status.innerHTML = `
    <span class="status-loading status-loading-rec">
      <span class="status-signal" aria-hidden="true">
        <span class="status-signal-bar"></span>
        <span class="status-signal-bar"></span>
        <span class="status-signal-bar"></span>
      </span>
      <span class="status-loading-copy">
        <span id="rec-status-text">Starting recommendations…</span>
        <span class="status-progress-row">
          <span class="status-progress-track"><span id="rec-progress-fill" class="status-progress-fill"></span></span>
          <span id="rec-progress-pct" class="status-progress-pct">0%</span>
        </span>
        <span id="rec-status-subtext" class="status-subtext">Preparing estimate…</span>
      </span>
    </span>
  `;
  const recStatusText = document.getElementById("rec-status-text");
  const recStatusSubtext = document.getElementById("rec-status-subtext");
  const recProgressFill = document.getElementById("rec-progress-fill");
  const recProgressPct = document.getElementById("rec-progress-pct");
  let pollTimer = null;
  try {
    const startRes = await authFetch(`${API}/api/recommendations/start`, { method: "POST" });
    const startData = await startRes.json();
    if (!startRes.ok) throw new Error(startData.detail || "Could not start recommendations.");
    const jobId = startData.job_id;
    if (!jobId) throw new Error("No recommendation job id returned.");

    const data = await new Promise((resolve, reject) => {
      pollTimer = setInterval(async () => {
        try {
          const progressRes = await authFetch(`${API}/api/recommendations/progress/${jobId}`);
          const progress = await progressRes.json();
          if (!progressRes.ok) {
            reject(new Error(progress.detail || "Could not load recommendation progress."));
            return;
          }
          if (recStatusText) recStatusText.textContent = progress.message || "Loading recommendations…";
          if (recProgressFill) recProgressFill.style.width = `${Math.max(0, Math.min(100, progress.percent || 0))}%`;
          if (recProgressPct) recProgressPct.textContent = `${Math.max(0, Math.min(100, progress.percent || 0))}%`;
          if (recStatusSubtext) {
            const elapsedMs = progress.elapsed_ms || 0;
            const remainingMs = progress.remaining_ms || 0;
            if (progress.status === "running" && remainingMs <= 0) {
              const progressCounts = progress.total ? ` • ${progress.completed || 0}/${progress.total}` : "";
              recStatusSubtext.textContent = `Elapsed ${fmtEta(elapsedMs)}${progressCounts} • Finalizing… this is taking longer than usual`;
            } else {
              const progressCounts = progress.total ? ` • ${progress.completed || 0}/${progress.total}` : "";
              recStatusSubtext.textContent = `Elapsed ${fmtEta(elapsedMs)}${progressCounts} • About ${fmtEta(remainingMs)} remaining`;
            }
          }
          if (progress.status === "completed") {
            clearInterval(pollTimer);
            resolve(progress.result || {});
            return;
          }
          if (progress.status === "error") {
            clearInterval(pollTimer);
            reject(new Error(progress.error || "Recommendations failed."));
          }
        } catch (err) {
          clearInterval(pollTimer);
          reject(err);
        }
      }, 900);
    });
    status.textContent = "";
    renderRecommendations(data);
  } catch (err) {
    status.textContent = "Error: " + describeRequestError(err, "Please refresh or try again.");
  } finally {
    if (pollTimer) clearInterval(pollTimer);
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
      const key = `sell__${s.ticker}`;
      recReasoningMap[key] = { ticker: s.ticker, name: s.name, type: "Sell", reasoning: s.reasoning };
      const pnlCls   = s.unrealised_pnl >= 0 ? "change-pos" : "change-neg";
      const predStr  = s.score_value != null ? `<span class="${s.score_value >= 61 ? "change-pos" : s.score_value <= 39 ? "change-neg" : ""}">${s.score_value}/100 ${safe((s.direction || "neutral").toUpperCase())}</span>` : "—";
      const trigCls  = s.trigger === "STOP LOSS" ? "badge-low" : s.trigger === "TAKE PROFIT" ? "badge-high" : "badge-medium";
      const fs = s.factor_scores || {};
      const riskParts = [];
      if (s.annualised_vol_pct != null) riskParts.push(`Vol: ${s.annualised_vol_pct.toFixed(0)}%`);
      if (s.max_drawdown_pct != null) riskParts.push(`DD: ${s.max_drawdown_pct.toFixed(0)}%`);
      if (fs.quality != null) riskParts.push(`Quality: ${fs.quality}/100`);
      const riskCtx = riskParts.length ? `<div class="rec-risk-ctx">${riskParts.join(" · ")}</div>` : "";
      return `<tr>
        <td><strong>${safe(s.ticker)}</strong>${riskCtx}</td>
        <td style="color:var(--text-muted);font-size:0.85rem">${safe(s.name)}</td>
        <td><span class="${trigCls}">${safe(s.trigger)}</span></td>
        <td>${s.qty}</td>
        <td>${fmt(s.current_price)}</td>
        <td><strong>${fmt(s.estimated_proceeds)}</strong></td>
        <td><span class="${pnlCls}">${s.unrealised_pnl >= 0 ? "+" : ""}${fmt(s.unrealised_pnl)} (${s.unrealised_pct >= 0 ? "+" : ""}${s.unrealised_pct.toFixed(1)}%)</span></td>
        <td>${predStr}</td>
        <td><button class="btn-reasoning" onclick="openRecReasoning('${key}')">View</button></td>
        <td><button class="btn-paper-sell" onclick="paperTrade(this,'sell','${safe(s.ticker)}',${s.qty},${s.current_price})">− Paper Sell</button></td>
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
      const key = `buy__${b.ticker}`;
      recReasoningMap[key] = { ticker: b.ticker, name: b.name, type: "Buy", reasoning: b.reasoning };
      const accStr = b.accuracy_pct != null ? `${b.accuracy_pct}%` : "<span style='color:var(--text-muted)'>No data</span>";
      const fs = b.factor_scores || {};
      const factorLine = (label, key2, title) => {
        const v = fs[key2];
        if (v == null) return "";
        const cls = v >= 70 ? "factor-green" : v >= 45 ? "factor-amber" : "factor-red";
        return `<span class="factor-badge ${cls}" title="${title}: ${v}/100">${label}:${v}</span>`;
      };
      const dcfMoS = b.dcf && b.dcf.margin_of_safety_pct != null
        ? (() => { const v = b.dcf.margin_of_safety_pct; return `<span class="${v >= 0 ? "change-pos" : "change-neg"}" title="DCF Margin of Safety">DCF:${v >= 0 ? "▲" : "▼"}${Math.abs(v).toFixed(0)}%</span>`; })()
        : "";
      const volStr = b.annualised_vol_pct != null ? `<span title="Annualised Volatility">Vol:${b.annualised_vol_pct.toFixed(0)}%</span>` : "";
      const factorBar = [factorLine("V","value","Value"), factorLine("M","momentum","Momentum"), factorLine("Q","quality","Quality"), factorLine("G","growth","Growth"), dcfMoS, volStr].filter(Boolean).join(" ");
      return `<tr>
        <td style="color:var(--text-muted)">#${i + 1}</td>
        <td><strong>${safe(b.ticker)}</strong>${factorBar ? `<div class="rec-factor-bar">${factorBar}</div>` : ""}</td>
        <td style="color:var(--text-muted);font-size:0.85rem">${safe(b.name)}</td>
        <td><span class="badge-${b.confidence}">${b.confidence.toUpperCase()}</span></td>
        <td>${accStr}</td>
        <td><span class="${b.score_value >= 61 ? "change-pos" : ""}">${b.score_value}/100 ${safe((b.direction || "bullish").toUpperCase())}</span></td>
        <td>${fmt(b.current_price)}</td>
        <td><strong>${b.qty}</strong></td>
        <td><strong>${fmt(b.estimated_cost)}</strong></td>
        <td><button class="btn-reasoning" onclick="openRecReasoning('${key}')">View</button></td>
        <td><button class="btn-paper-buy" onclick="paperTrade(this,'buy','${safe(b.ticker)}',${b.qty},${b.current_price})">+ Paper Buy</button></td>
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

// ── Paper Portfolio ───────────────────────────────────────────

function fmtGbp(n) {
  if (n == null) return "—";
  return (n < 0 ? "-" : "") + "£" + Math.abs(n).toLocaleString("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

async function loadPaperPortfolio() {
  const status = document.getElementById("paper-status");
  status.textContent = "Loading…";
  try {
    const res  = await authFetch(`${API}/api/paper-portfolio`);
    const data = await res.json();
    status.textContent = "";
    renderPaperPortfolio(data);
  } catch (err) {
    status.textContent = "Error: " + err.message;
  }
}

function renderPaperPortfolio(data) {
  const summary    = data.summary    || {};
  const positions  = data.positions  || [];
  const txs        = data.transactions || [];
  const empty      = document.getElementById("paper-empty");
  const summaryEl  = document.getElementById("paper-summary");

  // Summary bar
  const pnl    = summary.total_pnl ?? 0;
  const pnlPct = summary.total_pnl_pct ?? 0;
  const pnlCls = pnl >= 0 ? "change-pos" : "change-neg";
  const rlsCls = (summary.realised_pnl ?? 0) >= 0 ? "change-pos" : "change-neg";
  const holdingsValue = summary.total_current_value ?? ((summary.total_value ?? 0) - (summary.cash ?? 0));
  const unrealised = summary.total_unrealised_pnl ?? (holdingsValue - (summary.total_invested ?? 0));

  document.getElementById("paper-cash").textContent     = fmtGbp(summary.cash);
  document.getElementById("paper-invested").textContent = fmtGbp(holdingsValue);
  document.getElementById("paper-total").textContent    = fmtGbp(summary.total_value);

  document.getElementById("paper-unrealised").innerHTML =
    `<span class="${unrealised >= 0 ? "change-pos" : "change-neg"}">${fmtGbp(unrealised)}</span>`;
  document.getElementById("paper-realised").innerHTML =
    `<span class="${rlsCls}">${fmtGbp(summary.realised_pnl)}</span>`;
  document.getElementById("paper-pnl").innerHTML =
    `<span class="${pnlCls}">${fmtGbp(pnl)} (${pnl >= 0 ? "+" : ""}${pnlPct}%)</span>`;

  summaryEl.classList.remove("hidden");

  // Positions table
  const posWrap    = document.getElementById("paper-positions-wrap");
  const posHeading = document.getElementById("paper-positions-heading");
  const posBody    = document.getElementById("paper-positions-body");
  if (positions.length > 0) {
    posHeading.style.display = "";
    posWrap.style.display    = "";
    posBody.innerHTML = positions.map(p => {
      const uCls = p.unrealised_pnl >= 0 ? "change-pos" : "change-neg";
      const rCls = p.realised_pnl  >= 0 ? "change-pos" : "change-neg";
      return `<tr>
        <td><strong>${safe(p.ticker)}</strong></td>
        <td style="color:var(--text-muted);font-size:0.85rem">${safe(p.name)}</td>
        <td>${p.shares}</td>
        <td>${fmtGbp(p.avg_cost)}</td>
        <td>${fmtGbp(p.current_price)}</td>
        <td>${fmtGbp(p.cost_basis)}</td>
        <td>${fmtGbp(p.current_value)}</td>
        <td><span class="${uCls}">${fmtGbp(p.unrealised_pnl)}</span></td>
        <td><span class="${uCls}">${p.unrealised_pct >= 0 ? "+" : ""}${p.unrealised_pct}%</span></td>
        <td><span class="${rCls}">${fmtGbp(p.realised_pnl)}</span></td>
      </tr>`;
    }).join("");
  } else {
    posHeading.style.display = "none";
    posWrap.style.display    = "none";
  }

  // Trade history
  const histWrap    = document.getElementById("paper-history-wrap");
  const histHeading = document.getElementById("paper-history-heading");
  const histBody    = document.getElementById("paper-history-body");
  if (txs.length > 0) {
    histHeading.style.display = "";
    histWrap.style.display    = "";
    histBody.innerHTML = txs.map(tx => {
      const typeCls = tx.type === "buy" ? "paper-trade-type-buy" : "paper-trade-type-sell";
      const value   = (tx.qty || 0) * (tx.price || 0);
      const tradePnl = tx.realised_pnl;
      const pnlHtml = tradePnl == null
        ? '<span class="result-pending">Open</span>'
        : `<span class="${tradePnl >= 0 ? "change-pos" : "change-neg"}">${fmtGbp(tradePnl)}</span>`;
      return `<tr>
        <td>${safe(tx.date)}</td>
        <td><span class="${typeCls}">${tx.type.toUpperCase()}</span></td>
        <td><strong>${safe(tx.ticker)}</strong></td>
        <td>${tx.qty}</td>
        <td>${fmtGbp(tx.price)}</td>
        <td>${fmtGbp(value)}</td>
        <td>${pnlHtml}</td>
      </tr>`;
    }).join("");
  } else {
    histHeading.style.display = "none";
    histWrap.style.display    = "none";
  }

  empty.style.display = (positions.length === 0 && txs.length === 0) ? "" : "none";
}

async function paperTrade(btn, type, ticker, qty, price) {
  btn.disabled = true;
  const origText = btn.textContent;
  btn.textContent = type === "buy" ? "Buying…" : "Selling…";
  try {
    const res = await authFetch(`${API}/api/paper-portfolio/${type}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ticker, qty, price }),
    });
    let data = {};
    try { data = await res.json(); } catch {}
    if (!res.ok) {
      alert("Paper trade failed: " + (data.detail || `HTTP ${res.status}`));
      btn.textContent = origText;
      btn.disabled = false;
      return;
    }
    btn.textContent = type === "buy" ? "✓ Bought" : "✓ Sold";
    btn.style.opacity = "0.6";
    const activeTab = document.querySelector(".tab-btn.active")?.dataset.tab || "";

    // Refresh the visible screen in place after every paper trade.
    if (activeTab === "recommendations") {
      await loadRecommendations();
    } else {
      await loadPaperPortfolio();
    }
  } catch (err) {
    alert("Error: " + err.message);
    btn.textContent = origText;
    btn.disabled = false;
  }
}

document.getElementById("btn-refresh-paper").addEventListener("click", loadPaperPortfolio);

document.getElementById("btn-reset-paper").addEventListener("click", async () => {
  if (!confirm("Reset your entire paper portfolio back to £100,000? This cannot be undone.")) return;
  try {
    await authFetch(`${API}/api/paper-portfolio/reset`, { method: "DELETE" });
    loadPaperPortfolio();
  } catch (err) {
    document.getElementById("paper-status").textContent = "Reset failed: " + err.message;
  }
});

// ── Alerts ────────────────────────────────────────────────────

async function loadAlerts() {
  const status = document.getElementById("alerts-status");
  status.textContent = "Loading…";
  try {
    const [alertsRes, statusRes, settingsRes] = await Promise.all([
      authFetch(`${API}/api/alerts`),
      authFetch(`${API}/api/alerts/status`),
      authFetch(`${API}/api/settings`),
    ]);
    const alerts = await alertsRes.json();
    const monStatus = await statusRes.json();
    const settings = await settingsRes.json();
    status.textContent = "";
    renderMonitorBar(monStatus);
    renderAlertsTable(alerts);
    populateAlertSettings(settings);
  } catch (err) {
    status.textContent = "Error loading alerts. Is the backend running?";
  }
}

function populateAlertSettings(s) {
  const set = (id, val) => { const el = document.getElementById(id); if (el && val != null) el.value = val; };
  set("as-price-swing", s.alert_price_swing_pct);
  set("as-cooldown",    s.alert_cooldown_hours);
  set("as-top-buys",    s.alert_top_buys);
  set("as-top-sells",   s.alert_top_sells);
  set("as-buy-score",   s.alert_buy_min_score);
  set("as-sell-score",  s.alert_sell_max_score);
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
  document.getElementById("mon-swing").textContent = s.strategy?.focus || "Strong BUY and SELL signals";
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
    const res = await authFetch(`${API}/api/alerts/test-preview`, { method: "POST" });
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

document.getElementById("btn-save-alert-settings").addEventListener("click", async () => {
  const msg = document.getElementById("alert-settings-msg");
  msg.className = "alert-settings-msg";
  msg.textContent = "Saving…";
  try {
    // Fetch current settings first to preserve non-alert fields (initial_float etc.)
    const existing = await (await authFetch(`${API}/api/settings`)).json();
    const updated = {
      ...existing,
      alert_price_swing_pct: parseFloat(document.getElementById("as-price-swing").value),
      alert_cooldown_hours:  parseFloat(document.getElementById("as-cooldown").value),
      alert_top_buys:        parseInt(document.getElementById("as-top-buys").value, 10),
      alert_top_sells:       parseInt(document.getElementById("as-top-sells").value, 10),
      alert_buy_min_score:   parseInt(document.getElementById("as-buy-score").value, 10),
      alert_sell_max_score:  parseInt(document.getElementById("as-sell-score").value, 10),
    };
    await authFetch(`${API}/api/settings`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(updated),
    });
    msg.textContent = "Saved ✓";
    setTimeout(() => { msg.textContent = ""; }, 3000);
  } catch (err) {
    msg.className = "alert-settings-msg error";
    msg.textContent = "Error saving settings";
  }
});

// ── Portfolio ─────────────────────────────────────────────────

function pnlHtml(val) {
  if (val == null) return "<span>—</span>";
  const sign = val >= 0 ? "+" : "";
  const cls  = val >= 0 ? "change-pos" : "change-neg";
  return `<span class="${cls}">${sign}$${Math.abs(val).toLocaleString("en-US", {minimumFractionDigits: 2, maximumFractionDigits: 2})}</span>`;
}

function fmtUsd(val) {
  if (val == null || Number.isNaN(Number(val))) return "—";
  return "$" + Number(val).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
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

    summary.classList.remove("hidden");
    document.getElementById("port-invested").textContent    = fmtUsd(s.total_invested);
    document.getElementById("port-current").textContent     = fmtUsd(s.total_current_value);
    document.getElementById("port-unrealised").innerHTML    = pnlHtml(s.total_unrealised_pnl);
    document.getElementById("port-realised").innerHTML      = pnlHtml(s.total_realised_pnl);
    const totalEl = document.getElementById("port-total");
    totalEl.innerHTML = pnlHtml(s.total_pnl);

    if (!positions || positions.length === 0) {
      body.innerHTML = "";
      empty.classList.add("visible");
      return;
    }

    empty.classList.remove("visible");

    body.innerHTML = positions.map(p => `
      <tr data-ticker="${p.ticker}">
        <td><strong>${p.ticker}</strong></td>
        <td style="color:var(--text-muted);font-size:0.85rem">${p.name}</td>
        <td>${p.shares}</td>
        <td>$${p.avg_cost.toFixed(2)}</td>
        <td>$${p.current_price.toFixed(2)}</td>
        <td>${fmtUsd(p.cost_basis)}</td>
        <td>${fmtUsd(p.current_value)}</td>
        <td>${pnlHtml(p.unrealised_pnl)} <span style="color:var(--text-muted);font-size:0.78rem">(${p.unrealised_pct >= 0 ? "+" : ""}${p.unrealised_pct}%)</span></td>
        <td>${pnlHtml(p.realised_pnl)}</td>
      </tr>
    `).join("");

    body.querySelectorAll("tr").forEach(row => {
      row.addEventListener("click", () => openDetail(row.dataset.ticker));
    });

  } catch (err) {
    status.textContent = err?.message || "Error loading portfolio.";
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
