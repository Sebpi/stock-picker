(function () {
  const { useEffect, useMemo, useRef, useState } = React;
  const h = React.createElement;
  const API = "";
  const PICK_SHOVELS_API = "https://pick-shovels-wistful-morning-252.fly.dev";
  const TOKEN_KEY = "stocklens_token";

  const TABS = [
    ["screener", "Screener"],
    ["watchlist", "Watchlist"],
    ["sentiment", "Sentiment"],
    ["ai", "AI Advisor"],
    ["predictions", "Predictions"],
    ["thesis", "Thesis"],
    ["backtest", "Backtest"],
    ["recommendations", "Signals"],
    ["alerts", "Alerts"],
    ["portfolio", "Portfolio"],
    ["paper", "Paper P&L"],
  ];

  // ──────────────────────────────────────────────────────────────
  // Utilities
  // ──────────────────────────────────────────────────────────────

  function cx() {
    return Array.from(arguments).filter(Boolean).join(" ");
  }

  function token() {
    return localStorage.getItem(TOKEN_KEY) || "";
  }

  function setToken(value) {
    if (value) localStorage.setItem(TOKEN_KEY, value);
    else localStorage.removeItem(TOKEN_KEY);
  }

  async function api(path, opts) {
    const init = Object.assign({ headers: {} }, opts || {});
    init.headers = Object.assign({}, init.headers || {});
    if (token()) init.headers.Authorization = `Bearer ${token()}`;
    if (init.body && !(init.body instanceof FormData) && !init.headers["Content-Type"]) {
      init.headers["Content-Type"] = "application/json";
    }
    const res = await fetch(`${API}${path}`, init);
    const text = await res.text();
    let data = null;
    try { data = text ? JSON.parse(text) : null; } catch { data = text; }
    if (!res.ok) {
      if (res.status === 401) setToken("");
      throw new Error((data && data.detail) || `HTTP ${res.status}`);
    }
    return data;
  }

  function fmtUsd(value, digits) {
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    return n.toLocaleString("en-US", { style: "currency", currency: "USD", minimumFractionDigits: digits ?? 2, maximumFractionDigits: digits ?? 2 });
  }

  function fmtGbp(value, digits) {
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    return (n < 0 ? "-" : "") + "£" + Math.abs(n).toLocaleString("en-GB", { minimumFractionDigits: digits ?? 2, maximumFractionDigits: digits ?? 2 });
  }

  function fmtPct(value, digits) {
    const n = Number(value);
    if (!Number.isFinite(n)) return "—";
    return `${n >= 0 ? "+" : ""}${n.toFixed(digits == null ? 1 : digits)}%`;
  }

  function fmtDate(value) {
    if (!value) return "—";
    try { return new Date(value).toLocaleString(); } catch { return String(value); }
  }

  function fmtCap(value) {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return "—";
    if (n >= 1e12) return `$${(n / 1e12).toFixed(1)}T`;
    if (n >= 1e9) return `$${(n / 1e9).toFixed(1)}B`;
    if (n >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
    return `$${n.toLocaleString()}`;
  }

  function scoreTone(score) {
    const n = Number(score || 0);
    if (n >= 70) return "text-pulse-green";
    if (n >= 50) return "text-pulse-amber";
    return "text-pulse-red";
  }

  function scoreBg(score) {
    const n = Number(score || 0);
    if (n >= 70) return "bg-pulse-green";
    if (n >= 50) return "bg-pulse-amber";
    return "bg-pulse-red";
  }

  function deltaTone(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return "text-pulse-muted";
    return n >= 0 ? "text-pulse-green" : "text-pulse-red";
  }

  // ──────────────────────────────────────────────────────────────
  // Primitives
  // ──────────────────────────────────────────────────────────────

  function Card(props) {
    return h("section", {
      className: cx("rounded-xl border border-pulse-line bg-pulse-card/88 shadow-glow", props.className)
    }, props.children);
  }

  function Button(props) {
    const kind = props.kind || "secondary";
    const passthrough = Object.assign({}, props);
    delete passthrough.kind;
    delete passthrough.className;
    return h("button", Object.assign({}, passthrough, {
      className: cx(
        "min-h-11 rounded-lg px-4 text-sm font-semibold transition disabled:opacity-50 disabled:cursor-not-allowed",
        kind === "primary"
          ? "bg-gradient-to-r from-pulse-cyan to-pulse-magenta text-black shadow-glow"
          : kind === "danger"
            ? "border border-pulse-red/40 bg-pulse-red/10 text-pulse-red hover:bg-pulse-red/20"
            : kind === "ghost"
              ? "border border-transparent bg-transparent text-pulse-muted hover:text-pulse-cyan"
              : "border border-pulse-line bg-pulse-panel text-pulse-ink hover:border-pulse-cyan/50",
        props.className
      )
    }), props.children);
  }

  function TextInput(props) {
    const passthrough = Object.assign({}, props);
    delete passthrough.className;
    return h("input", Object.assign({}, passthrough, {
      className: cx(
        "h-11 w-full rounded-lg border border-pulse-line bg-pulse-panel px-3 text-base text-pulse-ink placeholder:text-pulse-dim outline-none focus:border-pulse-cyan focus:ring-2 focus:ring-pulse-cyan/20",
        props.className
      )
    }));
  }

  function TextArea(props) {
    const passthrough = Object.assign({}, props);
    delete passthrough.className;
    return h("textarea", Object.assign({}, passthrough, {
      className: cx(
        "w-full rounded-lg border border-pulse-line bg-pulse-panel px-3 py-2 text-sm text-pulse-ink placeholder:text-pulse-dim outline-none focus:border-pulse-cyan focus:ring-2 focus:ring-pulse-cyan/20",
        props.className
      )
    }));
  }

  function Select(props) {
    const passthrough = Object.assign({}, props);
    delete passthrough.className;
    return h("select", Object.assign({}, passthrough, {
      className: cx(
        "h-11 w-full rounded-lg border border-pulse-line bg-pulse-panel px-3 text-base text-pulse-ink outline-none focus:border-pulse-cyan",
        props.className
      )
    }));
  }

  function Pill(props) {
    return h("span", { className: cx("inline-flex items-center rounded-full border px-2.5 py-1 font-mono text-[10px] uppercase tracking-[0.12em]", props.className) }, props.children);
  }

  function Metric(props) {
    return h("div", { className: cx("rounded-lg border border-pulse-line bg-pulse-panel p-3", props.className) },
      h("div", { className: "font-mono text-[10px] uppercase tracking-[0.18em] text-pulse-dim" }, props.label),
      h("div", { className: cx("mt-2 font-mono text-lg tabular-nums", props.tone || "text-pulse-ink") }, props.value == null || props.value === "" ? "—" : props.value),
      props.hint ? h("div", { className: "mt-1 text-xs text-pulse-muted" }, props.hint) : null
    );
  }

  function ProgressBar(props) {
    const value = Math.max(0, Math.min(100, Number(props.value || 0)));
    return h("div", { className: cx("h-1.5 overflow-hidden rounded-full bg-pulse-bg", props.trackClassName) },
      h("div", { className: cx("h-full rounded-full transition-all", props.color || scoreBg(value)), style: { width: `${value}%` } })
    );
  }

  function Empty(props) {
    return h(Card, { className: "p-5 text-sm text-pulse-muted" }, props.children || "Nothing here yet.");
  }

  function Status(props) {
    if (!props.message) return null;
    const tone = props.tone === "error" ? "text-pulse-red" : props.tone === "ok" ? "text-pulse-green" : "text-pulse-muted";
    return h("p", { className: cx("text-sm", tone, props.className) }, props.message);
  }

  function SectionHead(props) {
    return h("div", { className: "mb-4 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between" },
      h("div", { className: "min-w-0" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, props.kicker || "StockLens"),
        h("h2", { className: "mt-1 text-2xl font-semibold tracking-tight" }, props.title),
        props.subtitle ? h("p", { className: "mt-1 max-w-2xl text-sm text-pulse-muted" }, props.subtitle) : null
      ),
      props.actions ? h("div", { className: "grid w-full grid-cols-2 gap-2 sm:flex sm:w-auto sm:flex-wrap sm:justify-end" }, props.actions) : null
    );
  }

  function ResponsiveTable({ columns, rows, mobileRender, onRowClick, emptyText, minWidth }) {
    return h(React.Fragment, null,
      h("div", { className: "grid gap-3 md:hidden" }, rows.length
        ? rows.map((row, i) => h(Card, { key: i, className: "p-4 cursor-pointer", onClick: onRowClick ? () => onRowClick(row) : undefined }, mobileRender(row, i)))
        : h(Empty, null, emptyText || "No rows.")),
      h("div", { className: "hidden overflow-x-auto rounded-xl border border-pulse-line bg-pulse-card md:block" },
        h("table", { className: "min-w-full border-collapse text-sm", style: minWidth ? { minWidth: minWidth + "px" } : null },
          h("thead", { className: "bg-pulse-panel text-left font-mono text-[10px] uppercase tracking-[0.16em] text-pulse-dim" },
            h("tr", null, columns.map(col => h("th", { key: col.key, className: cx("whitespace-nowrap border-b border-pulse-line px-2 py-3", col.headClass) }, col.label)))
          ),
          h("tbody", null, rows.length ? rows.map((row, i) =>
            h("tr", {
              key: i,
              className: cx("border-b border-pulse-line/60 last:border-0", onRowClick ? "cursor-pointer hover:bg-pulse-panel/60" : "hover:bg-pulse-panel/40"),
              onClick: onRowClick ? (e) => { if (e.target.tagName !== "BUTTON" && e.target.closest("button") == null) onRowClick(row); } : undefined,
            },
              columns.map(col => h("td", { key: col.key, className: cx("whitespace-nowrap px-2 py-3", col.className) }, col.render ? col.render(row) : row[col.key]))
            )
          ) : h("tr", null, h("td", { className: "px-3 py-4 text-pulse-muted", colSpan: columns.length }, emptyText || "No rows.")))
        )
      )
    );
  }

  function SlideOver({ title, kicker, onClose, children, width }) {
    return h("div", { className: "fixed inset-0 z-50" },
      h("div", { className: "absolute inset-0 bg-black/70", onClick: onClose }),
      h("section", { className: cx("absolute inset-x-0 bottom-0 max-h-[92dvh] overflow-y-auto rounded-t-2xl border-t border-pulse-line bg-pulse-panel p-4 shadow-2xl md:inset-y-0 md:left-auto md:right-0 md:max-h-none md:rounded-none md:border-l md:border-t-0 md:p-5", width || "md:w-[640px]") },
        h("div", { className: "sticky top-0 z-10 -mx-4 -mt-4 mb-4 flex items-start justify-between gap-3 border-b border-pulse-line bg-pulse-panel/95 p-4 backdrop-blur md:-mx-5 md:-mt-5 md:p-5" },
          h("div", { className: "min-w-0" },
            h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, kicker || "Detail"),
            h("h3", { className: "mt-1 truncate text-xl font-semibold" }, title)
          ),
          h(Button, { onClick: onClose, className: "min-h-9 px-3 shrink-0" }, "Close")
        ),
        children
      )
    );
  }

  function ConfidencePill({ value, score }) {
    const label = String(value || (Number(score) >= 70 ? "high" : Number(score) >= 50 ? "medium" : "low")).toUpperCase();
    const tone = label.includes("HIGH") ? "border-pulse-green/30 bg-pulse-green/10 text-pulse-green" : label.includes("LOW") ? "border-pulse-red/30 bg-pulse-red/10 text-pulse-red" : "border-pulse-amber/30 bg-pulse-amber/10 text-pulse-amber";
    return h(Pill, { className: tone }, label);
  }

  function FactorCluster({ scores }) {
    const config = [["V", "value"], ["M", "momentum"], ["Q", "quality"], ["G", "growth"], ["C", "composite"]];
    const descriptions = {
      value: "Value: P/E, P/B, EV/EBITDA, FCF yield, PEG.",
      momentum: "Momentum: RSI-14, 52w position, price vs 50d SMA.",
      quality: "Quality: ROE, margins, debt/equity.",
      growth: "Growth: revenue, EPS, forward trend.",
      composite: "Composite: blended factor score across V, M, Q, G.",
    };
    return h("div", { className: "flex flex-nowrap items-center gap-1 overflow-x-auto whitespace-nowrap pb-1" },
      config.map(([label, key]) => {
        const value = scores ? scores[key] : null;
        const tone = value == null ? "border-pulse-line text-pulse-dim" : value >= 70 ? "border-pulse-green/30 bg-pulse-green/10 text-pulse-green" : value >= 45 ? "border-pulse-amber/30 bg-pulse-amber/10 text-pulse-amber" : "border-pulse-red/30 bg-pulse-red/10 text-pulse-red";
        return h("span", { key: label, title: descriptions[key], className: cx("inline-flex h-10 min-w-12 flex-col items-center justify-center rounded-md border font-mono text-[10px] font-bold", tone) },
          label,
          h("small", { className: "text-[11px] leading-none opacity-95" }, value == null ? "—" : Math.round(value))
        );
      })
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Login
  // ──────────────────────────────────────────────────────────────

  function Login({ onLogin }) {
    const params = new URLSearchParams(window.location.search);
    const resetToken = params.get("reset_token");
    const [mode, setMode] = useState(resetToken ? "reset" : "login");
    const [username, setUsername] = useState("admin");
    const [password, setPassword] = useState("");
    const [newPassword, setNewPassword] = useState("");
    const [confirm, setConfirm] = useState("");
    const [message, setMessage] = useState("");
    const [busy, setBusy] = useState(false);

    async function submitLogin(e) {
      e.preventDefault();
      setBusy(true); setMessage("");
      try {
        const data = await api("/api/auth/login", { method: "POST", body: JSON.stringify({ username, password }) });
        setToken(data.access_token);
        onLogin();
      } catch (err) { setMessage(err.message || "Could not sign in."); } finally { setBusy(false); }
    }

    async function forgot(e) {
      e.preventDefault();
      setBusy(true); setMessage("");
      try {
        const data = await api("/api/auth/forgot-password", { method: "POST", body: JSON.stringify({ username }) });
        setMessage(data.message || "If that username exists, a reset link has been sent.");
      } catch (err) { setMessage(err.message || "Could not request reset."); } finally { setBusy(false); }
    }

    async function reset(e) {
      e.preventDefault();
      if (newPassword !== confirm) { setMessage("Passwords do not match."); return; }
      setBusy(true); setMessage("");
      try {
        await api("/api/auth/reset-password", { method: "POST", body: JSON.stringify({ token: resetToken, new_password: newPassword }) });
        window.history.replaceState(null, "", "/");
        setMode("login");
        setMessage("Password updated. Please sign in.");
      } catch (err) { setMessage(err.message || "Could not reset password."); } finally { setBusy(false); }
    }

    return h("main", { className: "flex min-h-screen items-center justify-center px-4 py-10" },
      h(Card, { className: "w-full max-w-sm p-6" },
        h("div", { className: "mb-6 flex items-center gap-3" },
          h("img", { src: "/static/logo.svg", className: "h-10 w-10", alt: "" }),
          h("div", null,
            h("h1", { className: "text-xl font-semibold" }, "Stock", h("span", { className: "bg-gradient-to-r from-pulse-cyan to-pulse-magenta bg-clip-text text-transparent" }, "Lens")),
            h("p", { className: "text-xs text-pulse-muted" }, "React mobile-first")
          )
        ),
        mode === "login" ? h("form", { onSubmit: submitLogin, className: "grid gap-3" },
          h("label", { className: "text-xs uppercase tracking-wide text-pulse-muted" }, "Username"),
          h(TextInput, { value: username, onChange: e => setUsername(e.target.value), autoComplete: "username" }),
          h("label", { className: "text-xs uppercase tracking-wide text-pulse-muted" }, "Password"),
          h(TextInput, { value: password, onChange: e => setPassword(e.target.value), type: "password", autoComplete: "current-password" }),
          message ? h("p", { className: "text-sm text-pulse-amber" }, message) : null,
          h(Button, { kind: "primary", disabled: busy, type: "submit", className: "mt-2" }, busy ? "Signing in..." : "Sign in"),
          h("button", { type: "button", onClick: () => { setMode("forgot"); setMessage(""); }, className: "text-sm text-pulse-muted hover:text-pulse-cyan" }, "Forgot password?")
        ) : mode === "forgot" ? h("form", { onSubmit: forgot, className: "grid gap-3" },
          h("label", { className: "text-xs uppercase tracking-wide text-pulse-muted" }, "Username"),
          h(TextInput, { value: username, onChange: e => setUsername(e.target.value) }),
          message ? h("p", { className: "text-sm text-pulse-amber" }, message) : null,
          h(Button, { kind: "primary", disabled: busy, type: "submit", className: "mt-2" }, busy ? "Sending..." : "Send reset link"),
          h("button", { type: "button", onClick: () => { setMode("login"); setMessage(""); }, className: "text-sm text-pulse-muted hover:text-pulse-cyan" }, "Back to sign in")
        ) : h("form", { onSubmit: reset, className: "grid gap-3" },
          h("label", { className: "text-xs uppercase tracking-wide text-pulse-muted" }, "New password"),
          h(TextInput, { value: newPassword, onChange: e => setNewPassword(e.target.value), type: "password" }),
          h("label", { className: "text-xs uppercase tracking-wide text-pulse-muted" }, "Confirm password"),
          h(TextInput, { value: confirm, onChange: e => setConfirm(e.target.value), type: "password" }),
          message ? h("p", { className: "text-sm text-pulse-amber" }, message) : null,
          h(Button, { kind: "primary", disabled: busy, type: "submit", className: "mt-2" }, busy ? "Updating..." : "Set password")
        )
      )
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Shell
  // ──────────────────────────────────────────────────────────────

  function Shell({ user, active, setActive, logout, children }) {
    return h("div", { className: "min-h-screen pb-20 md:pb-0" },
      h("header", { className: "sticky top-0 z-30 border-b border-pulse-line bg-pulse-bg/86 px-3 pt-[max(.75rem,env(safe-area-inset-top))] backdrop-blur md:px-5" },
        h("div", { className: "mx-auto flex max-w-7xl items-center gap-3 py-3" },
          h("img", { src: "/static/logo.svg", className: "h-8 w-8 shrink-0", alt: "" }),
          h("div", { className: "min-w-0" },
            h("div", { className: "text-base font-semibold leading-tight" }, "Stock", h("span", { className: "bg-gradient-to-r from-pulse-cyan to-pulse-magenta bg-clip-text text-transparent" }, "Lens")),
            h("div", { className: "truncate text-[11px] text-pulse-dim" }, user || "signed in", " · v3.5.0")
          ),
          h("a", { href: "/legacy", className: "ml-auto hidden rounded-lg border border-pulse-line px-3 py-2 text-xs text-pulse-muted hover:text-pulse-cyan sm:inline-flex" }, "Legacy"),
          h(Button, { onClick: logout, className: "ml-auto sm:ml-0 min-h-9 px-3 text-xs" }, "Sign out")
        ),
        h("nav", { className: "scrollbar-none mx-auto flex max-w-7xl gap-1 overflow-x-auto pb-2" },
          TABS.map(([id, label]) => h("button", {
            key: id, onClick: () => setActive(id),
            className: cx("shrink-0 rounded-lg px-3 py-2 text-xs font-medium transition",
              active === id ? "bg-pulse-card text-pulse-ink ring-1 ring-pulse-line" : "text-pulse-muted hover:bg-pulse-panel hover:text-pulse-ink")
          }, label))
        )
      ),
      h("main", { className: "mx-auto max-w-7xl px-3 py-4 md:px-5 md:py-6" }, children),
      h("nav", { className: "fixed inset-x-0 bottom-0 z-40 border-t border-pulse-line bg-pulse-bg/95 px-2 pb-[max(.5rem,env(safe-area-inset-bottom))] pt-2 backdrop-blur md:hidden" },
        h("div", { className: "grid grid-cols-5 gap-1" },
          [["screener","Scan"],["watchlist","Watch"],["predictions","Preds"],["recommendations","Sig"],["portfolio","Port"]].map(([id, label]) => h("button", {
            key: id, onClick: () => setActive(id),
            className: cx("rounded-lg px-1 py-2 text-[10px] font-medium", active === id ? "bg-pulse-card text-pulse-cyan" : "text-pulse-muted")
          }, label))
        )
      )
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Stock Detail Slide-Over (used by Screener, Watchlist, Portfolio)
  // ──────────────────────────────────────────────────────────────

  function StockDetail({ ticker, onClose }) {
    const [data, setData] = useState(null);
    const [busy, setBusy] = useState(true);
    const [error, setError] = useState("");
    const [adding, setAdding] = useState(false);
    const [added, setAdded] = useState(false);

    useEffect(() => {
      let alive = true;
      setBusy(true); setError(""); setData(null); setAdded(false);
      api(`/api/stock/${encodeURIComponent(ticker)}`)
        .then(d => { if (alive) setData(d); })
        .catch(err => { if (alive) setError(err.message); })
        .finally(() => { if (alive) setBusy(false); });
      return () => { alive = false; };
    }, [ticker]);

    async function addToWatch() {
      setAdding(true);
      try {
        await api(`/api/watchlist/${encodeURIComponent(ticker)}`, { method: "POST" });
        setAdded(true);
      } catch {} finally { setAdding(false); }
    }

    return h(SlideOver, { title: ticker, kicker: "Stock detail", onClose },
      busy ? h(Empty, null, "Loading...") :
      error ? h(Empty, null, error) :
      data ? h("div", { className: "grid gap-4" },
        h("div", { className: "flex items-start justify-between gap-3" },
          h("div", null,
            h("h3", { className: "text-lg font-semibold" }, data.name || ticker),
            h("div", { className: "text-sm text-pulse-muted" }, data.sector || "")
          ),
          h("div", { className: "text-right" },
            h("div", { className: "font-mono text-2xl" }, data.price != null ? "$" + Number(data.price).toFixed(2) : "—"),
            h("div", { className: cx("font-mono text-sm", deltaTone(data.change_pct)) }, data.change_pct != null ? fmtPct(data.change_pct, 2) : "")
          )
        ),
        h("div", { className: "grid grid-cols-2 gap-2 sm:grid-cols-4" },
          h(Metric, { label: "P/E", value: data.pe ?? "—" }),
          h(Metric, { label: "PEG", value: data.peg ?? "—" }),
          h(Metric, { label: "P/B", value: data.pb ?? "—" }),
          h(Metric, { label: "EV/EBITDA", value: data.ev_ebitda ?? "—" }),
          h(Metric, { label: "FCF Yield", value: data.fcf_yield != null ? data.fcf_yield + "%" : "—" }),
          h(Metric, { label: "Margin", value: data.profit_margin != null ? (data.profit_margin * 100).toFixed(1) + "%" : "—" }),
          h(Metric, { label: "Beta", value: data.beta ?? "—" }),
          h(Metric, { label: "Mkt Cap", value: fmtCap(data.market_cap) }),
          h(Metric, { label: "EPS Growth", value: data.eps_growth != null ? (data.eps_growth * 100).toFixed(1) + "%" : "—" }),
          h(Metric, { label: "Rev Growth", value: data.revenue_growth != null ? (data.revenue_growth * 100).toFixed(1) + "%" : "—" }),
          h(Metric, { label: "52W High", value: data.week_52_high != null ? "$" + Number(data.week_52_high).toFixed(2) : "—" }),
          h(Metric, { label: "52W Low", value: data.week_52_low != null ? "$" + Number(data.week_52_low).toFixed(2) : "—" })
        ),
        Array.isArray(data.history) && data.history.length ? h(Card, { className: "p-4" },
          h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, "Recent close (last 30)"),
          h(Sparkline, { data: data.history.slice(-30).map(p => Number(p.close)) })
        ) : null,
        data.description ? h(Card, { className: "p-4 text-sm leading-relaxed text-pulse-muted" }, data.description) : null,
        h(Button, { kind: added ? "secondary" : "primary", onClick: addToWatch, disabled: adding || added, className: "w-full" }, added ? "✓ In watchlist" : adding ? "Adding..." : "+ Add to watchlist")
      ) : null
    );
  }

  function Sparkline({ data, height }) {
    if (!Array.isArray(data) || data.length < 2) return h("p", { className: "mt-2 text-sm text-pulse-muted" }, "No history.");
    const w = 600, hgt = height || 80;
    const min = Math.min.apply(null, data);
    const max = Math.max.apply(null, data);
    const span = max - min || 1;
    const step = w / (data.length - 1);
    const points = data.map((v, i) => `${(i * step).toFixed(1)},${(hgt - ((v - min) / span) * hgt).toFixed(1)}`).join(" ");
    const last = data[data.length - 1];
    const first = data[0];
    const color = last >= first ? "#3fde7e" : "#ff4d6e";
    return h("svg", { viewBox: `0 0 ${w} ${hgt}`, className: "mt-2 h-20 w-full", preserveAspectRatio: "none" },
      h("polyline", { fill: "none", stroke: color, strokeWidth: 2, points })
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Screener
  // ──────────────────────────────────────────────────────────────

  const SECTORS = ["Technology","Healthcare","Financial Services","Consumer Cyclical","Consumer Defensive","Energy","Industrials","Communication Services","Basic Materials","Utilities","Real Estate"];
  const INDEXES = [["", "All universes"], ["sp500", "S&P 500"], ["nasdaq100", "NASDAQ 100"], ["ftse250", "FTSE 250"]];

  const SCREEN_FILTERS = [
    { id: "pe",         label: "P/E",            backend: "pe",          scale: 1,     defaultOp: "max", hint: "Share price ÷ EPS · <15 cheap, 15-25 fair, >30 expensive" },
    { id: "peg",        label: "PEG",            backend: "peg",         scale: 1,     defaultOp: "max", hint: "P/E ÷ growth rate · <1 undervalued, 1-2 fair, >2 expensive" },
    { id: "pb",         label: "P/B",            backend: "pb",          scale: 1,     defaultOp: "max", hint: "Price ÷ book · <1.5 near assets, >5 high premium" },
    { id: "ev",         label: "EV/EBITDA",      backend: "ev_ebitda",   scale: 1,     defaultOp: "max", hint: "Enterprise value ÷ operating profit · <10 cheap, >20 expensive" },
    { id: "fcf",        label: "FCF Yield %",    backend: "fcf_yield",   scale: 1,     defaultOp: "min", hint: "FCF ÷ market cap · >5% strong, <2% weak" },
    { id: "cap",        label: "Market Cap $B",  backend: "market_cap",  scale: 1e9,   defaultOp: "min", hint: ">$200B mega · $10-200B large · $2-10B mid · <$2B small" },
    { id: "vol",        label: "Avg Vol (M)",    backend: "volume",      scale: 1e6,   defaultOp: "min", hint: ">1M liquid · <500K thin" },
    { id: "rev-growth", label: "Rev Growth %",   backend: "rev_growth",  scale: 1,     defaultOp: "min", hint: ">10% strong · 5-10% decent · <5% slow · negative shrinking" },
  ];

  function Screener({ openDetail }) {
    const [filters, setFilters] = useState(() => {
      const init = { index: "", sector: "", search: "" };
      SCREEN_FILTERS.forEach(f => { init[f.id] = ""; init[f.id + "_op"] = f.defaultOp; });
      return init;
    });
    const [rows, setRows] = useState([]);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState("");
    const [status, setStatus] = useState("");

    function setField(name, value) {
      setFilters(prev => Object.assign({}, prev, { [name]: value }));
    }

    function toggleOp(id) {
      setFilters(prev => Object.assign({}, prev, { [id + "_op"]: prev[id + "_op"] === "max" ? "min" : "max" }));
    }

    async function run() {
      setBusy(true); setError(""); setStatus(filters.search ? `Searching "${filters.search}"...` : "Screening stocks...");
      try {
        const params = new URLSearchParams();
        if (filters.index) params.set("index", filters.index);
        if (filters.sector) params.set("sector", filters.sector);
        if (filters.search) params.set("q", filters.search);
        SCREEN_FILTERS.forEach(f => {
          const v = filters[f.id];
          if (v === "" || v == null) return;
          const num = parseFloat(v) * (f.scale || 1);
          if (!Number.isFinite(num)) return;
          params.set(`${filters[f.id + "_op"]}_${f.backend}`, String(num));
        });
        const data = await api(`/api/screen?${params.toString()}`);
        const arr = Array.isArray(data) ? data : (data.results || []);
        setRows(arr);
        setStatus(arr.length === 0 ? "No stocks matched your criteria." : `${arr.length} stock${arr.length !== 1 ? "s" : ""} found.`);
      } catch (err) {
        setError(err.message);
      } finally {
        setBusy(false);
      }
    }

    useEffect(() => { run(); }, []); // eslint-disable-line

    async function addToWatch(ev, ticker) {
      ev.stopPropagation();
      try { await api(`/api/watchlist/${encodeURIComponent(ticker)}`, { method: "POST" }); ev.target.textContent = "✓ Added"; ev.target.disabled = true; } catch {}
    }

    const columns = [
      { key: "ticker", label: "Ticker", render: r => h("strong", { className: "font-mono text-pulse-cyan" }, r.ticker) },
      { key: "name", label: "Name", render: r => h("span", { className: "text-pulse-muted" }, r.name || "—") },
      { key: "sector", label: "Sector", render: r => r.sector || "—" },
      { key: "price", label: "Price", className: "font-mono", render: r => r.price != null ? "$" + Number(r.price).toFixed(2) : "—" },
      { key: "pe", label: "P/E", className: "font-mono", render: r => r.pe ?? "—" },
      { key: "peg", label: "PEG", className: "font-mono", render: r => r.peg ?? "—" },
      { key: "pb", label: "P/B", className: "font-mono", render: r => r.pb ?? "—" },
      { key: "ev_ebitda", label: "EV/EBITDA", className: "font-mono", render: r => r.ev_ebitda ?? "—" },
      { key: "fcf_yield", label: "FCF Y", className: "font-mono", render: r => r.fcf_yield != null ? r.fcf_yield + "%" : "—" },
      { key: "rev_growth", label: "Rev Gr", className: "font-mono", render: r => r.rev_growth != null ? r.rev_growth + "%" : "—" },
      { key: "cap", label: "Mkt Cap", className: "font-mono", render: r => fmtCap(r.market_cap) },
      { key: "actions", label: "", render: r => h(Button, { onClick: ev => addToWatch(ev, r.ticker), className: "min-h-8 px-2 text-xs" }, "+ Watch") },
    ];

    return h("div", null,
      h(SectionHead, { title: "Screener", kicker: "Mobile-first research", subtitle: "Filter the universe by valuation, growth, liquidity and quality." }),
      h(Card, { className: "mb-4 p-4" },
        h("div", { className: "grid gap-3 sm:grid-cols-2 lg:grid-cols-4" },
          h("div", null,
            h("label", { className: "block text-[10px] font-mono uppercase tracking-wide text-pulse-dim mb-1" }, "Index"),
            h(Select, { value: filters.index, onChange: e => setField("index", e.target.value) },
              INDEXES.map(([v, label]) => h("option", { key: v, value: v }, label))
            )
          ),
          h("div", null,
            h("label", { className: "block text-[10px] font-mono uppercase tracking-wide text-pulse-dim mb-1" }, "Sector"),
            h(Select, { value: filters.sector, onChange: e => setField("sector", e.target.value) },
              h("option", { value: "" }, "All sectors"),
              SECTORS.map(s => h("option", { key: s, value: s }, s))
            )
          ),
          SCREEN_FILTERS.map(f => h("div", { key: f.id },
            h("label", { className: "block text-[10px] font-mono uppercase tracking-wide text-pulse-dim mb-1" }, f.label, h("span", { className: "ml-1 text-pulse-dim", title: f.hint }, "ⓘ")),
            h("div", { className: "flex gap-1" },
              h("button", {
                type: "button",
                onClick: () => toggleOp(f.id),
                title: "Toggle ≤ / ≥",
                className: "h-11 w-11 shrink-0 rounded-lg border border-pulse-line bg-pulse-panel text-pulse-cyan font-mono text-base hover:border-pulse-cyan",
              }, filters[f.id + "_op"] === "max" ? "≤" : "≥"),
              h(TextInput, { type: "number", step: "any", placeholder: "—", value: filters[f.id], onChange: e => setField(f.id, e.target.value) })
            )
          ))
        ),
        h("div", { className: "mt-3 flex flex-col gap-2 sm:flex-row" },
          h(TextInput, { placeholder: "Search by ticker or name (Enter)", value: filters.search, onChange: e => setField("search", e.target.value), onKeyDown: e => { if (e.key === "Enter") run(); } }),
          h(Button, { kind: "primary", onClick: run, disabled: busy }, busy ? "Running..." : "Run screen")
        ),
        status ? h("p", { className: "mt-3 text-sm text-pulse-muted" }, status) : null,
        error ? h("p", { className: "mt-3 text-sm text-pulse-red" }, error) : null
      ),
      h(ResponsiveTable, {
        columns, rows,
        minWidth: 1100,
        onRowClick: r => openDetail(r.ticker),
        mobileRender: r => h("div", { className: "grid gap-3" },
          h("div", { className: "flex items-start justify-between gap-3" },
            h("div", null, h("div", { className: "font-mono text-lg text-pulse-cyan" }, r.ticker), h("div", { className: "text-sm text-pulse-muted" }, r.name || "—")),
            h("div", { className: "font-mono text-sm text-right" },
              h("div", null, r.price != null ? "$" + Number(r.price).toFixed(2) : "—"),
              h("div", { className: "text-xs text-pulse-muted" }, r.sector || "—")
            )
          ),
          h("div", { className: "grid grid-cols-3 gap-2" },
            h(Metric, { label: "P/E", value: r.pe ?? "—" }),
            h(Metric, { label: "PEG", value: r.peg ?? "—" }),
            h(Metric, { label: "FCF Y", value: r.fcf_yield != null ? r.fcf_yield + "%" : "—" }),
            h(Metric, { label: "Rev Gr", value: r.rev_growth != null ? r.rev_growth + "%" : "—" }),
            h(Metric, { label: "Cap", value: fmtCap(r.market_cap) }),
            h(Metric, { label: "P/B", value: r.pb ?? "—" })
          ),
          h(Button, { onClick: ev => addToWatch(ev, r.ticker), className: "w-full min-h-9 text-xs" }, "+ Watch")
        )
      })
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Watchlist
  // ──────────────────────────────────────────────────────────────

  function Watchlist({ openDetail }) {
    const [rows, setRows] = useState([]);
    const [ticker, setTicker] = useState("");
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState("");

    async function load() {
      setBusy(true); setError("");
      try { setRows(await api("/api/watchlist")); } catch (err) { setError(err.message); } finally { setBusy(false); }
    }
    async function add() {
      if (!ticker.trim()) return;
      try { await api(`/api/watchlist/${encodeURIComponent(ticker.trim().toUpperCase())}`, { method: "POST" }); setTicker(""); load(); } catch (err) { setError(err.message); }
    }
    async function remove(sym) {
      try { await api(`/api/watchlist/${encodeURIComponent(sym)}`, { method: "DELETE" }); load(); } catch (err) { setError(err.message); }
    }
    useEffect(() => { load(); }, []);

    return h("div", null,
      h(SectionHead, { title: "Watchlist", subtitle: "Pinned tickers with live prices.", actions: [h(Button, { key: "refresh", onClick: load, disabled: busy }, "Refresh")] }),
      h(Card, { className: "mb-4 p-4" },
        h("div", { className: "flex gap-2" },
          h(TextInput, { placeholder: "Ticker", value: ticker, onChange: e => setTicker(e.target.value.toUpperCase()), onKeyDown: e => { if (e.key === "Enter") add(); } }),
          h(Button, { kind: "primary", onClick: add }, "Add")
        ),
        error ? h("p", { className: "mt-3 text-sm text-pulse-red" }, error) : null
      ),
      h(ResponsiveTable, {
        rows,
        onRowClick: r => openDetail(r.ticker),
        emptyText: busy ? "Loading..." : "No watchlist items yet.",
        columns: [
          { key: "ticker", label: "Ticker", render: r => h("strong", { className: "font-mono text-pulse-cyan" }, r.ticker) },
          { key: "name", label: "Name", render: r => r.name || "—" },
          { key: "price", label: "Price", className: "font-mono", render: r => r.price != null ? "$" + Number(r.price).toFixed(2) : "—" },
          { key: "change", label: "Change", className: "font-mono", render: r => h("span", { className: deltaTone(r.change_pct) }, fmtPct(r.change_pct, 2)) },
          { key: "actions", label: "", render: r => h(Button, { onClick: e => { e.stopPropagation(); remove(r.ticker); }, className: "min-h-8 px-2 text-xs" }, "Remove") },
        ],
        mobileRender: r => h("div", { className: "flex items-center justify-between gap-3" },
          h("div", null, h("div", { className: "font-mono text-lg text-pulse-cyan" }, r.ticker), h("div", { className: "text-sm text-pulse-muted" }, r.name || "—")),
          h("div", { className: "text-right" }, h("div", { className: "font-mono" }, r.price != null ? "$" + Number(r.price).toFixed(2) : "—"), h("div", { className: cx("text-xs", deltaTone(r.change_pct)) }, fmtPct(r.change_pct, 2))),
          h(Button, { onClick: e => { e.stopPropagation(); remove(r.ticker); }, className: "min-h-9 px-2 text-xs" }, "Remove")
        )
      })
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Sentiment
  // ──────────────────────────────────────────────────────────────

  function Sentiment() {
    const [ticker, setTicker] = useState("");
    const [data, setData] = useState(null);
    const [busy, setBusy] = useState(false);
    const [status, setStatus] = useState("");

    async function scan(scope) {
      setBusy(true);
      setStatus(scope === "ticker" ? `Scanning ${ticker}...` : scope === "list" ? "Loading watchlist..." : "Scanning watchlist...");
      try {
        const url = scope === "ticker" ? `/api/sentiment?ticker=${encodeURIComponent(ticker)}`
                  : scope === "list"   ? `/api/sentiment?watchlist=true`
                  :                       `/api/sentiment`;
        const d = await api(url);
        setData({ scope, payload: d });
        if (scope === "list") setStatus(`Loaded ${(d.watchlist || []).length} watchlist ticker${(d.watchlist || []).length === 1 ? "" : "s"}.`);
        else if (scope === "scan") setStatus(`Scanned ${(d.results || []).length} ticker${(d.results || []).length === 1 ? "" : "s"}.`);
        else setStatus("Done.");
      } catch (err) { setStatus("Error: " + err.message); } finally { setBusy(false); }
    }

    function sentimentTone(label, score) {
      const l = String(label || "").toLowerCase();
      if (l.includes("bull")) return "text-pulse-green";
      if (l.includes("bear")) return "text-pulse-red";
      if (l === "neutral") return "text-pulse-amber";
      const n = Number(score);
      if (!Number.isFinite(n)) return "text-pulse-muted";
      if (n > 0) return "text-pulse-green";
      if (n < 0) return "text-pulse-red";
      return "text-pulse-amber";
    }

    function sentimentBadge(label) {
      const l = String(label || "").toLowerCase();
      if (l.includes("bull")) return "border-pulse-green/30 bg-pulse-green/10 text-pulse-green";
      if (l.includes("bear")) return "border-pulse-red/30 bg-pulse-red/10 text-pulse-red";
      if (l === "neutral") return "border-pulse-amber/30 bg-pulse-amber/10 text-pulse-amber";
      return "border-pulse-line text-pulse-muted";
    }

    function recoTone(rec) {
      const r = String(rec || "").toLowerCase();
      if (r.includes("strong_buy") || r === "buy") return "text-pulse-green";
      if (r.includes("strong_sell") || r === "sell") return "text-pulse-red";
      return "text-pulse-muted";
    }

    function renderResultCard(item, i) {
      return h(Card, { key: i, className: "p-4" },
        h("div", { className: "flex items-start justify-between gap-3" },
          h("div", { className: "min-w-0" },
            h("div", { className: "font-mono text-lg text-pulse-cyan" }, item.ticker || item.symbol || "—"),
            h("div", { className: "truncate text-sm text-pulse-muted" }, item.name || "")
          ),
          h("div", { className: "text-right" },
            h("div", { className: "font-mono text-base" }, item.price != null ? "$" + Number(item.price).toFixed(2) : "—"),
            item.change_pct != null ? h("div", { className: cx("font-mono text-xs", deltaTone(item.change_pct)) }, fmtPct(item.change_pct, 2)) : null
          )
        ),
        h("div", { className: "mt-3 flex flex-wrap items-center gap-2" },
          item.sentiment ? h(Pill, { className: sentimentBadge(item.sentiment) }, String(item.sentiment).toUpperCase()) : null,
          item.sentiment_score != null ? h("span", { className: cx("font-mono text-xs", sentimentTone(item.sentiment, item.sentiment_score)) }, "score ", item.sentiment_score >= 0 ? "+" : "", item.sentiment_score) : null,
          item.recommendation ? h("span", { className: cx("font-mono text-xs uppercase", recoTone(item.recommendation)) }, String(item.recommendation).replace("_", " ")) : null,
          item.target_mean_price != null ? h("span", { className: "font-mono text-xs text-pulse-muted" }, "target $", Number(item.target_mean_price).toFixed(2)) : null
        ),
        item.summary ? h("p", { className: "mt-3 text-sm text-pulse-muted" }, item.summary) : null,
        Array.isArray(item.headlines) && item.headlines.length ? h("div", { className: "mt-3" },
          h("div", { className: "font-mono text-[10px] uppercase tracking-[0.18em] text-pulse-dim" }, "Recent headlines"),
          h("ul", { className: "mt-2 grid gap-1 text-sm text-pulse-muted" }, item.headlines.slice(0, 5).map((line, j) =>
            h("li", { key: j, className: "list-disc pl-5" }, typeof line === "string" ? line : (line.title || line.headline || JSON.stringify(line)))
          ))
        ) : item.headline_count === 0 ? h("p", { className: "mt-3 text-xs text-pulse-dim" }, "No recent headlines.") : null,
        item.error ? h("p", { className: "mt-3 text-sm text-pulse-red" }, item.error) : null
      );
    }

    function renderBody() {
      if (!data) return h(Empty, null, busy ? "Loading..." : "Run a scan to see sentiment.");
      const { scope, payload } = data;

      // Plain string list (list-watchlist mode) — payload.watchlist is array of tickers
      if (scope === "list" && Array.isArray(payload.watchlist)) {
        if (payload.watchlist.length === 0) return h(Empty, null, "Your watchlist is empty. Add tickers in the Watchlist tab.");
        return h(Card, { className: "p-4" },
          h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim mb-3" }, `${payload.watchlist.length} watchlist ticker${payload.watchlist.length === 1 ? "" : "s"} — click to scan`),
          h("div", { className: "flex flex-wrap gap-2" }, payload.watchlist.map(t => h("button", {
            key: t,
            onClick: () => { setTicker(t); scan("ticker"); },
            className: "rounded-lg border border-pulse-line bg-pulse-panel px-3 py-2 font-mono text-sm text-pulse-cyan hover:border-pulse-cyan/60"
          }, t)))
        );
      }

      // Single ticker scan — payload is the result object itself
      if (scope === "ticker" && payload.ticker) {
        return h("div", { className: "grid gap-3 md:grid-cols-2" }, renderResultCard(payload, 0));
      }

      // Watchlist scan — payload.results is array of result objects
      const items = Array.isArray(payload.results) ? payload.results : (Array.isArray(payload) ? payload : []);
      if (items.length === 0) return h(Empty, null, payload.detail || "No sentiment results.");
      return h("div", { className: "grid gap-3 md:grid-cols-2" }, items.map(renderResultCard));
    }

    return h("div", null,
      h(SectionHead, { title: "Sentiment Scanner", kicker: "News + signals", subtitle: "Live news and social sentiment for a ticker or your full watchlist." }),
      h(Card, { className: "mb-4 p-4" },
        h("div", { className: "grid gap-2 sm:grid-cols-[1fr_auto_auto_auto]" },
          h(TextInput, { value: ticker, onChange: e => setTicker(e.target.value.toUpperCase()), placeholder: "Ticker (e.g. NVDA)", onKeyDown: e => { if (e.key === "Enter" && ticker.trim()) scan("ticker"); } }),
          h(Button, { onClick: () => ticker.trim() && scan("ticker"), disabled: busy }, "Scan ticker"),
          h(Button, { onClick: () => scan("list"), disabled: busy }, "List watchlist"),
          h(Button, { kind: "primary", onClick: () => scan("scan"), disabled: busy }, busy ? "Working..." : "Scan watchlist")
        ),
        h(Status, { message: status, className: "mt-3" })
      ),
      renderBody()
    );
  }

  // ──────────────────────────────────────────────────────────────
  // AI Advisor
  // ──────────────────────────────────────────────────────────────

  function AIAdvisor() {
    const [researchQuery, setResearchQuery] = useState("");
    const [researchOut, setResearchOut] = useState("");
    const [researchBusy, setResearchBusy] = useState(false);
    const [researchStatus, setResearchStatus] = useState("");

    const [askQuery, setAskQuery] = useState("");
    const [askOut, setAskOut] = useState("");
    const [askBusy, setAskBusy] = useState(false);
    const [askStatus, setAskStatus] = useState("");

    async function research() {
      const q = researchQuery.trim();
      if (!q) return;
      setResearchBusy(true); setResearchOut(""); setResearchStatus("Running multi-signal stock research...");
      try {
        const data = await api("/api/stock-research", { method: "POST", body: JSON.stringify({ query: q }) });
        setResearchOut(data.response || data.detail || "(empty response)");
        setResearchStatus("Done.");
      } catch (err) { setResearchStatus("Error: " + err.message); } finally { setResearchBusy(false); }
    }

    async function ask() {
      const q = askQuery.trim();
      if (!q) return;
      setAskBusy(true); setAskOut(""); setAskStatus("Asking Claude...");
      try {
        const data = await api("/api/recommend", { method: "POST", body: JSON.stringify({ query: q }) });
        setAskOut(data.response || data.detail || "(empty response)");
        setAskStatus("");
      } catch (err) { setAskStatus("Error: " + err.message); } finally { setAskBusy(false); }
    }

    return h("div", null,
      h(SectionHead, { title: "AI Advisor", kicker: "Claude · research + chat", subtitle: "Live fundamentals & news pipeline (Research), or open-ended investment chat (Ask)." }),
      h("div", { className: "grid gap-4 lg:grid-cols-2" },
        h(Card, { className: "p-4" },
          h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, "Stock Research"),
          h("h3", { className: "mt-1 text-lg font-semibold" }, "Live data → Claude narrative"),
          h("p", { className: "mt-1 text-sm text-pulse-muted" }, "Enter one or more tickers. Pulls live fundamentals, news, analyst & technical signals, sends only the data to Claude."),
          h(TextInput, { className: "mt-3", placeholder: "e.g. NVDA AMD TSM", value: researchQuery, onChange: e => setResearchQuery(e.target.value), onKeyDown: e => { if (e.key === "Enter") research(); } }),
          h(Button, { kind: "primary", onClick: research, disabled: researchBusy, className: "mt-3 w-full" }, researchBusy ? "Researching..." : "Research stock"),
          h(Status, { message: researchStatus, className: "mt-3" }),
          researchOut ? h("div", { className: "mt-3 rounded-lg border border-pulse-line bg-pulse-panel/70 p-3 text-sm leading-relaxed text-pulse-ink whitespace-pre-wrap" }, researchOut) : null
        ),
        h(Card, { className: "p-4" },
          h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, "General Question"),
          h("h3", { className: "mt-1 text-lg font-semibold" }, "Ask Claude"),
          h("p", { className: "mt-1 text-sm text-pulse-muted" }, "Describe what you're looking for and Claude will suggest stocks with brief analysis."),
          h(TextArea, { rows: 5, className: "mt-3", placeholder: "e.g. dividend-paying tech stocks with stable earnings...", value: askQuery, onChange: e => setAskQuery(e.target.value) }),
          h(Button, { kind: "primary", onClick: ask, disabled: askBusy, className: "mt-3 w-full" }, askBusy ? "Thinking..." : "Ask Claude"),
          h(Status, { message: askStatus, className: "mt-3" }),
          askOut ? h("div", { className: "mt-3 rounded-lg border border-pulse-line bg-pulse-panel/70 p-3 text-sm leading-relaxed text-pulse-ink whitespace-pre-wrap" }, askOut) : null
        )
      )
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Predictions
  // ──────────────────────────────────────────────────────────────

  const PRED_PERIODS = [["all", "All"], ["today", "Today"], ["week", "This Week"], ["month", "This Month"], ["ytd", "YTD"]];

  function Predictions() {
    const [rows, setRows] = useState([]);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState("");
    const [status, setStatus] = useState("");
    const [selected, setSelected] = useState(null);
    const [period, setPeriod] = useState("all");
    const [showFactorGuide, setShowFactorGuide] = useState(false);
    const [learning, setLearning] = useState(null);
    const [rebuilding, setRebuilding] = useState(false);

    async function load() {
      setBusy(true); setError("");
      try {
        const data = await api("/api/predictions");
        setRows(Array.isArray(data) ? data : (data.predictions || data.rows || []));
      } catch (err) { setError(err.message); } finally { setBusy(false); }
    }
    async function loadLearning(evaluate) {
      try {
        const data = await api(`/api/predictions/learning?evaluate=${evaluate ? "true" : "false"}`);
        setLearning(data);
      } catch (err) {
        console.warn("Prediction learning summary unavailable", err);
      }
    }
    async function waitForGenerateCompletion(maxPolls = 40, delayMs = 1500) {
      for (let i = 0; i < maxPolls; i++) {
        await new Promise(r => setTimeout(r, delayMs));
        const job = await api("/api/predictions/generate/status");
        if (!job || !job.running) return job || {};
      }
      return { running: true };
    }
    async function generate() {
      setBusy(true); setError("");
      try {
        const started = await api("/api/predictions/generate", { method: "POST" });
        setStatus(started?.status === "already_running" ? "Generation already running. Waiting for completion..." : "Generating today's predictions...");
        const job = await waitForGenerateCompletion();
        if (job.running) {
          setStatus("Generation is taking longer than expected. Refresh actuals shortly.");
        } else if (job.error) {
          setError(`Generate failed: ${job.error}`);
          setStatus("");
        } else {
          const count = Number(job.count || 0);
          setStatus(`Generation complete${count > 0 ? `: ${count} predictions` : ""}.`);
        }
        await load();
        loadLearning(true);
      } catch (err) {
        setError(err.message);
        setStatus("");
      } finally {
        setBusy(false);
      }
    }
    async function backfill() {
      setBusy(true); setError("");
      try {
        const result = await api("/api/predictions/backfill-factors", { method: "POST" });
        setStatus(result?.message || "Backfill started.");
        await load();
        loadLearning(false);
      } catch (err) {
        setError(err.message);
        setStatus("");
      } finally {
        setBusy(false);
      }
    }
    async function rebuildCalibration() {
      setRebuilding(true); setError("");
      try {
        const result = await api("/api/predictions/calibration/rebuild", { method: "POST" });
        setStatus(result?.governance?.message || "Calibration model rebuilt.");
        await load();
        await loadLearning(false);
      } catch (err) {
        setError(err.message);
        setStatus("");
      } finally {
        setRebuilding(false);
      }
    }
    useEffect(() => { load(); loadLearning(true); }, []);

    const filtered = useMemo(() => filterByPeriod(rows, period), [rows, period]);
    const accuracy = useMemo(() => computeAccuracy(filtered), [filtered]);

    return h("div", null,
      h(SectionHead, { title: "Predictions", kicker: "Daily ranking", subtitle: "Multi-horizon forecasts with factor scores. Claude analyses macro trends, news and fundamentals.", actions: [
        h(Button, { key: "refresh", onClick: async () => { await load(); loadLearning(false); }, disabled: busy }, "Refresh actuals"),
        h(Button, { key: "back", onClick: backfill, disabled: busy, title: "Retry factor scores for today's predictions" }, "Backfill factors"),
        h(Button, { key: "rebuild", onClick: rebuildCalibration, disabled: busy || rebuilding, title: "Rebuild the adaptive calibration model" }, rebuilding ? "Rebuilding..." : "Rebuild model"),
        h(Button, { key: "gen", kind: "primary", onClick: generate, disabled: busy }, busy ? "Working..." : "Generate today"),
      ] }),
      error ? h(Empty, null, error) : null,
      status ? h(Status, { message: status, tone: "ok", className: "mb-3" }) : null,
      h("div", { className: "mb-3 flex flex-wrap gap-2" },
        PRED_PERIODS.map(([k, label]) => h("button", {
          key: k, onClick: () => setPeriod(k),
          className: cx("rounded-lg border px-3 py-1.5 text-xs font-mono", period === k ? "border-pulse-cyan bg-pulse-cyan/10 text-pulse-cyan" : "border-pulse-line text-pulse-muted hover:border-pulse-cyan/50")
        }, label))
      ),
      accuracy ? h(Card, { className: "mb-4 p-4" },
        h("div", { className: "grid grid-cols-2 gap-3 sm:grid-cols-4" },
          h(Metric, { label: "Directional Acc.", value: accuracy.acc_pct + "%", tone: accuracy.acc_pct >= 60 ? "text-pulse-green" : accuracy.acc_pct >= 50 ? "text-pulse-amber" : "text-pulse-red" }),
          h(Metric, { label: "Total Preds", value: accuracy.total }),
          h(Metric, { label: "Avg Predicted", value: fmtPct(accuracy.avg_pred, 1) }),
          h(Metric, { label: "Avg Actual", value: fmtPct(accuracy.avg_actual, 1) })
        )
      ) : null,
      learning ? h(PredictionLearningPanel, { learning }) : null,
      h(Card, { className: "mb-4" },
        h("button", {
          className: "flex w-full items-center justify-between p-4 text-left",
          onClick: () => setShowFactorGuide(!showFactorGuide),
        },
          h("span", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, showFactorGuide ? "Hide factor guide" : "How to read Factor Scores"),
          h("span", { className: "text-pulse-muted" }, showFactorGuide ? "−" : "+")
        ),
        showFactorGuide ? h("div", { className: "border-t border-pulse-line p-4 text-sm leading-relaxed text-pulse-muted" },
          h("div", { className: "grid gap-2 md:grid-cols-2" },
            h("p", null, h("strong", { className: "text-pulse-ink" }, "V "), "— Value: P/E, P/B, EV/EBITDA, FCF yield, PEG. ≥70 cheap."),
            h("p", null, h("strong", { className: "text-pulse-ink" }, "M "), "— Momentum: RSI-14, 52w position, price vs 50d SMA. ≥70 bullish."),
            h("p", null, h("strong", { className: "text-pulse-ink" }, "Q "), "— Quality: ROE, gross & net margin, debt/equity. ≥70 strong balance sheet."),
            h("p", null, h("strong", { className: "text-pulse-ink" }, "G "), "— Growth: revenue, EPS, fwd P/E trend. ≥70 accelerating."),
            h("p", null, h("strong", { className: "text-pulse-ink" }, "C "), "— Composite: average of V+M+Q+G. ≥70 broad conviction."),
            h("p", null, h("strong", { className: "text-pulse-ink" }, "MoS "), "— Margin of Safety from DCF. Positive = trading below estimated fair value, negative = trading above.")
          ),
          h("p", { className: "mt-3" }, h("strong", { className: "text-pulse-ink" }, "Reading:"), " Start with C as the headline (≥65 solid). Check pillars — high V + low Q = cheap-but-risky. The best setups combine V≥65, M≥60, Q≥60.")
        ) : null
      ),
      h("div", { className: "grid gap-3 lg:hidden" },
        filtered.length ? filtered.map((p, i) => h(PredictionCard, { key: i, p, onOpen: () => setSelected(p) })) : h(Empty, null, busy ? "Loading..." : "No predictions for this period.")
      ),
      h("div", { className: "hidden overflow-x-auto rounded-xl border border-pulse-line bg-pulse-card lg:block" },
        h("table", { className: "min-w-[1200px] text-sm" },
          h("thead", { className: "bg-pulse-panel font-mono text-[10px] uppercase tracking-[0.16em] text-pulse-dim" },
            h("tr", null, ["Date", "Ticker", "Current", "Signal", "Factors", "MoS", "3M", "6M", "12M", "Actual", "Variance", "Result", "Confidence", ""].map((x, i) => h("th", { className: "px-3 py-3 text-left", key: i }, x)))
          ),
          h("tbody", null, filtered.length ? filtered.map((p, i) => h(PredictionRow, { key: i, p, onOpen: () => setSelected(p) })) :
            h("tr", null, h("td", { className: "px-3 py-4 text-pulse-muted", colSpan: 14 }, busy ? "Loading..." : "No predictions for this period.")))
        )
      ),
      selected ? h(SlideOver, { title: `${selected.ticker} prediction`, kicker: "Prediction thesis", onClose: () => setSelected(null) },
        h("div", { className: "grid gap-4" },
          h("div", { className: "grid grid-cols-2 gap-2 sm:grid-cols-3" },
            h(Metric, { label: "Score", value: selected.score == null ? "—" : `${selected.score}/100`, tone: scoreTone(selected.score) }),
            h(Metric, { label: "3M", value: fmtPct(selected.predicted_3m_pct, 1) }),
            h(Metric, { label: "12M", value: fmtPct(selected.predicted_12m_pct, 1) })
          ),
          h(Card, { className: "p-4" }, h(FactorCluster, { scores: selected.factor_scores || {} })),
          selected.dcf && selected.dcf.margin_of_safety_pct != null ? h(Metric, { label: "DCF Margin of Safety", value: fmtPct(selected.dcf.margin_of_safety_pct, 0), tone: selected.dcf.margin_of_safety_pct >= 0 ? "text-pulse-green" : "text-pulse-red" }) : null,
          selected.learning_adjustment ? h(Card, { className: "p-4" },
            h("div", { className: "font-mono text-[10px] uppercase tracking-[0.2em] text-pulse-cyan" }, "Learning adjustment"),
            h("div", { className: "mt-3 grid grid-cols-1 gap-2 sm:grid-cols-3" },
              h(Metric, { label: "Total", value: fmtPct(selected.learning_adjustment.total_adjustment, 2) }),
              h(Metric, { label: "Bias", value: fmtPct(selected.learning_adjustment.bias_adjustment, 2) }),
              h(Metric, { label: "Factors", value: fmtPct(selected.learning_adjustment.factor_adjustment, 2) })
            ),
            h("p", { className: "mt-3 text-xs text-pulse-muted" }, (selected.learning_adjustment.notes || []).join(" · ") || "No active adjustment for this ticker.")
          ) : null,
          h(Card, { className: "p-4 text-sm leading-relaxed text-pulse-muted whitespace-pre-wrap" }, selected.reasoning || "No reasoning available.")
        )
      ) : null
    );
  }

  function filterByPeriod(preds, period) {
    if (period === "all") return preds;
    const toLocalIso = (dt) => `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, "0")}-${String(dt.getDate()).padStart(2, "0")}`;
    const today = new Date();
    const todayIso = toLocalIso(today);
    const startOfWeek = new Date(today); startOfWeek.setHours(0,0,0,0);
    const dow = startOfWeek.getDay(); startOfWeek.setDate(startOfWeek.getDate() + (dow === 0 ? -6 : 1 - dow));
    const weekIso = toLocalIso(startOfWeek);
    const monthIso = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-01`;
    const ytdIso = `${today.getFullYear()}-01-01`;
    return preds.filter(p => {
      const d = String(p.date || "");
      if (period === "today") return d === todayIso;
      if (period === "week")  return d >= weekIso && d <= todayIso;
      if (period === "month") return d >= monthIso && d <= todayIso;
      if (period === "ytd")   return d >= ytdIso && d <= todayIso;
      return true;
    });
  }

  function computeAccuracy(preds) {
    const scored = preds.filter(p => p.actual_pct != null && p.predicted_pct != null);
    if (scored.length === 0) return null;
    let correct = 0, sumPred = 0, sumActual = 0;
    scored.forEach(p => {
      if (Math.sign(p.predicted_pct) === Math.sign(p.actual_pct)) correct += 1;
      sumPred += Number(p.predicted_pct);
      sumActual += Number(p.actual_pct);
    });
    return {
      total: scored.length,
      acc_pct: Math.round((correct / scored.length) * 100),
      avg_pred: sumPred / scored.length,
      avg_actual: sumActual / scored.length,
    };
  }

  function PredictionLearningPanel({ learning }) {
    const horizons = learning.by_horizon || [];
    const calibration = learning.calibration || {};
    const governance = learning.governance || {};
    const global1d = (calibration.global || {})["1d"] || {};
    const factors1d = (calibration.factor_learning || {})["1d"] || {};
    const factorRows = Object.entries(factors1d)
      .filter(([, data]) => data && data.correlation != null)
      .sort((a, b) => Math.abs(Number(b[1].correlation || 0)) - Math.abs(Number(a[1].correlation || 0)))
      .slice(0, 4);
    const plainPct = (value) => {
      const n = Number(value);
      return Number.isFinite(n) ? `${n.toFixed(1)}%` : "—";
    };
    const modelLabel = [learning.model_version, learning.prompt_version].filter(Boolean).join(" · ") || "versioned";
    const gateTone = {
      green: "border-pulse-green/40 bg-pulse-green/10 text-pulse-green",
      amber: "border-pulse-amber/40 bg-pulse-amber/10 text-pulse-amber",
      red: "border-pulse-red/40 bg-pulse-red/10 text-pulse-red",
      muted: "border-pulse-line bg-pulse-panel text-pulse-muted",
    }[governance.tone || "muted"];
    return h(Card, { className: "mb-4 p-4" },
      h("div", { className: "mb-3 flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between" },
        h("div", null,
          h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, "Learning loop"),
          h("h3", { className: "mt-1 text-lg font-semibold text-pulse-ink" }, "Prediction outcome memory"),
          h("p", { className: "mt-1 text-sm text-pulse-muted" }, `Every generated signal is stored for 12 months, then evaluated when each horizon matures.`)
        ),
        h("div", { className: "rounded-lg border border-pulse-line bg-pulse-panel px-3 py-2 text-xs text-pulse-muted" },
          modelLabel
        )
      ),
      governance.message ? h("div", { className: cx("mb-3 rounded-lg border px-3 py-2 text-sm", gateTone) },
        h("div", { className: "font-semibold" }, (governance.status || "status").toUpperCase()),
        h("div", { className: "mt-1 text-xs opacity-90" }, governance.message)
      ) : null,
      h("div", { className: "grid grid-cols-2 gap-3 sm:grid-cols-4" },
        h(Metric, { label: "Evaluated", value: learning.evaluated_outcomes || 0 }),
        h(Metric, { label: "Pending", value: learning.pending_outcomes || 0 }),
        h(Metric, { label: "Due Now", value: learning.matured_pending_outcomes || 0, tone: Number(learning.matured_pending_outcomes || 0) ? "text-pulse-amber" : "text-pulse-green" }),
        h(Metric, { label: "Evaluated Now", value: learning.evaluated_now || 0 })
      ),
      h("div", { className: "mt-4 grid gap-3 lg:grid-cols-[1fr_1.3fr]" },
        h("div", { className: "rounded-lg border border-pulse-line bg-pulse-panel p-3" },
          h("div", { className: "font-mono text-[10px] uppercase tracking-[0.2em] text-pulse-cyan" }, "Adaptive calibration"),
          h("div", { className: "mt-2 grid grid-cols-2 gap-2" },
            h(Metric, { label: "1D Samples", value: global1d.samples || 0 }),
            h(Metric, { label: "1D Hit Rate", value: plainPct(global1d.directional_hit_rate_pct), tone: Number(global1d.directional_hit_rate_pct || 0) >= 55 ? "text-pulse-green" : "text-pulse-amber" }),
            h(Metric, { label: "Mean Error", value: plainPct(global1d.mean_error_pct) }),
            h(Metric, { label: "MAE", value: plainPct(global1d.mean_absolute_error_pct) })
          ),
          h("p", { className: "mt-2 text-xs text-pulse-muted" },
            calibration.enabled === false ? "Learning is disabled." :
            global1d.eligible ? "Calibration is active for future prediction runs." :
            "Calibration is warming up until enough outcomes mature."
          )
        ),
        h("div", { className: "rounded-lg border border-pulse-line bg-pulse-panel p-3" },
          h("div", { className: "font-mono text-[10px] uppercase tracking-[0.2em] text-pulse-cyan" }, "Learned factor signal"),
          factorRows.length ? h("div", { className: "mt-2 grid gap-2 sm:grid-cols-2" },
            factorRows.map(([name, data]) => h("div", { key: name, className: "rounded-md border border-pulse-line/70 bg-pulse-card/70 p-2" },
              h("div", { className: "flex items-center justify-between gap-2" },
                h("span", { className: "font-mono text-xs uppercase text-pulse-ink" }, name.replace(/_/g, " ")),
                h("span", { className: cx("font-mono text-xs", Number(data.correlation || 0) >= 0 ? "text-pulse-green" : "text-pulse-red") }, Number(data.correlation || 0).toFixed(3))
              ),
              h("div", { className: "mt-1 text-xs text-pulse-muted" }, `${data.samples || 0} samples · ${data.direction || "weak"}`)
            ))
          ) : h("p", { className: "mt-2 text-sm text-pulse-muted" }, "Factor learning will appear once enough evaluated predictions include factor scores.")
        )
      ),
      governance.gates && governance.gates.length ? h("div", { className: "mt-3 grid gap-2 sm:grid-cols-3" },
        governance.gates.map((gate, i) => h("div", {
          key: i,
          className: cx(
            "rounded-md border p-2 text-xs",
            gate.passed ? "border-pulse-green/30 bg-pulse-green/10" : "border-pulse-amber/30 bg-pulse-amber/10"
          )
        },
          h("div", { className: "font-mono uppercase text-pulse-ink" }, gate.name),
          h("div", { className: "mt-1 text-pulse-muted" }, `${gate.value ?? "—"} / ${gate.target ?? "—"}`)
        ))
      ) : null,
      calibration.recommendations && calibration.recommendations.length ? h("div", { className: "mt-3 rounded-lg border border-pulse-line bg-pulse-panel p-3" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.2em] text-pulse-cyan" }, "Model notes"),
        h("ul", { className: "mt-2 space-y-1 text-xs text-pulse-muted" },
          calibration.recommendations.slice(0, 4).map((item, i) => h("li", { key: i }, item))
        )
      ) : null,
      h("div", { className: "mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-3" },
        horizons.length ? horizons.map((row) => h("div", {
          key: row.horizon,
          className: "rounded-lg border border-pulse-line bg-pulse-panel p-3"
        },
          h("div", { className: "mb-2 flex items-center justify-between gap-2" },
            h("span", { className: "font-mono text-xs uppercase text-pulse-cyan" }, row.horizon),
            h("span", { className: cx("font-mono text-sm", Number(row.directional_hit_rate_pct || 0) >= 55 ? "text-pulse-green" : "text-pulse-amber") }, plainPct(row.directional_hit_rate_pct))
          ),
          h("div", { className: "grid grid-cols-2 gap-2 text-xs text-pulse-muted" },
            h("div", null, "MAE ", h("span", { className: "font-mono text-pulse-ink" }, plainPct(row.mean_absolute_error_pct))),
            h("div", null, "Done ", h("span", { className: "font-mono text-pulse-ink" }, row.evaluated || 0)),
            h("div", null, "Forecast ", h("span", { className: "font-mono text-pulse-ink" }, plainPct(row.avg_forecast_pct))),
            h("div", null, "Actual ", h("span", { className: "font-mono text-pulse-ink" }, plainPct(row.avg_realised_pct)))
          )
        )) : h("div", { className: "rounded-lg border border-dashed border-pulse-line p-3 text-sm text-pulse-muted" }, "Generate predictions and let horizons mature to build learning data.")
      )
    );
  }

  function PredictionRow({ p, onOpen }) {
    const variance = p.actual_pct != null && p.predicted_pct != null ? p.actual_pct - p.predicted_pct : null;
    const correct = p.actual_pct != null && p.predicted_pct != null && Math.sign(p.actual_pct) === Math.sign(p.predicted_pct);
    return h("tr", { className: "border-t border-pulse-line/70" },
      h("td", { className: "px-3 py-3 text-xs text-pulse-muted" }, p.date || "—"),
      h("td", { className: "px-3 py-3" }, h("strong", { className: "font-mono text-pulse-cyan" }, p.ticker), h("div", { className: "text-xs text-pulse-muted" }, p.name || "")),
      h("td", { className: "px-3 py-3 font-mono" }, p.current_price != null ? "$" + Number(p.current_price).toFixed(2) : "—"),
      h("td", { className: cx("px-3 py-3 font-mono", scoreTone(p.score)) }, p.score == null ? "—" : `${p.score}/100`),
      h("td", { className: "px-3 py-3" }, h(FactorCluster, { scores: p.factor_scores || {} })),
      h("td", { className: cx("px-3 py-3 font-mono", p.dcf && p.dcf.margin_of_safety_pct != null ? deltaTone(p.dcf.margin_of_safety_pct) : "") }, p.dcf && p.dcf.margin_of_safety_pct != null ? fmtPct(p.dcf.margin_of_safety_pct, 0) : "—"),
      h("td", { className: "px-3 py-3 font-mono" }, fmtPct(p.predicted_3m_pct, 1)),
      h("td", { className: "px-3 py-3 font-mono" }, fmtPct(p.predicted_6m_pct, 1)),
      h("td", { className: "px-3 py-3 font-mono" }, fmtPct(p.predicted_12m_pct, 1)),
      h("td", { className: cx("px-3 py-3 font-mono", deltaTone(p.actual_pct)) }, p.actual_pct == null ? "—" : fmtPct(p.actual_pct, 1)),
      h("td", { className: cx("px-3 py-3 font-mono", variance != null ? deltaTone(variance) : "") }, variance == null ? "—" : fmtPct(variance, 1)),
      h("td", { className: "px-3 py-3" },
        p.actual_pct == null
          ? h("span", { className: "text-pulse-muted text-xs" }, "pending")
          : h("span", {
              className: cx(
                "inline-flex h-7 w-7 items-center justify-center rounded-full border text-sm font-bold",
                correct
                  ? "border-pulse-green/40 bg-pulse-green/20 text-pulse-green"
                  : "border-pulse-red/40 bg-pulse-red/20 text-pulse-red"
              ),
              title: correct ? "Prediction direction matched" : "Prediction direction missed"
            }, correct ? "↑" : "↓")
      ),
      h("td", { className: "px-3 py-3" }, h(ConfidencePill, { value: p.confidence, score: p.score })),
      h("td", { className: "px-3 py-3" }, h(Button, { onClick: onOpen, className: "min-h-8 px-2 text-xs" }, "View"))
    );
  }

  function PredictionCard({ p, onOpen }) {
    return h(Card, { className: "overflow-hidden p-4" },
      h("div", { className: "flex items-start justify-between gap-3" },
        h("div", { className: "min-w-0" }, h("div", { className: "font-mono text-xl text-pulse-cyan" }, p.ticker), h("div", { className: "truncate text-sm text-pulse-muted" }, p.name || "—")),
        h("div", { className: cx("font-mono text-xl", scoreTone(p.score)) }, p.score == null ? "—" : Math.round(p.score))
      ),
      h("div", { className: "mt-3" }, h(FactorCluster, { scores: p.factor_scores || {} })),
      h("div", { className: "mt-4 grid grid-cols-2 gap-2 min-[420px]:grid-cols-3" },
        h(Metric, { label: "3M", value: fmtPct(p.predicted_3m_pct, 1) }),
        h(Metric, { label: "6M", value: fmtPct(p.predicted_6m_pct, 1) }),
        h(Metric, { label: "12M", value: fmtPct(p.predicted_12m_pct, 1) })
      ),
      p.dcf && p.dcf.margin_of_safety_pct != null ? h("div", { className: "mt-3" },
        h(Metric, { label: "MoS", value: fmtPct(p.dcf.margin_of_safety_pct, 0), tone: p.dcf.margin_of_safety_pct >= 0 ? "text-pulse-green" : "text-pulse-red" })
      ) : null,
      h(Button, { onClick: onOpen, className: "mt-4 w-full" }, "View thesis")
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Thesis
  // ──────────────────────────────────────────────────────────────

  function Thesis() {
    const [ticker, setTicker] = useState("CEG");
    const [runFresh, setRunFresh] = useState(false);
    const [thesis, setThesis] = useState(null);
    const [history, setHistory] = useState([]);
    const [recon, setRecon] = useState(null);
    const [busy, setBusy] = useState(false);
    const [status, setStatus] = useState("");
    const [compareInput, setCompareInput] = useState("");
    const [compareData, setCompareData] = useState(null);
    const [panel, setPanel] = useState(null);
    const [panelData, setPanelData] = useState(null);

    async function loadLatest(sym) {
      const target = (sym || ticker).trim().toUpperCase();
      if (!target) return;
      setBusy(true); setStatus(`Loading latest thesis for ${target}...`);
      try {
        const data = await api(`/v1/thesis/${encodeURIComponent(target)}/latest`);
        setThesis(data); setTicker(target); setStatus(`Loaded ${target}.`);
        loadHistory(target); reconcile(data);
      } catch (err) { setStatus(err.message || "No thesis found."); } finally { setBusy(false); }
    }

    async function run() {
      const target = ticker.trim().toUpperCase();
      if (!target) return;
      setBusy(true); setStatus(`Starting agent run for ${target}...`);
      try {
        const r = await api("/v1/runs", { method: "POST", body: JSON.stringify({ tickers: [target], run_fresh: runFresh }) });
        pollRun(r.run_id, target);
      } catch (err) { setStatus(err.message || "Could not start run."); setBusy(false); }
    }

    async function pollRun(runId, target) {
      let attempts = 0;
      const timer = setInterval(async () => {
        attempts += 1;
        try {
          const data = await api(`/v1/runs/${encodeURIComponent(runId)}`);
          setStatus(`Run ${data.status || "running"} · completed ${(data.completed || []).length}`);
          if (["complete", "completed", "partial", "failed"].includes(data.status) || attempts > 90) {
            clearInterval(timer); setBusy(false); loadLatest(target);
          }
        } catch (err) { clearInterval(timer); setBusy(false); setStatus(err.message || "Run failed."); }
      }, 2500);
    }

    async function loadHistory(sym) {
      try {
        const data = await api(`/v1/thesis/${encodeURIComponent(sym)}/history?limit=12`);
        setHistory(data.theses || []);
      } catch { setHistory([]); }
    }

    async function loadById(id) {
      setBusy(true);
      try { const data = await api(`/v1/thesis/id/${encodeURIComponent(id)}`); setThesis(data); reconcile(data); }
      catch (err) { setStatus(err.message || "Could not load thesis."); }
      finally { setBusy(false); }
    }

    async function reconcile(t) {
      setRecon(null);
      if (!t || !t.ticker) return;
      try {
        const res = await fetch(`${PICK_SHOVELS_API}/api/reconcile/${encodeURIComponent(t.ticker)}?theme_id=ai-infra`, {
          method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ stock_analysis: t })
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        setRecon(await res.json());
      } catch { setRecon({ unavailable: true }); }
    }

    async function runCompare() {
      const raw = compareInput.trim();
      if (!raw) return;
      setBusy(true); setStatus(`Comparing ${raw}...`);
      try { const data = await api(`/v1/thesis/compare?tickers=${encodeURIComponent(raw)}`); setCompareData(data); setStatus("Compare loaded."); }
      catch (err) { setStatus("Compare failed: " + err.message); }
      finally { setBusy(false); }
    }

    async function openPanel(kind) {
      setPanel(kind); setPanelData(null);
      try {
        if (kind === "ops") setPanelData(await api("/v1/operations/status"));
        if (kind === "health") setPanelData(await api("/v1/agents/health"));
        if (kind === "evaluate") setPanelData(await api("/v1/evaluate/status"));
      } catch (err) { setPanelData({ error: err.message }); }
    }

    return h("div", null,
      h(SectionHead, { title: "Agent Thesis", kicker: "8-agent deep dive", subtitle: "Run fresh agents, view dated cached results, compare tickers, and inspect agent operations.", actions: [
        h(Button, { key: "ops", onClick: () => openPanel("ops") }, "Operations"),
        h(Button, { key: "eval", onClick: () => openPanel("evaluate") }, "Evaluate"),
        h(Button, { key: "health", onClick: () => openPanel("health") }, "Agent Health"),
        h(Button, { key: "refresh", onClick: () => loadLatest(), disabled: busy }, "Refresh latest"),
      ] }),
      h(Card, { className: "mb-4 p-4" },
        h("div", { className: "grid gap-3 sm:grid-cols-[1fr_auto_auto_auto]" },
          h(TextInput, { value: ticker, onChange: e => setTicker(e.target.value.toUpperCase()), onKeyDown: e => { if (e.key === "Enter") loadLatest(); }, placeholder: "Ticker" }),
          h("label", { className: "flex min-h-11 items-center gap-2 rounded-lg border border-pulse-line bg-pulse-panel px-3 text-sm text-pulse-muted" },
            h("input", { type: "checkbox", checked: runFresh, onChange: e => setRunFresh(e.target.checked) }),
            "Run fresh"
          ),
          h(Button, { onClick: () => loadLatest(), disabled: busy }, "View latest"),
          h(Button, { kind: "primary", onClick: run, disabled: busy }, busy ? "Working..." : "Run agents")
        ),
        h("div", { className: "mt-3 grid gap-2 sm:grid-cols-[1fr_auto]" },
          h(TextInput, { value: compareInput, onChange: e => setCompareInput(e.target.value.toUpperCase()), placeholder: "Compare: MSFT,AAPL,NVDA", onKeyDown: e => { if (e.key === "Enter") runCompare(); } }),
          h(Button, { kind: "primary", onClick: runCompare, disabled: busy }, "Compare")
        ),
        h(Status, { message: status, className: "mt-3" })
      ),
      compareData ? h(CompareCard, { data: compareData, onClose: () => setCompareData(null) }) : null,
      thesis ? h(ThesisView, { thesis, history, recon, onHistory: loadById }) : h(Empty, null, "Enter a ticker, load latest, or run agents."),
      panel ? h(SlideOver, { title: panel === "ops" ? "Operations" : panel === "health" ? "Agent Health" : "Evaluation", kicker: "Agent infrastructure", onClose: () => { setPanel(null); setPanelData(null); } },
        panelData == null ? h(Empty, null, "Loading...") :
        panelData.error ? h(Empty, null, panelData.error) :
        h(PanelView, { kind: panel, data: panelData })
      ) : null
    );
  }

  function PanelView({ kind, data }) {
    if (kind === "ops") return h(OperationsPanel, { data });
    if (kind === "health") return h(AgentHealthPanel, { data });
    if (kind === "evaluate") return h(EvaluatePanel, { data });
    return h(Card, { className: "p-4 text-sm text-pulse-muted" }, "No panel renderer.");
  }

  function OperationsPanel({ data }) {
    const th = data && data.thesis_scheduler ? data.thesis_scheduler : {};
    const ev = data && data.evaluation_scheduler ? data.evaluation_scheduler : {};
    const pr = data && data.prediction_scheduler ? data.prediction_scheduler : {};
    const mon = data && data.monitor_scheduler ? data.monitor_scheduler : {};
    const fail = data && data.background_failures ? data.background_failures : {};
    const generated = data && data.generated_at ? data.generated_at : null;
    const [cfg, setCfg] = useState(null);
    const [cfgBusy, setCfgBusy] = useState(false);
    const [cfgStatus, setCfgStatus] = useState("");

    useEffect(() => {
      let alive = true;
      (async () => {
        try {
          const settings = await api("/v1/settings/scheduler");
          if (alive) setCfg(settings || {});
        } catch (err) {
          if (alive) setCfgStatus(`Could not load scheduler settings: ${err.message}`);
        }
      })();
      return () => { alive = false; };
    }, []);

    async function saveSchedulerSettings() {
      if (!cfg) return;
      setCfgBusy(true);
      setCfgStatus("Saving...");
      try {
        await api("/v1/settings/scheduler", {
          method: "PATCH",
          body: JSON.stringify({
            thesis_auto_run_enabled: !!cfg.thesis_auto_run_enabled,
            thesis_auto_run_interval_minutes: Math.max(15, Number(cfg.thesis_auto_run_interval_minutes || 1440)),
            thesis_auto_run_max_tickers: Math.max(1, Math.min(50, Number(cfg.thesis_auto_run_max_tickers || 8))),
            evaluation_auto_run_enabled: !!cfg.evaluation_auto_run_enabled,
            evaluation_auto_run_interval_minutes: Math.max(60, Number(cfg.evaluation_auto_run_interval_minutes || 1440)),
            prediction_auto_run_enabled: !!cfg.prediction_auto_run_enabled,
            prediction_auto_run_interval_minutes: Math.max(5, Number(cfg.prediction_auto_run_interval_minutes || 15)),
            monitor_auto_run_enabled: !!cfg.monitor_auto_run_enabled,
            monitor_auto_run_interval_minutes: Math.max(1, Number(cfg.monitor_auto_run_interval_minutes || 5)),
          }),
        });
        setCfgStatus(`Saved and applied at ${new Date().toLocaleTimeString()}`);
      } catch (err) {
        setCfgStatus(`Error: ${err.message}`);
      } finally {
        setCfgBusy(false);
      }
    }

    return h("div", { className: "grid gap-4" },
      generated ? h(Status, { message: `Updated ${fmtDate(generated)}` }) : null,
      h("div", { className: "grid gap-3 sm:grid-cols-2" },
        h(OpsCard, { title: "Thesis Scheduler", status: th.enabled ? "ENABLED" : "DISABLED", rows: [
          ["Active", th.active ? "yes" : "no"],
          ["Runs", th.runs_started == null ? "-" : String(th.runs_started)],
          ["Last Run", th.last_run ? fmtDate(th.last_run) : "never"],
          ["Last Error", th.last_error || "none"],
        ] }),
        h(OpsCard, { title: "Evaluation Scheduler", status: ev.enabled ? "ENABLED" : "DISABLED", rows: [
          ["Active", ev.active ? "yes" : "no"],
          ["Runs", ev.runs_started == null ? "-" : String(ev.runs_started)],
          ["Last Run", ev.last_run ? fmtDate(ev.last_run) : "never"],
          ["Last Error", ev.last_error || "none"],
        ] }),
        h(OpsCard, { title: "Prediction Scheduler", status: pr.enabled ? "ENABLED" : "DISABLED", rows: [
          ["Active", pr.active ? "yes" : "no"],
          ["Runs", pr.runs_started == null ? "-" : String(pr.runs_started)],
          ["Last Run", pr.last_run ? fmtDate(pr.last_run) : "never"],
          ["Last Error", pr.last_error || "none"],
        ] }),
        h(OpsCard, { title: "Monitor Scheduler", status: mon.enabled ? "ENABLED" : "DISABLED", rows: [
          ["Active", mon.active ? "yes" : "no"],
          ["Runs", mon.runs_started == null ? "-" : String(mon.runs_started)],
          ["Last Run", mon.last_run ? fmtDate(mon.last_run) : "never"],
          ["Last Error", mon.last_error || "none"],
        ] }),
      ),
      h(Card, { className: "p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.2em] text-pulse-dim" }, "Background Failures"),
        h("div", { className: "mt-3 grid gap-2 sm:grid-cols-2" },
          ["thesis", "evaluation", "prediction", "monitor"].map((k) => h(Metric, {
            key: k,
            label: k,
            value: fail[k] && fail[k].count != null ? String(fail[k].count) : "0",
            hint: fail[k] && fail[k].last_error ? String(fail[k].last_error) : "no recent failures",
            tone: fail[k] && fail[k].count > 0 ? "text-pulse-red" : "text-pulse-green",
          }))
        )
      ),
      h(Card, { className: "p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.2em] text-pulse-dim" }, "Scheduler Settings"),
        cfg ? h("div", { className: "mt-3 grid gap-3 sm:grid-cols-2" },
          h("div", { className: "grid gap-2 rounded-lg border border-pulse-line bg-pulse-panel p-3" },
            h("div", { className: "text-sm font-semibold" }, "Thesis Auto-Run"),
            h("label", { className: "flex items-center justify-between text-sm" },
              h("span", { className: "text-pulse-muted" }, "Enabled"),
              h("input", {
                type: "checkbox",
                checked: !!cfg.thesis_auto_run_enabled,
                onChange: (e) => setCfg(prev => Object.assign({}, prev, { thesis_auto_run_enabled: e.target.checked }))
              })
            ),
            h("label", { className: "flex items-center justify-between text-sm gap-2" },
              h("span", { className: "text-pulse-muted" }, "Interval (minutes)"),
              h(TextInput, {
                type: "number",
                min: 15,
                step: 60,
                className: "max-w-[120px]",
                value: cfg.thesis_auto_run_interval_minutes ?? 1440,
                onChange: (e) => setCfg(prev => Object.assign({}, prev, { thesis_auto_run_interval_minutes: e.target.value }))
              })
            ),
            h("label", { className: "flex items-center justify-between text-sm gap-2" },
              h("span", { className: "text-pulse-muted" }, "Max tickers per run"),
              h(TextInput, {
                type: "number",
                min: 1,
                max: 50,
                className: "max-w-[120px]",
                value: cfg.thesis_auto_run_max_tickers ?? 8,
                onChange: (e) => setCfg(prev => Object.assign({}, prev, { thesis_auto_run_max_tickers: e.target.value }))
              })
            )
          ),
          h("div", { className: "grid gap-2 rounded-lg border border-pulse-line bg-pulse-panel p-3" },
            h("div", { className: "text-sm font-semibold" }, "Evaluation Auto-Run"),
            h("label", { className: "flex items-center justify-between text-sm" },
              h("span", { className: "text-pulse-muted" }, "Enabled"),
              h("input", {
                type: "checkbox",
                checked: !!cfg.evaluation_auto_run_enabled,
                onChange: (e) => setCfg(prev => Object.assign({}, prev, { evaluation_auto_run_enabled: e.target.checked }))
              })
            ),
            h("label", { className: "flex items-center justify-between text-sm gap-2" },
              h("span", { className: "text-pulse-muted" }, "Interval (minutes)"),
              h(TextInput, {
                type: "number",
                min: 60,
                step: 60,
                className: "max-w-[120px]",
                value: cfg.evaluation_auto_run_interval_minutes ?? 1440,
                onChange: (e) => setCfg(prev => Object.assign({}, prev, { evaluation_auto_run_interval_minutes: e.target.value }))
              })
            )
          ),
          h("div", { className: "grid gap-2 rounded-lg border border-pulse-line bg-pulse-panel p-3" },
            h("div", { className: "text-sm font-semibold" }, "Prediction Auto-Run"),
            h("label", { className: "flex items-center justify-between text-sm" },
              h("span", { className: "text-pulse-muted" }, "Enabled"),
              h("input", {
                type: "checkbox",
                checked: !!cfg.prediction_auto_run_enabled,
                onChange: (e) => setCfg(prev => Object.assign({}, prev, { prediction_auto_run_enabled: e.target.checked }))
              })
            ),
            h("label", { className: "flex items-center justify-between text-sm gap-2" },
              h("span", { className: "text-pulse-muted" }, "Interval (minutes)"),
              h(TextInput, {
                type: "number",
                min: 5,
                step: 5,
                className: "max-w-[120px]",
                value: cfg.prediction_auto_run_interval_minutes ?? 15,
                onChange: (e) => setCfg(prev => Object.assign({}, prev, { prediction_auto_run_interval_minutes: e.target.value }))
              })
            )
          ),
          h("div", { className: "grid gap-2 rounded-lg border border-pulse-line bg-pulse-panel p-3" },
            h("div", { className: "text-sm font-semibold" }, "Monitor Auto-Run"),
            h("label", { className: "flex items-center justify-between text-sm" },
              h("span", { className: "text-pulse-muted" }, "Enabled"),
              h("input", {
                type: "checkbox",
                checked: !!cfg.monitor_auto_run_enabled,
                onChange: (e) => setCfg(prev => Object.assign({}, prev, { monitor_auto_run_enabled: e.target.checked }))
              })
            ),
            h("label", { className: "flex items-center justify-between text-sm gap-2" },
              h("span", { className: "text-pulse-muted" }, "Interval (minutes)"),
              h(TextInput, {
                type: "number",
                min: 1,
                step: 1,
                className: "max-w-[120px]",
                value: cfg.monitor_auto_run_interval_minutes ?? 5,
                onChange: (e) => setCfg(prev => Object.assign({}, prev, { monitor_auto_run_interval_minutes: e.target.value }))
              })
            )
          )
        ) : h("p", { className: "mt-2 text-sm text-pulse-muted" }, "Loading scheduler settings..."),
        h("div", { className: "mt-3 flex flex-wrap items-center gap-3" },
          h(Button, { kind: "primary", onClick: saveSchedulerSettings, disabled: cfgBusy || !cfg }, cfgBusy ? "Saving..." : "Save & Apply"),
          cfgStatus ? h("span", { className: "text-xs text-pulse-muted" }, cfgStatus) : null
        )
      )
    );
  }

  function OpsCard({ title, status, rows }) {
    return h(Card, { className: "p-4" },
      h("div", { className: "flex items-center justify-between gap-2" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.2em] text-pulse-dim" }, title),
        h(Pill, { className: status === "ENABLED" ? "border-pulse-green/40 bg-pulse-green/10 text-pulse-green" : "border-pulse-line bg-pulse-panel text-pulse-dim" }, status)
      ),
      h("div", { className: "mt-3 grid gap-2" },
        rows.map(([k, v]) => h("div", { key: k, className: "flex items-center justify-between text-sm" },
          h("span", { className: "text-pulse-muted" }, k),
          h("span", { className: "font-mono text-pulse-ink text-right" }, v)
        ))
      )
    );
  }

  function AgentHealthPanel({ data }) {
    const generated = data && data.generated_at ? data.generated_at : null;
    const agents = data && data.agents
      ? (Array.isArray(data.agents) ? data.agents : Object.values(data.agents))
      : [];
    return h("div", { className: "grid gap-4" },
      generated ? h(Status, { message: `Updated ${fmtDate(generated)}` }) : null,
      agents.length ? h("div", { className: "grid gap-3" },
        agents.map((a, i) => {
          const stale = !!a.stale;
          const tone = stale ? "text-pulse-red" : "text-pulse-green";
          return h(Card, { key: a.agent_id || i, className: "p-4" },
            h("div", { className: "flex items-start justify-between gap-3" },
              h("div", null,
                h("div", { className: "font-semibold text-sm" }, a.agent_id || "unknown agent"),
                h("div", { className: "text-xs text-pulse-muted mt-1" }, a.last_run ? `Last run ${fmtDate(a.last_run)}` : "No run recorded")
              ),
              h(Pill, { className: stale ? "border-pulse-red/40 bg-pulse-red/10 text-pulse-red" : "border-pulse-green/40 bg-pulse-green/10 text-pulse-green" }, stale ? "STALE" : "FRESH")
            ),
            h("div", { className: `mt-3 text-xs ${tone}` }, a.last_error ? `Error: ${a.last_error}` : "No errors recorded")
          );
        })
      ) : h(Empty, null, "No agent health records returned.")
    );
  }

  function EvaluatePanel({ data }) {
    const summary = data && data.summary ? data.summary : {};
    const backtest = data && data.backtest ? data.backtest : {};
    const outcomes = data && data.outcomes ? data.outcomes : {};
    const scheduler = data && data.scheduler ? data.scheduler : {};
    const rows = Object.entries(summary);
    const cals = Object.entries(backtest.calibration || {});
    return h("div", { className: "grid gap-4" },
      h(Card, { className: "p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.2em] text-pulse-dim" }, "Evaluation Status"),
        h("div", { className: "mt-3 grid gap-2 sm:grid-cols-3" },
          h(Metric, { label: "Scheduler", value: scheduler.enabled ? "ENABLED" : "DISABLED", tone: scheduler.enabled ? "text-pulse-green" : "text-pulse-muted" }),
          h(Metric, { label: "Pending", value: outcomes.pending == null ? "0" : String(outcomes.pending) }),
          h(Metric, { label: "Matured Pending", value: outcomes.matured_pending == null ? "0" : String(outcomes.matured_pending) })
        )
      ),
      h(Card, { className: "p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.2em] text-pulse-dim" }, "Evaluation Summary"),
        rows.length ? h("div", { className: "mt-3 grid gap-2 sm:grid-cols-3" },
          rows.map(([horizon, r]) => h(Metric, {
            key: horizon,
            label: horizon,
            value: `${Math.round(Number((r && r.directional_hit_rate) || 0) * 100)}% hit`,
            hint: `MAE ${Number((r && r.mean_absolute_error) || 0).toFixed(2)} (${(r && r.total) || 0} samples)`,
          }))
        ) : h("p", { className: "mt-2 text-sm text-pulse-muted" }, "No matured forecasts available yet.")
      ),
      h(Card, { className: "p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.2em] text-pulse-dim" }, "Calibration"),
        cals.length ? h("div", { className: "mt-3 overflow-x-auto" },
          h("table", { className: "min-w-full text-sm" },
            h("thead", { className: "font-mono text-[10px] uppercase tracking-[0.16em] text-pulse-dim" },
              h("tr", null, ["Bucket", "Hit Rate", "Total"].map((x) => h("th", { key: x, className: "px-2 py-2 text-left" }, x)))
            ),
            h("tbody", null,
              cals.map(([bucket, row]) => h("tr", { key: bucket, className: "border-t border-pulse-line/60" },
                h("td", { className: "px-2 py-2 text-pulse-muted" }, bucket),
                h("td", { className: "px-2 py-2 font-mono" }, `${Math.round(Number((row && row.hit_rate) || 0) * 100)}%`),
                h("td", { className: "px-2 py-2 font-mono" }, String((row && row.total) || 0))
              ))
            )
          )
        ) : h("p", { className: "mt-2 text-sm text-pulse-muted" }, "Calibration metrics are not available yet.")
      )
    );
  }

  function CompareCard({ data, onClose }) {
    const items = Array.isArray(data) ? data : (data.comparison || data.results || []);
    async function exportComparePdf() {
      const tickers = items.map(i => i.ticker).filter(Boolean).join(",");
      if (!tickers) return;
      const res = await fetch(`/v1/thesis/compare/export.pdf?tickers=${encodeURIComponent(tickers)}`, {
        headers: token() ? { Authorization: `Bearer ${token()}` } : {},
      });
      if (!res.ok) throw new Error(`Export failed (HTTP ${res.status})`);
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `compare_${tickers.replace(/,/g, "_")}.pdf`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    }
    return h(Card, { className: "mb-4 p-4" },
      h("div", { className: "flex items-start justify-between gap-3" },
        h("div", null,
          h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, "Compare"),
          h("h3", { className: "mt-1 text-lg font-semibold" }, "Side-by-side")
        ),
        h("div", { className: "flex gap-2" },
          h(Button, { onClick: exportComparePdf, className: "min-h-9 px-3 text-xs" }, "Export PDF"),
          h(Button, { onClick: onClose, className: "min-h-9 px-3 text-xs" }, "Close")
        )
      ),
      Array.isArray(items) && items.length ? h("div", { className: "mt-3 overflow-x-auto" },
        h("table", { className: "min-w-full text-sm" },
          h("thead", { className: "bg-pulse-panel font-mono text-[10px] uppercase tracking-[0.16em] text-pulse-dim" },
            h("tr", null, ["Ticker", "Score", "Risk", "Evidence", "12M Base"].map(x => h("th", { key: x, className: "px-3 py-2 text-left" }, x)))
          ),
          h("tbody", null, items.map((t, i) => h("tr", { key: i, className: "border-t border-pulse-line/60" },
            h("td", { className: "px-3 py-2" }, h("strong", { className: "font-mono text-pulse-cyan" }, t.ticker)),
            h("td", { className: cx("px-3 py-2 font-mono", scoreTone(t.composite_score)) }, t.composite_score == null ? "—" : Number(t.composite_score).toFixed(1)),
            h("td", { className: "px-3 py-2" }, t.risk_rating || "—"),
            h("td", { className: "px-3 py-2" }, t.evidence_quality || "—"),
            h("td", { className: cx("px-3 py-2 font-mono", deltaTone(t.forecast_12m && t.forecast_12m.base_return_pct)) }, t.forecast_12m ? fmtPct(t.forecast_12m.base_return_pct, 1) : "—")
          )))
        )
      ) : h(Empty, null, "No comparison data.")
    );
  }

  function ThesisView({ thesis, history, recon, onHistory }) {
    const score = Number(thesis.composite_score || 0);
    const direction = score >= 60 ? "BULLISH" : score <= 40 ? "BEARISH" : "NEUTRAL";
    const forecast = thesis.forecast || {};
    async function exportPdf(e) {
      e.preventDefault();
      try {
        const res = await fetch(`/v1/thesis/${encodeURIComponent(thesis.ticker)}/export.pdf`, {
          headers: token() ? { Authorization: `Bearer ${token()}` } : {},
        });
        if (!res.ok) throw new Error(`Export failed (HTTP ${res.status})`);
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `thesis_${thesis.ticker}.pdf`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
      } catch (_) {
        window.open(`/v1/thesis/${encodeURIComponent(thesis.ticker)}/export.pdf`, "_blank");
      }
    }

    return h("div", { className: "grid gap-4" },
      h(Card, { className: "p-4" },
        h("div", { className: "flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between" },
          h("div", null,
            h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, "Deep dive"),
            h("h3", { className: "mt-1 text-2xl font-semibold" }, thesis.ticker, " Analysis"),
            h("p", { className: "mt-1 text-sm text-pulse-muted" }, fmtDate(thesis.generated_at), thesis.thesis_id ? ` · thesis ${String(thesis.thesis_id).slice(0, 8)}` : "")
          ),
          h("a", { href: "#", onClick: exportPdf, className: "inline-flex min-h-10 items-center justify-center rounded-lg border border-pulse-line px-3 text-sm text-pulse-ink" }, "Export PDF")
        )
      ),
      h(Reconciliation, { data: recon }),
      h("div", { className: "grid gap-3 sm:grid-cols-[180px_1fr]" },
        h(Card, { className: "p-4" },
          h("div", { className: "font-mono text-[10px] uppercase tracking-[0.2em] text-pulse-dim" }, "Composite"),
          h("div", { className: cx("mt-2 font-mono text-5xl font-light", scoreTone(score)) }, score.toFixed(1), h("span", { className: "text-lg text-pulse-dim" }, "/100")),
          h(Pill, { className: cx("mt-4", score >= 60 ? "border-pulse-green/30 bg-pulse-green/10 text-pulse-green" : score <= 40 ? "border-pulse-red/30 bg-pulse-red/10 text-pulse-red" : "border-pulse-amber/30 bg-pulse-amber/10 text-pulse-amber") }, direction)
        ),
        h(Card, { className: "p-4" },
          h("div", { className: "grid grid-cols-2 gap-2 sm:grid-cols-4" },
            h(Metric, { label: "Price", value: fmtUsd(thesis.current_price) }),
            h(Metric, { label: "Risk", value: thesis.risk_rating || "—" }),
            h(Metric, { label: "Evidence", value: thesis.evidence_quality || "—" }),
            h(Metric, { label: "12M base", value: forecast["12m"] ? fmtPct(forecast["12m"].base_return_pct, 1) : "—" })
          )
        )
      ),
      history && history.length ? h(Card, { className: "p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, "Dated results · last 12 months"),
        h("div", { className: "mt-3 flex gap-2 overflow-x-auto pb-1 scrollbar-none" },
          history.map(row => h("button", { key: row.thesis_id, onClick: () => onHistory(row.thesis_id), className: "shrink-0 rounded-lg border border-pulse-line bg-pulse-panel px-3 py-2 text-left" },
            h("div", { className: cx("font-mono text-sm", scoreTone(row.composite_score)) }, Number(row.composite_score || 0).toFixed(1)),
            h("div", { className: "text-[11px] text-pulse-muted" }, row.generated_at ? new Date(row.generated_at).toLocaleDateString() : "—")
          ))
        )
      ) : null,
      h("div", { className: "grid gap-3 md:grid-cols-3" },
        ["3m", "6m", "12m"].map(key => h(ForecastCard, { key, label: key.toUpperCase(), forecast: forecast[key], weighted: thesis.weighted_scores && thesis.weighted_scores[key] }))
      ),
      h("div", { className: "grid gap-3 lg:grid-cols-3" },
        ["bull", "base", "bear"].map(key => h(Card, { key, className: "p-4" },
          h("div", { className: cx("font-mono text-[10px] uppercase tracking-[0.2em]", key === "bull" ? "text-pulse-green" : key === "bear" ? "text-pulse-red" : "text-pulse-amber") }, key, " case"),
          h("p", { className: "mt-3 text-sm leading-relaxed text-pulse-muted" }, (thesis.narrative || {})[key] || "No narrative.")
        ))
      ),
      h(Card, { className: "p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, "Agent scores"),
        h("div", { className: "mt-3 grid gap-3" },
          Object.entries(thesis.agent_scores || {}).map(([agent, value]) => h("div", { key: agent, className: "grid grid-cols-[112px_1fr_40px] items-center gap-3 text-sm" },
            h("span", { className: "truncate text-pulse-muted" }, agent.replace("agent.", "").replace(/_/g, " ")),
            h(ProgressBar, { value }),
            h("strong", { className: cx("font-mono text-right", scoreTone(value)) }, Number(value || 0).toFixed(0))
          ))
        )
      ),
      h("div", { className: "grid gap-3 md:grid-cols-2" },
        h(ListCard, { title: "Drivers", items: thesis.drivers, tone: "text-pulse-green" }),
        h(ListCard, { title: "Risks", items: thesis.risks, tone: "text-pulse-red" })
      )
    );
  }

  function ForecastCard({ label, forecast, weighted }) {
    return h(Card, { className: "p-4" },
      h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, label),
      forecast ? h("div", { className: "mt-3 grid gap-2" },
        h("div", { className: "flex justify-between" }, h("span", { className: "text-pulse-muted" }, "Base"), h("strong", { className: cx("font-mono", Number(forecast.base_return_pct) >= 0 ? "text-pulse-green" : "text-pulse-red") }, fmtPct(forecast.base_return_pct, 1))),
        h("div", { className: "flex justify-between" }, h("span", { className: "text-pulse-muted" }, "Bull"), h("strong", { className: "font-mono text-pulse-green" }, fmtPct(forecast.bull_return_pct, 1))),
        h("div", { className: "flex justify-between" }, h("span", { className: "text-pulse-muted" }, "Bear"), h("strong", { className: "font-mono text-pulse-red" }, fmtPct(forecast.bear_return_pct, 1))),
        h("div", { className: "pt-2 text-xs text-pulse-dim" }, "Confidence ", forecast.confidence == null ? "—" : `${Math.round(Number(forecast.confidence) * 100)}%`, " · score ", weighted == null ? "—" : Number(weighted).toFixed(1))
      ) : h("p", { className: "mt-3 text-sm text-pulse-muted" }, "No forecast.")
    );
  }

  function ListCard({ title, items, tone }) {
    const list = Array.isArray(items) ? items.filter(Boolean) : [];
    return h(Card, { className: "p-4" },
      h("div", { className: cx("font-mono text-[10px] uppercase tracking-[0.24em]", tone) }, title),
      list.length ? h("ul", { className: "mt-3 grid gap-2 text-sm leading-relaxed text-pulse-muted" }, list.map((item, i) => h("li", { key: i }, item))) : h("p", { className: "mt-3 text-sm text-pulse-muted" }, "None recorded.")
    );
  }

  function Reconciliation({ data }) {
    if (!data) return h(Card, { className: "p-4 text-sm text-pulse-muted" }, "Checking Picks-shovels theme signal...");
    if (data.unavailable) return h(Card, { className: "border-pulse-cyan/40 p-4 text-sm text-pulse-muted" }, "Picks-shovels reconciliation is unavailable. Thesis remains valid, but theme context was not loaded.");
    const summary = data.summary || {};
    const conflicts = data.conflicts || [];
    const color = summary.color === "red" ? "border-pulse-red/50 bg-pulse-red/10" : summary.color === "yellow" ? "border-pulse-amber/50 bg-pulse-amber/10" : summary.color === "green" ? "border-pulse-green/50 bg-pulse-green/10" : "border-pulse-cyan/50 bg-pulse-cyan/10";
    return h(Card, { className: cx("p-4", color) },
      h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, "Signal reconciliation"),
      h("h3", { className: "mt-2 font-semibold" }, summary.message || "Theme signal checked"),
      h("p", { className: "mt-1 text-sm text-pulse-muted" }, data.theme_data ? `Tier ${data.theme_data.tier ?? "—"} · ${data.theme_data.exposure_pct ?? "—"}% exposure · using displayed Stock Picker thesis` : ""),
      conflicts.length ? h("div", { className: "mt-4 grid gap-3" }, conflicts.map((c, i) => h("div", { key: i, className: "rounded-lg border border-pulse-line bg-pulse-panel/80 p-3" },
        h("div", { className: "font-semibold" }, c.title),
        h("div", { className: "mt-3 grid gap-2 sm:grid-cols-2" }, h(Metric, { label: "Stock view", value: c.stock_view }), h(Metric, { label: "Theme view", value: c.theme_view })),
        h("p", { className: "mt-3 text-sm text-pulse-muted" }, c.explanation),
        h("div", { className: "mt-3 grid gap-3 sm:grid-cols-2" },
          h("div", null, h("div", { className: "font-mono text-[10px] uppercase tracking-wide text-pulse-dim" }, "What it means"), h("ul", { className: "mt-2 list-disc pl-4 text-sm text-pulse-muted" }, (c.what_it_means || []).map((x, j) => h("li", { key: j }, x)))),
          h("div", null, h("div", { className: "font-mono text-[10px] uppercase tracking-wide text-pulse-dim" }, "What to do"), h("ul", { className: "mt-2 list-disc pl-4 text-sm text-pulse-muted" }, (c.what_to_do || []).map((x, j) => h("li", { key: j }, x))))
        )
      ))) : h("p", { className: "mt-3 text-sm text-pulse-muted" }, "No conflict detected between Stock Picker and Picks-shovels theme signals.")
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Backtest + P&L Simulator
  // ──────────────────────────────────────────────────────────────

  function Backtest() {
    const [bt, setBt] = useState(null);
    const [sim, setSim] = useState(null);
    const [btBusy, setBtBusy] = useState(false);
    const [simBusy, setSimBusy] = useState(false);
    const [btStatus, setBtStatus] = useState("");
    const [simStatus, setSimStatus] = useState("");

    async function runBacktest() {
      setBtBusy(true); setBtStatus("Running backtest... fetching 4 weeks of historical data (30-60s)..."); setBt(null);
      try { const data = await api("/api/predictions/backtest"); setBt(data); setBtStatus(""); }
      catch (err) { setBtStatus("Error: " + err.message); }
      finally { setBtBusy(false); }
    }

    async function runSim() {
      setSimBusy(true); setSimStatus("Running Monte Carlo simulator (1,000 paths, 30-60s)..."); setSim(null);
      try { const data = await api("/api/predictions/simulate"); setSim(data); setSimStatus(""); }
      catch (err) { setSimStatus("Error: " + err.message); }
      finally { setSimBusy(false); }
    }

    return h("div", null,
      h(SectionHead, { title: "4-Week Backtest", kicker: "Signal validation", subtitle: "Replays the sentiment scoring model against 4 weeks of historical data to measure directional accuracy.", actions: [h(Button, { key: "run", kind: "primary", onClick: runBacktest, disabled: btBusy }, btBusy ? "Running..." : "▶ Run Backtest")] }),
      h(Card, { className: "mb-4 p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, "How to read"),
        h("div", { className: "mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-3 text-sm text-pulse-muted" },
          h("div", null, h("strong", { className: "text-pulse-ink" }, "🎯 Accuracy %"), " — % of days the model called the direction right. 50% = coin flip, >60% = edge."),
          h("div", null, h("strong", { className: "text-pulse-ink" }, "📏 Variance ±%"), " — Average distance between predicted and actual magnitude. Lower is better."),
          h("div", null, h("strong", { className: "text-pulse-ink" }, "🔴 Red + small variance"), " — Direction inverted. Stock behaves counter-cyclically — flip the signal."),
          h("div", null, h("strong", { className: "text-pulse-ink" }, "🟢 Green + small variance"), " — Ideal: right direction, accurate magnitude."),
          h("div", null, h("strong", { className: "text-pulse-ink" }, "🟡 Green + large variance"), " — Right direction, off magnitude. Trust direction not size."),
          h("div", null, h("strong", { className: "text-pulse-ink" }, "⚪ 50-60%"), " — Marginal, close to random. Watch for improvement.")
        )
      ),
      h(Status, { message: btStatus, className: "mb-3" }),
      bt && bt.summary ? h(BacktestSummary, { data: bt }) : null,
      h("div", { className: "mt-6 border-t border-pulse-line pt-6" },
        h(SectionHead, { title: "P&L Simulator", kicker: "Monte Carlo", subtitle: "Replays buy signals through position sizing and runs 1,000 paths forward to project a 12-month range.", actions: [h(Button, { key: "sim", kind: "primary", onClick: runSim, disabled: simBusy }, simBusy ? "Running..." : "▶ Run Simulator")] }),
        h(Status, { message: simStatus, className: "mb-3" }),
        sim && sim.stats ? h(SimulatorResults, { data: sim }) : null
      )
    );
  }

  function BacktestSummary({ data }) {
    const s = data.summary;
    const tickerStats = data.by_ticker ? Object.entries(data.by_ticker).sort((a, b) => b[1].accuracy_pct - a[1].accuracy_pct) : [];
    return h("div", { className: "grid gap-4" },
      h("div", { className: "grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-6" },
        h(Metric, { label: "Accuracy", value: s.accuracy_pct + "%", tone: s.accuracy_pct >= 60 ? "text-pulse-green" : s.accuracy_pct >= 50 ? "text-pulse-amber" : "text-pulse-red" }),
        h(Metric, { label: "Days Tested", value: s.total }),
        h(Metric, { label: "Correct", value: s.correct }),
        h(Metric, { label: "Avg Variance", value: "±" + s.avg_abs_variance + "%" }),
        h(Metric, { label: "Avg Predicted", value: fmtPct(s.avg_predicted, 1) }),
        h(Metric, { label: "Avg Actual", value: fmtPct(s.avg_actual, 1) })
      ),
      tickerStats.length ? h(Card, { className: "p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, "By ticker"),
        h("div", { className: "mt-3 grid grid-cols-2 gap-2 md:grid-cols-4 lg:grid-cols-6" },
          tickerStats.map(([t, st]) => h("div", { key: t, className: "rounded-lg border border-pulse-line bg-pulse-panel p-3" },
            h("div", { className: "font-mono text-pulse-cyan" }, t),
            h("div", { className: cx("font-mono text-lg", st.accuracy_pct >= 60 ? "text-pulse-green" : st.accuracy_pct >= 50 ? "text-pulse-amber" : "text-pulse-red") }, st.accuracy_pct + "%"),
            h("div", { className: "text-xs text-pulse-muted" }, `${st.correct}/${st.total} · ±${st.avg_abs_variance}%`)
          ))
        )
      ) : null,
      Array.isArray(data.results) && data.results.length ? h("div", { className: "overflow-x-auto rounded-xl border border-pulse-line bg-pulse-card" },
        h("table", { className: "min-w-full text-sm" },
          h("thead", { className: "bg-pulse-panel font-mono text-[10px] uppercase tracking-[0.16em] text-pulse-dim" },
            h("tr", null, ["Date","Ticker","VIX","S&P 5d","Sentiment","Fund Adj","Predicted","Actual","Variance","Result"].map(x => h("th", { key: x, className: "px-3 py-2 text-left" }, x)))
          ),
          h("tbody", null, data.results.slice(0, 200).map((r, i) => h("tr", { key: i, className: "border-t border-pulse-line/60" },
            h("td", { className: "px-3 py-2 text-xs text-pulse-muted" }, r.date),
            h("td", { className: "px-3 py-2" }, h("strong", { className: "font-mono text-pulse-cyan" }, r.ticker)),
            h("td", { className: "px-3 py-2 font-mono" }, r.vix),
            h("td", { className: cx("px-3 py-2 font-mono", deltaTone(r.sp_5d_chg)) }, fmtPct(r.sp_5d_chg, 1)),
            h("td", { className: cx("px-3 py-2 font-mono", deltaTone(r.sentiment_score)) }, fmtPct(r.sentiment_score, 1)),
            h("td", { className: cx("px-3 py-2 font-mono", deltaTone(r.fund_adj)) }, fmtPct(r.fund_adj, 1)),
            h("td", { className: cx("px-3 py-2 font-mono", deltaTone(r.predicted_pct)) }, fmtPct(r.predicted_pct, 1)),
            h("td", { className: cx("px-3 py-2 font-mono", deltaTone(r.actual_pct)) }, fmtPct(r.actual_pct, 1)),
            h("td", { className: cx("px-3 py-2 font-mono", deltaTone(r.variance)) }, fmtPct(r.variance, 1)),
            h("td", { className: "px-3 py-2" }, h(Pill, { className: r.correct ? "border-pulse-green/30 bg-pulse-green/10 text-pulse-green" : "border-pulse-red/30 bg-pulse-red/10 text-pulse-red" }, r.correct ? "✓" : "✗"))
          )))
        )
      ) : null
    );
  }

  function SimulatorResults({ data }) {
    const s = data.stats, mc = data.monte_carlo;
    const probCls = mc.prob_target_pct >= 50 ? "text-pulse-green" : mc.prob_target_pct >= 25 ? "text-pulse-amber" : "text-pulse-red";
    const projectedPct = (val) => (val - s.initial_float) / s.initial_float * 100;
    return h("div", { className: "grid gap-4" },
      h("div", { className: "grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-6" },
        h(Metric, { label: "Start Capital", value: fmtGbp(s.initial_float, 0) }),
        h(Metric, { label: `After ${s.hist_weeks}wk (real)`, value: fmtGbp(s.hist_final_value, 0), tone: deltaTone(s.hist_return_pct), hint: fmtPct(s.hist_return_pct, 1) }),
        h(Metric, { label: "Win Rate", value: s.win_rate_pct + "%" }),
        h(Metric, { label: "Avg Win", value: "+" + s.avg_win_pct + "%", tone: "text-pulse-green" }),
        h(Metric, { label: "Avg Loss", value: "-" + s.avg_loss_pct + "%", tone: "text-pulse-red" }),
        h(Metric, { label: "Trades/Day", value: s.avg_trades_per_day })
      ),
      h(Card, { className: "p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, "Monte Carlo (1,000 sims, 12 months)"),
        h("div", { className: "mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-4" },
          h(McCard, { label: "Pessimistic (10th %)", value: fmtGbp(mc.p10, 0), pct: fmtPct(projectedPct(mc.p10), 1), tone: "text-pulse-red" }),
          h(McCard, { label: "Median (50th %)", value: fmtGbp(mc.p50, 0), pct: fmtPct(projectedPct(mc.p50), 1), tone: "text-pulse-ink" }),
          h(McCard, { label: "Optimistic (90th %)", value: fmtGbp(mc.p90, 0), pct: fmtPct(projectedPct(mc.p90), 1), tone: "text-pulse-green" }),
          h(McCard, { label: `Prob. hit ${fmtGbp(s.target, 0)}`, value: mc.prob_target_pct + "%", pct: "across 1,000 sims", tone: probCls })
        ),
        Array.isArray(mc.sample_paths) && mc.sample_paths.length ? h("div", { className: "mt-4" },
          h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, "Projected portfolio paths · 12 months"),
          h(MultiSparkline, {
            paths: [
              { data: mc.sample_paths[0] || [], color: "#ff4d6e", label: "Pessimistic (10th %)" },
              { data: mc.sample_paths[4] || [], color: "#00e5d9", label: "Median (50th %)" },
              { data: mc.sample_paths[9] || [], color: "#3fde7e", label: "Optimistic (90th %)" },
            ],
            target: s.target,
            targetLabel: `Target ${fmtGbp(s.target, 0)}`,
            startValue: s.initial_float,
            xStartLabel: "Day 0",
            xEndLabel: `Day ${(mc.sample_paths[0] || []).length - 1 || mc.n_days || 252}`,
          }),
          h("p", { className: "mt-3 text-xs leading-relaxed text-pulse-muted" },
            h("strong", { className: "text-pulse-ink" }, "Reading this chart: "),
            "Three representative paths sampled from the 1,000 Monte Carlo simulations. The vertical axis is portfolio value in GBP, horizontal is trading days forward. The dashed amber line is your target — paths that finish above it would hit the goal. Use the gap between the red (pessimistic) and green (optimistic) lines as a sense of how wide the outcome range is for your current trading edge."
          )
        ) : null
      ),
      h(Card, { className: "p-4 text-sm leading-relaxed text-pulse-muted" },
        h("strong", { className: "text-pulse-ink" }, "What this means: "),
        "Monte Carlo runs 1,000 possible 12-month paths using your recent win rate, average win/loss size, and trade frequency. The 10th percentile is a cautious case, the 50th is the middle, the 90th is strong upside. Probability of target shows how often paths finished above the goal."
      )
    );
  }

  function McCard({ label, value, pct, tone }) {
    return h("div", { className: "rounded-lg border border-pulse-line bg-pulse-panel p-3" },
      h("div", { className: "font-mono text-[10px] uppercase tracking-[0.18em] text-pulse-dim" }, label),
      h("div", { className: cx("mt-2 font-mono text-2xl", tone || "text-pulse-ink") }, value),
      h("div", { className: "mt-1 text-xs text-pulse-muted" }, pct)
    );
  }

  function MultiSparkline({ paths, target, targetLabel, startValue, xStartLabel, xEndLabel }) {
    const allData = paths.flatMap(p => p.data || []);
    if (!allData.length) return h("p", { className: "mt-2 text-sm text-pulse-muted" }, "No path data.");
    const extras = [target, startValue].filter(v => v != null);
    const min = Math.min.apply(null, allData.concat(extras));
    const max = Math.max.apply(null, allData.concat(extras));
    const span = max - min || 1;
    const w = 800, hgt = 200;
    function poly(arr) {
      if (arr.length < 2) return "";
      const step = w / (arr.length - 1);
      return arr.map((v, i) => `${(i * step).toFixed(1)},${(hgt - ((v - min) / span) * hgt).toFixed(1)}`).join(" ");
    }
    const yFor = v => hgt - ((v - min) / span) * hgt;
    const targetY = target != null ? yFor(target) : null;
    const startY = startValue != null ? yFor(startValue) : null;

    return h("div", { className: "mt-2" },
      // SVG chart
      h("svg", { viewBox: `0 0 ${w} ${hgt}`, className: "h-48 w-full", preserveAspectRatio: "none" },
        // Light horizontal gridlines
        [0.25, 0.5, 0.75].map(f => h("line", { key: f, x1: 0, x2: w, y1: hgt * f, y2: hgt * f, stroke: "#242a3a", strokeWidth: 0.5 })),
        // Start line (solid dim)
        startY != null ? h("line", { x1: 0, x2: w, y1: startY, y2: startY, stroke: "#5e6678", strokeWidth: 0.6, strokeDasharray: "2 4" }) : null,
        // Target line (amber dashed, bolder)
        targetY != null ? h("line", { x1: 0, x2: w, y1: targetY, y2: targetY, stroke: "#ffc857", strokeDasharray: "6 4", strokeWidth: 1.4 }) : null,
        // Sample paths
        paths.map((p, i) => h("polyline", { key: i, fill: "none", stroke: p.color, strokeWidth: 1.6, points: poly(p.data || []) }))
      ),
      // Y-axis range + X-axis labels (HTML so they don't get stretched by SVG preserveAspectRatio)
      h("div", { className: "mt-1 flex justify-between font-mono text-[10px] text-pulse-dim" },
        h("span", null, xStartLabel || "start"),
        h("span", null, "value range ", fmtGbp(min, 0), " → ", fmtGbp(max, 0)),
        h("span", null, xEndLabel || "end")
      ),
      // Legend
      h("div", { className: "mt-3 flex flex-wrap gap-x-4 gap-y-2 text-xs" },
        paths.map((p, i) => h("span", { key: i, className: "inline-flex items-center gap-2" },
          h("span", { className: "inline-block h-[2px] w-5", style: { backgroundColor: p.color } }),
          h("span", { className: "text-pulse-muted" }, p.label || `series ${i + 1}`)
        )),
        target != null ? h("span", { className: "inline-flex items-center gap-2" },
          h("span", { className: "inline-block h-[2px] w-5", style: { background: "repeating-linear-gradient(to right, #ffc857 0 4px, transparent 4px 8px)" } }),
          h("span", { className: "text-pulse-muted" }, targetLabel || "target")
        ) : null,
        startValue != null ? h("span", { className: "inline-flex items-center gap-2" },
          h("span", { className: "inline-block h-[2px] w-5", style: { background: "repeating-linear-gradient(to right, #5e6678 0 2px, transparent 2px 6px)" } }),
          h("span", { className: "text-pulse-muted" }, `Start ${fmtGbp(startValue, 0)}`)
        ) : null
      )
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Recommendations
  // ──────────────────────────────────────────────────────────────

  function Recommendations() {
    const [data, setData] = useState(null);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState("");
    const [progress, setProgress] = useState(null);
    const [reasoning, setReasoning] = useState(null);

    async function load() {
      if (busy) return;
      setBusy(true); setError(""); setProgress({ message: "Starting...", percent: 0 });
      try {
        const start = await api("/api/recommendations/start", { method: "POST" });
        const jobId = start.job_id || start.id;
        if (!jobId) { setData(start); setBusy(false); setProgress(null); return; }
        await pollProgress(jobId);
      } catch (err) { setError(err.message); setBusy(false); setProgress(null); }
    }

    async function pollProgress(jobId) {
      return new Promise((resolve, reject) => {
        const timer = setInterval(async () => {
          try {
            const p = await api(`/api/recommendations/progress/${jobId}`);
            setProgress(p);
            if (p.status === "completed") { clearInterval(timer); setData(p.result || {}); setBusy(false); setProgress(null); resolve(); }
            else if (p.status === "error") { clearInterval(timer); setError(p.error || "Recommendations failed"); setBusy(false); setProgress(null); reject(new Error(p.error || "Recommendations failed")); }
          } catch (err) { clearInterval(timer); setError(err.message); setBusy(false); setProgress(null); reject(err); }
        }, 1000);
      });
    }

    useEffect(() => {
      load();
    }, []);

    async function paperTrade(type, t) {
      try {
        await api(`/api/paper-portfolio/${type}`, { method: "POST", body: JSON.stringify({ ticker: t.ticker, qty: t.qty, price: t.current_price }) });
        load();
      } catch (err) { alert("Paper trade failed: " + err.message); }
    }

    const s = data?.summary || {};
    const buys = data?.buys || [];
    const sells = data?.sells || [];
    const explanation = data?.explanation || "";

    return h("div", null,
      h(SectionHead, { title: "Recommendations", kicker: "Buy/Sell signals", subtitle: "Sized from your float. Click View for reasoning.", actions: [h(Button, { key: "run", kind: "primary", onClick: load, disabled: busy }, busy ? "Running..." : "↻ Refresh")] }),
      error ? h(Empty, null, error) : null,
      progress ? h(Card, { className: "mb-4 p-4" },
        h("div", { className: "flex items-center justify-between text-sm" },
          h("span", { className: "text-pulse-muted" }, progress.message || "Loading recommendations..."),
          h("span", { className: "font-mono text-pulse-cyan" }, (progress.percent || 0) + "%")
        ),
        h("div", { className: "mt-2" }, h(ProgressBar, { value: progress.percent || 0, color: "bg-pulse-cyan" }))
      ) : null,
      data && s.initial_float ? h(Card, { className: "mb-4 p-4" },
        h("div", { className: "grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6" },
          h(Metric, { label: "Total Value", value: fmtGbp(s.total_portfolio_value, 0) }),
          h(Metric, { label: "Invested", value: fmtGbp(s.total_invested, 0) }),
          h(Metric, { label: "Available Cash", value: fmtGbp(s.available_cash, 0) }),
          h(Metric, { label: "Total P&L", value: fmtGbp(s.total_pnl, 0), tone: deltaTone(s.total_pnl) }),
          h(Metric, { label: `Target (${s.target_months || 12}mo)`, value: fmtGbp(s.target, 0) }),
          h(Metric, { label: "Remaining", value: fmtGbp(s.remaining_to_target, 0) })
        ),
        h("div", { className: "mt-4" },
          h("div", { className: "flex justify-between text-xs text-pulse-muted mb-2" },
            h("span", null, `${s.progress_pct || 0}% of target reached`),
            h("span", null, data.prediction_date ? `Predictions: ${data.prediction_date}` : "")
          ),
          h(ProgressBar, { value: s.progress_pct || 0, color: s.progress_pct >= 100 ? "bg-pulse-green" : "bg-pulse-cyan" })
        )
      ) : null,
      // Phase 5: portfolio trajectory banner — honest answer to "will this plan hit target?"
      data?.trajectory ? h(Card, { className: "mb-4 p-4 border-pulse-cyan/20" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, "Trajectory to target"),
        h("div", { className: "mt-3 grid grid-cols-2 gap-3 sm:grid-cols-3" },
          h(Metric, {
            label: "P(hit target) now",
            value: `${Math.round((data.trajectory.p_hit_target_current || 0) * 100)}%`,
            tone: (data.trajectory.p_hit_target_current || 0) >= 0.5 ? "positive" : "negative",
          }),
          h(Metric, {
            label: "If all buys accepted",
            value: `${Math.round((data.trajectory.p_hit_target_if_all_buys || 0) * 100)}%`,
            tone: (data.trajectory.p_hit_target_if_all_buys || 0) >= 0.5 ? "positive" : "negative",
          }),
          h(Metric, {
            label: "Δ from buys",
            value: `${(data.trajectory.p_hit_target_delta || 0) >= 0 ? "+" : ""}${Math.round((data.trajectory.p_hit_target_delta || 0) * 100)}pp`,
            tone: deltaTone(data.trajectory.p_hit_target_delta || 0),
          })
        ),
        h("div", { className: "mt-2 text-[11px] text-pulse-muted" },
          data.trajectory.alert_snapshot_age_hours != null
            ? `Cross-check snapshot ${data.trajectory.alert_snapshot_age_hours.toFixed(1)}h old`
            : "Cross-check snapshot unavailable — run Alerts to populate")
      ) : null,
      sells.length ? h(Card, { className: "mb-4 p-4 border-pulse-red/30" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-red" }, "⚠ Sell signals"),
        h("div", { className: "mt-3 overflow-x-auto" },
          h("table", { className: "min-w-full text-sm" },
            h("thead", { className: "bg-pulse-panel font-mono text-[10px] uppercase tracking-[0.16em] text-pulse-dim" },
              h("tr", null, ["Ticker","Trigger","Qty","Current","Proceeds","Unrealised P&L","Predicted","",""].map(x => h("th", { key: x, className: "px-3 py-2 text-left" }, x)))
            ),
            h("tbody", null, sells.map((sl, i) => h("tr", { key: i, className: "border-t border-pulse-line/60" },
              h("td", { className: "px-3 py-2" }, h("strong", { className: "font-mono text-pulse-cyan" }, sl.ticker), h("div", { className: "text-xs text-pulse-muted" }, sl.name)),
              h("td", { className: "px-3 py-2" }, h(Pill, { className: sl.trigger === "STOP LOSS" ? "border-pulse-red/30 bg-pulse-red/10 text-pulse-red" : sl.trigger === "TAKE PROFIT" ? "border-pulse-green/30 bg-pulse-green/10 text-pulse-green" : "border-pulse-amber/30 bg-pulse-amber/10 text-pulse-amber" }, sl.trigger)),
              h("td", { className: "px-3 py-2 font-mono" }, sl.qty),
              h("td", { className: "px-3 py-2 font-mono" }, fmtGbp(sl.current_price, 2)),
              h("td", { className: "px-3 py-2 font-mono" }, fmtGbp(sl.estimated_proceeds, 0)),
              h("td", { className: cx("px-3 py-2 font-mono", deltaTone(sl.unrealised_pnl)) }, fmtGbp(sl.unrealised_pnl, 0), " (", fmtPct(sl.unrealised_pct, 1), ")"),
              h("td", { className: "px-3 py-2 font-mono" }, sl.score_value != null ? `${sl.score_value}/100` : "—"),
              h("td", { className: "px-3 py-2" }, h(Button, { onClick: () => setReasoning(Object.assign({ type: "Sell" }, sl)), className: "min-h-8 px-2 text-xs" }, "View")),
              h("td", { className: "px-3 py-2" }, h(Button, { onClick: () => paperTrade("sell", sl), className: "min-h-8 px-2 text-xs", kind: "danger" }, "− Paper Sell"))
            )))
          )
        )
      ) : null,
      buys.length ? h(Card, { className: "mb-4 p-4 border-pulse-green/30" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-green" }, "↑ Buy signals"),
        h("div", { className: "mt-3 overflow-x-auto" },
          h("table", { className: "min-w-full text-sm" },
            h("thead", { className: "bg-pulse-panel font-mono text-[10px] uppercase tracking-[0.16em] text-pulse-dim" },
              h("tr", null, ["#","Ticker","Confidence","Cross-check","Δ Target","P(hit) Δ","Price","Qty","Est. Cost","",""].map(x => h("th", { key: x, className: "px-3 py-2 text-left" }, x)))
            ),
            h("tbody", null, buys.map((b, i) => {
              // Phase 5: consistency badge tone
              const cc = b.consistency || { badge: "stale", label: "—" };
              const ccClass = cc.badge === "agree"        ? "border-pulse-green/30 bg-pulse-green/10 text-pulse-green"
                            : cc.badge === "contradiction" ? "border-pulse-red/30 bg-pulse-red/10 text-pulse-red"
                            : cc.badge === "no_thesis"     ? "border-pulse-amber/30 bg-pulse-amber/10 text-pulse-amber"
                            :                                "border-pulse-line/40 bg-pulse-panel text-pulse-dim";
              const dGbp = b.delta_to_target_gbp;
              const dPct = b.delta_to_target_pct_of_gap;
              const pDelta = b.p_hit_target_delta || 0;
              return h("tr", { key: i, className: "border-t border-pulse-line/60" },
                h("td", { className: "px-3 py-2 text-pulse-muted" }, "#" + (i + 1)),
                h("td", { className: "px-3 py-2" }, h("strong", { className: "font-mono text-pulse-cyan" }, b.ticker), h("div", { className: "text-xs text-pulse-muted" }, b.name), h("div", { className: "mt-1" }, h(FactorCluster, { scores: b.factor_scores || {} }))),
                h("td", { className: "px-3 py-2" }, h(ConfidencePill, { value: b.confidence })),
                h("td", { className: "px-3 py-2" }, h(Pill, { className: ccClass }, cc.label)),
                h("td", { className: cx("px-3 py-2 font-mono", deltaTone(dGbp)) },
                  dGbp != null ? fmtGbp(dGbp, 0) : "—",
                  dPct != null ? h("div", { className: "text-[10px] text-pulse-muted" }, `${dPct >= 0 ? "+" : ""}${dPct.toFixed(1)}% of gap`) : null
                ),
                h("td", { className: cx("px-3 py-2 font-mono", deltaTone(pDelta)) },
                  `${pDelta >= 0 ? "+" : ""}${Math.round(pDelta * 100)}pp`,
                  h("div", { className: "text-[10px] text-pulse-muted" }, `→ ${Math.round((b.p_hit_target_with_this || 0) * 100)}%`)
                ),
                h("td", { className: "px-3 py-2 font-mono" }, fmtGbp(b.current_price, 2)),
                h("td", { className: "px-3 py-2 font-mono" }, b.qty),
                h("td", { className: "px-3 py-2 font-mono" }, fmtGbp(b.estimated_cost, 0)),
                h("td", { className: "px-3 py-2" }, h(Button, { onClick: () => setReasoning(Object.assign({ type: "Buy" }, b)), className: "min-h-8 px-2 text-xs" }, "View")),
                h("td", { className: "px-3 py-2" }, h(Button, { onClick: () => paperTrade("buy", b), className: "min-h-8 px-2 text-xs", kind: "primary" }, "+ Paper Buy"))
              );
            }))
          )
        )
      ) : null,
      data && !buys.length && !sells.length ? h(Empty, null, explanation || "No recommendations available for the latest prediction set.") : null,
      !data && !busy ? h(Empty, null, "Click Refresh to generate recommendations.") : null,
      reasoning ? h(SlideOver, { title: `${reasoning.ticker} ${reasoning.type}`, kicker: "Reasoning", onClose: () => setReasoning(null) },
        h("div", { className: "grid gap-3" },
          h(Card, { className: "p-4" },
            h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, "Ticker"),
            h("div", { className: "mt-2 text-xl font-semibold" }, reasoning.ticker, h("span", { className: "ml-2 text-sm text-pulse-muted" }, reasoning.name || ""))
          ),
          h(Card, { className: "p-4 text-sm leading-relaxed text-pulse-muted whitespace-pre-wrap" }, reasoning.reasoning || "No reasoning available.")
        )
      ) : null
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Alerts
  // ──────────────────────────────────────────────────────────────

  function Alerts() {
    const [alerts, setAlerts] = useState([]);
    const [monStatus, setMonStatus] = useState(null);
    const [settings, setSettings] = useState({});
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState("");
    const [savedMsg, setSavedMsg] = useState("");

    async function load() {
      setBusy(true); setError("");
      try {
        const [a, st, s] = await Promise.all([api("/api/alerts"), api("/api/alerts/status"), api("/api/settings")]);
        setAlerts(Array.isArray(a) ? a : []);
        setMonStatus(st || {});
        setSettings(s || {});
      } catch (err) { setError("Error loading alerts: " + err.message); } finally { setBusy(false); }
    }
    useEffect(() => { load(); }, []);

    async function saveSettings() {
      setSavedMsg("Saving...");
      try {
        const existing = await api("/api/settings");
        const updated = Object.assign({}, existing, settings);
        await api("/api/settings", { method: "POST", body: JSON.stringify(updated) });
        setSavedMsg("Saved ✓");
        setTimeout(() => setSavedMsg(""), 3000);
      } catch (err) { setSavedMsg("Save failed: " + err.message); }
    }

    async function testPreview() {
      setBusy(true);
      try {
        const data = await api("/api/alerts/test-preview", { method: "POST" });
        const parts = [];
        parts.push(data.email_sent ? "Email sent ✓" : "Email not configured");
        parts.push(data.sms_sent ? "SMS sent ✓" : "SMS not configured");
        setError(parts.join(" · "));
      } catch (err) { setError("Error: " + err.message); } finally { setBusy(false); }
    }

    async function clearHistory() {
      if (!confirm("Clear all alert history?")) return;
      try { await api("/api/alerts", { method: "DELETE" }); load(); } catch (err) { setError(err.message); }
    }

    function setField(key, value) { setSettings(s => Object.assign({}, s, { [key]: value })); }

    return h("div", null,
      h(SectionHead, { title: "Alerts", kicker: "Recommendation alerts", subtitle: "Email & WhatsApp notifications when high-conviction BUY or SELL signals appear.", actions: [
        h(Button, { key: "test", onClick: testPreview, disabled: busy }, "Send Preview"),
        h(Button, { key: "refresh", onClick: load, disabled: busy }, "↻ Refresh"),
        h(Button, { key: "clear", kind: "danger", onClick: clearHistory }, "Clear History"),
      ] }),
      error ? h("p", { className: "mb-3 text-sm text-pulse-amber" }, error) : null,
      monStatus ? h(Card, { className: "mb-4 p-4" },
        h("div", { className: "grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6" },
          h(Metric, { label: "Monitor", value: monStatus.active ? "● Running" : "○ Idle", tone: monStatus.active ? "text-pulse-green" : "text-pulse-muted" }),
          h(Metric, { label: "Last Check", value: monStatus.last_check ? new Date(monStatus.last_check).toLocaleTimeString() : "—" }),
          h(Metric, { label: "Watching", value: monStatus.watching != null ? `${monStatus.watching} stock${monStatus.watching !== 1 ? "s" : ""}` : "—" }),
          h(Metric, { label: "Email", value: monStatus.notifications?.email ? "✓ On" : "✗ Off", tone: monStatus.notifications?.email ? "text-pulse-green" : "text-pulse-muted" }),
          h(Metric, { label: "SMS", value: monStatus.notifications?.sms ? "✓ On" : "✗ Off", tone: monStatus.notifications?.sms ? "text-pulse-green" : "text-pulse-muted" }),
          h(Metric, { label: "Focus", value: monStatus.strategy?.focus || "Strong BUY/SELL" })
        )
      ) : null,
      h(Card, { className: "mb-4 p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, "Alert settings"),
        h("div", { className: "mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-3" },
          h(AlertSetting, { label: "Start Capital", unit: "GBP", value: settings.initial_float || "", onChange: v => setField("initial_float", v) }),
          h(AlertSetting, { label: "Price Swing Threshold", unit: "%", value: settings.alert_price_swing_pct || "", onChange: v => setField("alert_price_swing_pct", v) }),
          h(AlertSetting, { label: "Cooldown", unit: "hrs", value: settings.alert_cooldown_hours || "", onChange: v => setField("alert_cooldown_hours", v) }),
          h(AlertSetting, { label: "Max BUY alerts", unit: "/email", value: settings.alert_top_buys || "", onChange: v => setField("alert_top_buys", v) }),
          h(AlertSetting, { label: "Max SELL alerts", unit: "/email", value: settings.alert_top_sells || "", onChange: v => setField("alert_top_sells", v) }),
          h(AlertSetting, { label: "BUY min score", unit: "/100", value: settings.alert_buy_min_score || "", onChange: v => setField("alert_buy_min_score", v) }),
          h(AlertSetting, { label: "SELL max score", unit: "/100", value: settings.alert_sell_max_score || "", onChange: v => setField("alert_sell_max_score", v) })
        ),
        h("div", { className: "mt-4 flex items-center justify-between gap-3" },
          h("span", { className: cx("text-sm", savedMsg.includes("failed") ? "text-pulse-red" : "text-pulse-green") }, savedMsg),
          h(Button, { onClick: saveSettings }, "Save settings")
        )
      ),
      h(Card, { className: "p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, "Alert history"),
        alerts.length === 0 ? h("p", { className: "mt-3 text-sm text-pulse-muted" }, "No alerts yet.") :
        h("div", { className: "mt-3 grid gap-3" }, alerts.map((a, i) => h("div", { key: i, className: "rounded-lg border border-pulse-line bg-pulse-panel p-3" },
          h("div", { className: "flex items-start justify-between gap-3" },
            h("div", null,
              h("strong", { className: "font-mono text-pulse-cyan" }, a.ticker || "—"),
              h("span", { className: "ml-2 text-sm text-pulse-muted" }, a.name || "")
            ),
            h("div", { className: "text-xs text-pulse-muted" }, a.timestamp ? new Date(a.timestamp).toLocaleString() : "")
          ),
          h("div", { className: "mt-2 flex flex-wrap items-center gap-2" },
            a.price != null ? h("span", { className: "font-mono text-sm" }, "$" + Number(a.price).toFixed(2)) : null,
            (a.signals || []).map((s, j) => h(Pill, { key: j, className: s.type === "buy" ? "border-pulse-green/30 bg-pulse-green/10 text-pulse-green" : s.type === "sell" ? "border-pulse-red/30 bg-pulse-red/10 text-pulse-red" : "border-pulse-amber/30 bg-pulse-amber/10 text-pulse-amber" }, s.signal || s.type))
          ),
          h("div", { className: "mt-2 flex gap-3 text-xs text-pulse-muted" },
            h("span", null, a.notified_email ? "✓ Email" : "— Email"),
            h("span", null, a.notified_sms ? "✓ SMS" : "— SMS")
          )
        )))
      )
    );
  }

  function AlertSetting({ label, unit, value, onChange }) {
    return h("div", { className: "grid gap-1" },
      h("label", { className: "text-[10px] font-mono uppercase tracking-wide text-pulse-dim" }, label),
      h("div", { className: "flex items-center gap-2" },
        h(TextInput, { type: "number", value: value, onChange: e => onChange(e.target.value) }),
        h("span", { className: "shrink-0 text-xs text-pulse-muted" }, unit)
      )
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Portfolio
  // ──────────────────────────────────────────────────────────────

  function Portfolio({ openDetail }) {
    const [data, setData] = useState(null);
    const [busy, setBusy] = useState(false);
    const [status, setStatus] = useState("");
    const [trade, setTrade] = useState({ ticker: "", qty: "", price: "", date: "" });
    const fileRef = useRef(null);
    const pdfRef = useRef(null);

    async function load() {
      setBusy(true); setStatus("");
      try { setData(await api("/api/portfolio")); }
      catch (err) { setStatus(err.message); }
      finally { setBusy(false); }
    }
    useEffect(() => { load(); }, []);

    async function submitTrade(type) {
      if (!trade.ticker || !trade.qty || !trade.price) { setStatus("Please enter ticker, qty and price."); return; }
      setStatus(`Recording ${type}...`);
      try {
        await api(`/api/portfolio/${type}`, { method: "POST", body: JSON.stringify({ ticker: trade.ticker.toUpperCase(), qty: parseFloat(trade.qty), price: parseFloat(trade.price), date: trade.date || null }) });
        setTrade({ ticker: "", qty: "", price: "", date: "" });
        setStatus(`${type === "buy" ? "Buy" : "Sell"} recorded.`);
        load();
      } catch (err) { setStatus("Error: " + err.message); }
    }

    async function uploadCsv(e) {
      const file = e.target.files[0]; if (!file) return;
      setStatus("Importing CSV...");
      const form = new FormData(); form.append("file", file);
      try {
        const data = await api("/api/portfolio/import", { method: "POST", body: form });
        setStatus(`Imported ${data.imported || 0} trade${data.imported === 1 ? "" : "s"}.`);
        load();
      } catch (err) { setStatus("CSV import failed: " + err.message); }
      finally { e.target.value = ""; }
    }

    async function uploadPdf(e) {
      const file = e.target.files[0]; if (!file) return;
      setStatus("Importing PDF... Claude is parsing (15-30s)...");
      const form = new FormData(); form.append("file", file);
      try {
        const data = await api("/api/portfolio/import-pdf", { method: "POST", body: form });
        if (data.imported === 0 && data.skipped === 0) { setStatus(data.message || "No transactions found."); }
        else { setStatus(`Imported ${data.imported || 0} · skipped ${data.skipped || 0}.`); }
        load();
      } catch (err) { setStatus("PDF import failed: " + err.message); }
      finally { e.target.value = ""; }
    }

    const positions = data?.positions || [];
    const s = data?.summary || {};

    return h("div", null,
      h(SectionHead, { title: "Portfolio", kicker: "Holdings", subtitle: "Track purchased stocks, cost basis and profit/loss.", actions: [
        h("a", { key: "tpl", href: "/api/portfolio/template", download: true, className: "inline-flex min-h-11 items-center justify-center rounded-lg border border-pulse-line bg-pulse-panel px-4 text-sm text-pulse-ink" }, "↓ CSV Template"),
        h(Button, { key: "csv", onClick: () => fileRef.current?.click() }, "⇪ Import CSV"),
        h(Button, { key: "pdf", onClick: () => pdfRef.current?.click() }, "⇪ Import Saxo PDF"),
        h(Button, { key: "refresh", onClick: load, disabled: busy }, "↻ Refresh"),
        h("input", { key: "csvf", ref: fileRef, type: "file", accept: ".csv", className: "hidden", onChange: uploadCsv }),
        h("input", { key: "pdff", ref: pdfRef, type: "file", accept: ".pdf", className: "hidden", onChange: uploadPdf }),
      ] }),
      data ? h(Card, { className: "mb-4 p-4" },
        h("div", { className: "grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5" },
          h(Metric, { label: "Invested Cost", value: fmtUsd(s.total_invested) }),
          h(Metric, { label: "Market Value", value: fmtUsd(s.total_current_value) }),
          h(Metric, { label: "Unrealised P&L", value: fmtUsd(s.total_unrealised_pnl), tone: deltaTone(s.total_unrealised_pnl) }),
          h(Metric, { label: "Realised P&L", value: fmtUsd(s.total_realised_pnl), tone: deltaTone(s.total_realised_pnl) }),
          h(Metric, { label: "Total P&L", value: fmtUsd(s.total_pnl), tone: deltaTone(s.total_pnl) })
        )
      ) : null,
      h(Card, { className: "mb-4 p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim mb-3" }, "Record trade"),
        h("div", { className: "grid gap-2 sm:grid-cols-[1fr_1fr_1fr_1fr_auto_auto]" },
          h(TextInput, { placeholder: "Ticker", value: trade.ticker, onChange: e => setTrade(t => Object.assign({}, t, { ticker: e.target.value.toUpperCase() })) }),
          h(TextInput, { type: "number", placeholder: "Qty", value: trade.qty, onChange: e => setTrade(t => Object.assign({}, t, { qty: e.target.value })) }),
          h(TextInput, { type: "number", placeholder: "Price $", value: trade.price, onChange: e => setTrade(t => Object.assign({}, t, { price: e.target.value })) }),
          h(TextInput, { type: "date", value: trade.date, onChange: e => setTrade(t => Object.assign({}, t, { date: e.target.value })) }),
          h(Button, { kind: "primary", onClick: () => submitTrade("buy") }, "+ Buy"),
          h(Button, { kind: "danger", onClick: () => submitTrade("sell") }, "− Sell"),
        ),
        h(Status, { message: status, className: "mt-3" })
      ),
      h(ResponsiveTable, {
        rows: positions,
        onRowClick: r => openDetail(r.ticker),
        emptyText: busy ? "Loading..." : "No positions yet. Use the form above to record a purchase.",
        columns: [
          { key: "ticker", label: "Ticker", render: p => h("strong", { className: "font-mono text-pulse-cyan" }, p.ticker) },
          { key: "name", label: "Name", render: p => h("span", { className: "text-pulse-muted text-xs" }, p.name) },
          { key: "shares", label: "Shares", className: "font-mono" },
          { key: "avg_cost", label: "Avg Cost", className: "font-mono", render: p => fmtUsd(p.avg_cost) },
          { key: "current_price", label: "Current", className: "font-mono", render: p => fmtUsd(p.current_price) },
          { key: "cost_basis", label: "Cost Basis", className: "font-mono", render: p => fmtUsd(p.cost_basis) },
          { key: "current_value", label: "Value", className: "font-mono", render: p => fmtUsd(p.current_value) },
          { key: "unrealised_pnl", label: "Unrealised", className: "font-mono", render: p => h("span", { className: deltaTone(p.unrealised_pnl) }, fmtUsd(p.unrealised_pnl), " (", fmtPct(p.unrealised_pct, 1), ")") },
          { key: "realised_pnl", label: "Realised", className: "font-mono", render: p => h("span", { className: deltaTone(p.realised_pnl) }, fmtUsd(p.realised_pnl)) },
        ],
        mobileRender: p => h("div", null,
          h("div", { className: "flex items-start justify-between gap-3" },
            h("div", null, h("div", { className: "font-mono text-lg text-pulse-cyan" }, p.ticker), h("div", { className: "text-xs text-pulse-muted" }, p.name)),
            h("div", { className: "text-right font-mono" }, h("div", null, fmtUsd(p.current_value)), h("div", { className: cx("text-xs", deltaTone(p.unrealised_pnl)) }, fmtUsd(p.unrealised_pnl), " ", fmtPct(p.unrealised_pct, 1)))
          ),
          h("div", { className: "mt-3 grid grid-cols-3 gap-2" },
            h(Metric, { label: "Shares", value: p.shares }),
            h(Metric, { label: "Avg Cost", value: fmtUsd(p.avg_cost) }),
            h(Metric, { label: "Realised", value: fmtUsd(p.realised_pnl), tone: deltaTone(p.realised_pnl) })
          )
        )
      })
    );
  }

  // ──────────────────────────────────────────────────────────────
  // Paper P&L
  // ──────────────────────────────────────────────────────────────

  function PaperPnL() {
    const [data, setData] = useState(null);
    const [busy, setBusy] = useState(false);
    const [status, setStatus] = useState("");

    async function load() {
      setBusy(true); setStatus("");
      try { setData(await api("/api/paper-portfolio")); }
      catch (err) { setStatus(err.message); }
      finally { setBusy(false); }
    }
    useEffect(() => { load(); }, []);

    async function reset() {
      if (!confirm("Reset your entire paper portfolio? This cannot be undone.")) return;
      try { await api("/api/paper-portfolio/reset", { method: "DELETE" }); load(); } catch (err) { setStatus(err.message); }
    }

    const s = data?.summary || {};
    const positions = data?.positions || [];
    const txs = data?.transactions || [];
    const holdingsValue = s.total_current_value ?? ((s.total_value ?? 0) - (s.cash ?? 0));
    const unrealised = s.total_unrealised_pnl ?? (holdingsValue - (s.total_invested ?? 0));

    return h("div", null,
      h(SectionHead, { title: "Paper P&L", kicker: "Simulated trades", subtitle: "Trades added from the Recommendations tab.", actions: [
        h(Button, { key: "refresh", onClick: load, disabled: busy }, "↻ Refresh"),
        h(Button, { key: "reset", kind: "danger", onClick: reset }, "✕ Reset"),
      ] }),
      h(Status, { message: status, className: "mb-3" }),
      data ? h(Card, { className: "mb-4 p-4" },
        h("div", { className: "grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-7" },
          h(Metric, { label: "Start Float", value: fmtGbp(s.initial_float, 0) }),
          h(Metric, { label: "Cash", value: fmtGbp(s.cash) }),
          h(Metric, { label: "Holdings", value: fmtGbp(holdingsValue) }),
          h(Metric, { label: "Total Value", value: fmtGbp(s.total_value) }),
          h(Metric, { label: "Unrealised", value: fmtGbp(unrealised), tone: deltaTone(unrealised) }),
          h(Metric, { label: "Realised", value: fmtGbp(s.realised_pnl), tone: deltaTone(s.realised_pnl) }),
          h(Metric, { label: "Total P&L", value: `${fmtGbp(s.total_pnl)}`, hint: fmtPct(s.total_pnl_pct, 1), tone: deltaTone(s.total_pnl) })
        )
      ) : null,
      positions.length ? h("div", { className: "mb-4" },
        h("h3", { className: "mb-2 font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, "Open positions"),
        h("div", { className: "overflow-x-auto rounded-xl border border-pulse-line bg-pulse-card" },
          h("table", { className: "min-w-full text-sm" },
            h("thead", { className: "bg-pulse-panel font-mono text-[10px] uppercase tracking-[0.16em] text-pulse-dim" },
              h("tr", null, ["Ticker","Shares","Avg Cost","Current","Cost Basis","Value","Unrealised","%","Realised"].map(x => h("th", { key: x, className: "px-3 py-2 text-left" }, x)))
            ),
            h("tbody", null, positions.map((p, i) => h("tr", { key: i, className: "border-t border-pulse-line/60" },
              h("td", { className: "px-3 py-2" }, h("strong", { className: "font-mono text-pulse-cyan" }, p.ticker), h("div", { className: "text-xs text-pulse-muted" }, p.name)),
              h("td", { className: "px-3 py-2 font-mono" }, p.shares),
              h("td", { className: "px-3 py-2 font-mono" }, fmtGbp(p.avg_cost)),
              h("td", { className: "px-3 py-2 font-mono" }, fmtGbp(p.current_price)),
              h("td", { className: "px-3 py-2 font-mono" }, fmtGbp(p.cost_basis)),
              h("td", { className: "px-3 py-2 font-mono" }, fmtGbp(p.current_value)),
              h("td", { className: cx("px-3 py-2 font-mono", deltaTone(p.unrealised_pnl)) }, fmtGbp(p.unrealised_pnl)),
              h("td", { className: cx("px-3 py-2 font-mono", deltaTone(p.unrealised_pct)) }, fmtPct(p.unrealised_pct, 1)),
              h("td", { className: cx("px-3 py-2 font-mono", deltaTone(p.realised_pnl)) }, fmtGbp(p.realised_pnl))
            )))
          )
        )
      ) : null,
      txs.length ? h("div", null,
        h("h3", { className: "mb-2 font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, "Trade history"),
        h("div", { className: "overflow-x-auto rounded-xl border border-pulse-line bg-pulse-card" },
          h("table", { className: "min-w-full text-sm" },
            h("thead", { className: "bg-pulse-panel font-mono text-[10px] uppercase tracking-[0.16em] text-pulse-dim" },
              h("tr", null, ["Date","Type","Ticker","Shares","Price","Value","P&L"].map(x => h("th", { key: x, className: "px-3 py-2 text-left" }, x)))
            ),
            h("tbody", null, txs.map((t, i) => {
              const value = (Number(t.qty) || 0) * (Number(t.price) || 0);
              return h("tr", { key: i, className: "border-t border-pulse-line/60" },
                h("td", { className: "px-3 py-2 text-xs text-pulse-muted" }, t.date),
                h("td", { className: "px-3 py-2" }, h(Pill, { className: t.type === "buy" ? "border-pulse-green/30 bg-pulse-green/10 text-pulse-green" : "border-pulse-red/30 bg-pulse-red/10 text-pulse-red" }, t.type.toUpperCase())),
                h("td", { className: "px-3 py-2" }, h("strong", { className: "font-mono text-pulse-cyan" }, t.ticker)),
                h("td", { className: "px-3 py-2 font-mono" }, t.qty),
                h("td", { className: "px-3 py-2 font-mono" }, fmtGbp(t.price)),
                h("td", { className: "px-3 py-2 font-mono" }, fmtGbp(value)),
                h("td", { className: "px-3 py-2 font-mono" }, t.realised_pnl == null ? h("span", { className: "text-pulse-muted text-xs" }, "Open") : h("span", { className: deltaTone(t.realised_pnl) }, fmtGbp(t.realised_pnl)))
              );
            }))
          )
        )
      ) : null,
      positions.length === 0 && txs.length === 0 && data ? h(Empty, null, "No paper trades yet. Click \"+ Paper Buy\" on a recommendation to start.") : null
    );
  }

  // ──────────────────────────────────────────────────────────────
  // App
  // ──────────────────────────────────────────────────────────────

  function App() {
    const [ready, setReady] = useState(false);
    const [user, setUser] = useState("");
    const [active, setActive] = useState("screener");
    const [detail, setDetail] = useState(null);

    async function check() {
      if (!token()) { setReady(true); return; }
      try { const data = await api("/api/auth/me"); setUser(data.username || "user"); }
      catch { setToken(""); }
      finally { setReady(true); }
    }

    useEffect(() => { check(); }, []);
    if (!ready) return h("div", { className: "flex min-h-screen items-center justify-center text-pulse-muted" }, "Loading StockLens...");
    if (!token()) return h(Login, { onLogin: check });

    const openDetail = (ticker) => setDetail(ticker);

    const tabs = {
      screener: h(Screener, { openDetail }),
      watchlist: h(Watchlist, { openDetail }),
      sentiment: h(Sentiment),
      ai: h(AIAdvisor),
      predictions: h(Predictions),
      thesis: h(Thesis),
      backtest: h(Backtest),
      recommendations: h(Recommendations),
      alerts: h(Alerts),
      portfolio: h(Portfolio, { openDetail }),
      paper: h(PaperPnL),
    };

    return h(React.Fragment, null,
      h(Shell, { user, active, setActive, logout: () => { setToken(""); setUser(""); } }, tabs[active] || tabs.screener),
      detail ? h(StockDetail, { ticker: detail, onClose: () => setDetail(null) }) : null
    );
  }

  ReactDOM.createRoot(document.getElementById("root")).render(h(App));
})();
