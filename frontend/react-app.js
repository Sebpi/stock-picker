(function () {
  const { useEffect, useMemo, useState } = React;
  const h = React.createElement;
  const API = "";
  const PICK_SHOVELS_API = "https://pick-shovels-wistful-morning-252.fly.dev";
  const TOKEN_KEY = "stocklens_token";

  const TABS = [
    ["screener", "Screener"],
    ["watchlist", "Watchlist"],
    ["predictions", "Predictions"],
    ["thesis", "Thesis"],
    ["recommendations", "Signals"],
    ["portfolio", "Portfolio"],
    ["paper", "Paper P&L"],
    ["alerts", "Alerts"],
    ["sentiment", "Sentiment"],
    ["ops", "Ops"],
  ];

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

  function fmtUsd(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return "-";
    return n.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: n >= 1000 ? 0 : 2 });
  }

  function fmtPct(value, digits) {
    const n = Number(value);
    if (!Number.isFinite(n)) return "-";
    return `${n >= 0 ? "+" : ""}${n.toFixed(digits == null ? 1 : digits)}%`;
  }

  function fmtDate(value) {
    if (!value) return "-";
    try { return new Date(value).toLocaleString(); } catch { return String(value); }
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

  function Card(props) {
    return h("section", {
      className: cx("rounded-xl border border-pulse-line bg-pulse-card/88 shadow-glow", props.className)
    }, props.children);
  }

  function Button(props) {
    const kind = props.kind || "secondary";
    return h("button", Object.assign({}, props, {
      className: cx(
        "min-h-11 rounded-lg px-4 text-sm font-semibold transition disabled:opacity-50 disabled:cursor-not-allowed",
        kind === "primary"
          ? "bg-gradient-to-r from-pulse-cyan to-pulse-magenta text-black shadow-glow"
          : kind === "danger"
            ? "border border-pulse-red/40 bg-pulse-red/10 text-pulse-red"
            : "border border-pulse-line bg-pulse-panel text-pulse-ink hover:border-pulse-cyan/50",
        props.className
      )
    }), props.children);
  }

  function TextInput(props) {
    return h("input", Object.assign({}, props, {
      className: cx(
        "h-11 w-full rounded-lg border border-pulse-line bg-pulse-panel px-3 text-base text-pulse-ink placeholder:text-pulse-dim outline-none focus:border-pulse-cyan focus:ring-2 focus:ring-pulse-cyan/20",
        props.className
      )
    }));
  }

  function Pill(props) {
    return h("span", { className: cx("inline-flex items-center rounded-full border px-2.5 py-1 font-mono text-[10px] uppercase tracking-[0.12em]", props.className) }, props.children);
  }

  function Metric(props) {
    return h("div", { className: "rounded-lg border border-pulse-line bg-pulse-panel p-3" },
      h("div", { className: "font-mono text-[10px] uppercase tracking-[0.18em] text-pulse-dim" }, props.label),
      h("div", { className: cx("mt-2 font-mono text-lg tabular-nums", props.tone || "text-pulse-ink") }, props.value || "-"),
      props.hint ? h("div", { className: "mt-1 text-xs text-pulse-muted" }, props.hint) : null
    );
  }

  function ProgressBar(props) {
    const value = Math.max(0, Math.min(100, Number(props.value || 0)));
    return h("div", { className: "h-1.5 overflow-hidden rounded-full bg-pulse-bg" },
      h("div", { className: cx("h-full rounded-full", props.color || scoreBg(value)), style: { width: `${value}%` } })
    );
  }

  function Empty(props) {
    return h(Card, { className: "p-5 text-sm text-pulse-muted" }, props.children || "Nothing here yet.");
  }

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
        const data = await api("/api/auth/login", {
          method: "POST",
          body: JSON.stringify({ username, password })
        });
        setToken(data.access_token);
        onLogin();
      } catch (err) {
        setMessage(err.message || "Could not sign in.");
      } finally {
        setBusy(false);
      }
    }

    async function forgot(e) {
      e.preventDefault();
      setBusy(true); setMessage("");
      try {
        const data = await api("/api/auth/forgot-password", {
          method: "POST",
          body: JSON.stringify({ username })
        });
        setMessage(data.message || "If that username exists, a reset link has been sent.");
      } catch (err) {
        setMessage(err.message || "Could not request reset.");
      } finally {
        setBusy(false);
      }
    }

    async function reset(e) {
      e.preventDefault();
      if (newPassword !== confirm) {
        setMessage("Passwords do not match.");
        return;
      }
      setBusy(true); setMessage("");
      try {
        await api("/api/auth/reset-password", {
          method: "POST",
          body: JSON.stringify({ token: resetToken, new_password: newPassword })
        });
        window.history.replaceState(null, "", "/");
        setMode("login");
        setMessage("Password updated. Please sign in.");
      } catch (err) {
        setMessage(err.message || "Could not reset password.");
      } finally {
        setBusy(false);
      }
    }

    return h("main", { className: "flex min-h-screen items-center justify-center px-4 py-10" },
      h(Card, { className: "w-full max-w-sm p-6" },
        h("div", { className: "mb-6 flex items-center gap-3" },
          h("img", { src: "/static/logo.svg", className: "h-10 w-10", alt: "" }),
          h("div", null,
            h("h1", { className: "text-xl font-semibold" }, "Stock", h("span", { className: "bg-gradient-to-r from-pulse-cyan to-pulse-magenta bg-clip-text text-transparent" }, "Lens")),
            h("p", { className: "text-xs text-pulse-muted" }, "React mobile-first preview")
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

  function Shell({ user, active, setActive, logout, children }) {
    return h("div", { className: "min-h-screen pb-20 md:pb-0" },
      h("header", { className: "sticky top-0 z-30 border-b border-pulse-line bg-pulse-bg/86 px-3 pt-[max(.75rem,env(safe-area-inset-top))] backdrop-blur md:px-5" },
        h("div", { className: "mx-auto flex max-w-7xl items-center gap-3 py-3" },
          h("img", { src: "/static/logo.svg", className: "h-8 w-8 shrink-0", alt: "" }),
          h("div", { className: "min-w-0" },
            h("div", { className: "text-base font-semibold leading-tight" }, "Stock", h("span", { className: "bg-gradient-to-r from-pulse-cyan to-pulse-magenta bg-clip-text text-transparent" }, "Lens")),
            h("div", { className: "truncate text-[11px] text-pulse-dim" }, user || "signed in", " · v2.0.1")
          ),
          h("a", { href: "/legacy", className: "ml-auto hidden rounded-lg border border-pulse-line px-3 py-2 text-xs text-pulse-muted hover:text-pulse-cyan sm:inline-flex" }, "Legacy"),
          h(Button, { onClick: logout, className: "ml-auto sm:ml-0 min-h-9 px-3 text-xs" }, "Sign out")
        ),
        h("nav", { className: "scrollbar-none mx-auto flex max-w-7xl gap-1 overflow-x-auto pb-2" },
          TABS.map(([id, label]) => h("button", {
            key: id,
            onClick: () => setActive(id),
            className: cx(
              "shrink-0 rounded-lg px-3 py-2 text-xs font-medium transition",
              active === id ? "bg-pulse-card text-pulse-ink ring-1 ring-pulse-line" : "text-pulse-muted hover:bg-pulse-panel hover:text-pulse-ink"
            )
          }, label))
        )
      ),
      h("main", { className: "mx-auto max-w-7xl px-3 py-4 md:px-5 md:py-6" }, children),
      h("nav", { className: "fixed inset-x-0 bottom-0 z-40 border-t border-pulse-line bg-pulse-bg/95 px-2 pb-[max(.5rem,env(safe-area-inset-bottom))] pt-2 backdrop-blur md:hidden" },
        h("div", { className: "grid grid-cols-5 gap-1" },
          TABS.slice(0, 5).map(([id, label]) => h("button", {
            key: id,
            onClick: () => setActive(id),
            className: cx("rounded-lg px-1 py-2 text-[10px] font-medium", active === id ? "bg-pulse-card text-pulse-cyan" : "text-pulse-muted")
          }, label))
        )
      )
    );
  }

  function SectionHead(props) {
    return h("div", { className: "mb-4 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between" },
      h("div", null,
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, props.kicker || "StockLens"),
        h("h2", { className: "mt-1 text-2xl font-semibold tracking-tight" }, props.title),
        props.subtitle ? h("p", { className: "mt-1 max-w-2xl text-sm text-pulse-muted" }, props.subtitle) : null
      ),
      props.actions ? h("div", { className: "flex flex-wrap gap-2" }, props.actions) : null
    );
  }

  function ResponsiveTable({ columns, rows, mobileRender }) {
    return h(React.Fragment, null,
      h("div", { className: "grid gap-3 md:hidden" }, rows.length ? rows.map((row, i) => h(Card, { key: i, className: "p-4" }, mobileRender(row, i))) : h(Empty, null, "No rows.")),
      h("div", { className: "hidden overflow-x-auto rounded-xl border border-pulse-line bg-pulse-card md:block" },
        h("table", { className: "min-w-full border-collapse text-sm" },
          h("thead", { className: "bg-pulse-panel text-left font-mono text-[10px] uppercase tracking-[0.16em] text-pulse-dim" },
            h("tr", null, columns.map(col => h("th", { key: col.key, className: "whitespace-nowrap border-b border-pulse-line px-3 py-3" }, col.label)))
          ),
          h("tbody", null, rows.length ? rows.map((row, i) =>
            h("tr", { key: i, className: "border-b border-pulse-line/60 last:border-0 hover:bg-pulse-panel/60" },
              columns.map(col => h("td", { key: col.key, className: cx("whitespace-nowrap px-3 py-3", col.className) }, col.render ? col.render(row) : row[col.key]))
            )
          ) : h("tr", null, h("td", { className: "px-3 py-4 text-pulse-muted", colSpan: columns.length }, "No rows.")))
        )
      )
    );
  }

  function Screener() {
    const [filters, setFilters] = useState({ sector: "", pe: "", cap: "", rev: "" });
    const [rows, setRows] = useState([]);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState("");

    async function run() {
      setBusy(true); setError("");
      try {
        const params = new URLSearchParams();
        if (filters.sector) params.set("sector", filters.sector);
        if (filters.pe) params.set("max_pe", filters.pe);
        if (filters.cap) params.set("min_market_cap", filters.cap);
        if (filters.rev) params.set("min_rev_growth", filters.rev);
        const data = await api(`/api/screen?${params.toString()}`);
        setRows(Array.isArray(data) ? data : (data.results || []));
      } catch (err) {
        setError(err.message);
      } finally {
        setBusy(false);
      }
    }

    useEffect(() => { run(); }, []);

    const columns = [
      { key: "ticker", label: "Ticker", render: r => h("strong", { className: "text-pulse-cyan" }, r.ticker || r.symbol) },
      { key: "name", label: "Name", render: r => h("span", { className: "text-pulse-muted" }, r.name || r.company || "-") },
      { key: "sector", label: "Sector" },
      { key: "price", label: "Price", className: "font-mono", render: r => fmtUsd(r.price || r.current_price) },
      { key: "pe", label: "P/E", className: "font-mono", render: r => r.pe_ratio || r.pe || "-" },
      { key: "market_cap", label: "Mkt Cap", className: "font-mono", render: r => r.market_cap_b ? `$${Number(r.market_cap_b).toFixed(1)}B` : (r.market_cap ? `$${Math.round(Number(r.market_cap) / 1e9)}B` : "-") },
    ];

    return h("div", null,
      h(SectionHead, { title: "Screener", kicker: "Mobile-first research", subtitle: "Filter, scan and open high-signal stocks without fighting a desktop table on your phone." }),
      h(Card, { className: "mb-4 p-4" },
        h("div", { className: "grid gap-3 sm:grid-cols-2 lg:grid-cols-5" },
          h("select", { value: filters.sector, onChange: e => setFilters(Object.assign({}, filters, { sector: e.target.value })), className: "h-11 rounded-lg border border-pulse-line bg-pulse-panel px-3 text-pulse-ink" },
            h("option", { value: "" }, "All sectors"),
            ["Technology", "Healthcare", "Financial Services", "Consumer Cyclical", "Consumer Defensive", "Energy", "Industrials", "Communication Services", "Basic Materials", "Utilities", "Real Estate"].map(s => h("option", { key: s }, s))
          ),
          h(TextInput, { placeholder: "Max P/E", value: filters.pe, onChange: e => setFilters(Object.assign({}, filters, { pe: e.target.value })) }),
          h(TextInput, { placeholder: "Min market cap $B", value: filters.cap, onChange: e => setFilters(Object.assign({}, filters, { cap: e.target.value })) }),
          h(TextInput, { placeholder: "Min revenue growth %", value: filters.rev, onChange: e => setFilters(Object.assign({}, filters, { rev: e.target.value })) }),
          h(Button, { kind: "primary", onClick: run, disabled: busy }, busy ? "Running..." : "Run screen")
        ),
        error ? h("p", { className: "mt-3 text-sm text-pulse-red" }, error) : null
      ),
      h(ResponsiveTable, {
        columns, rows,
        mobileRender: r => h("div", { className: "grid gap-3" },
          h("div", { className: "flex items-start justify-between gap-3" },
            h("div", null, h("div", { className: "font-mono text-lg text-pulse-cyan" }, r.ticker || r.symbol), h("div", { className: "text-sm text-pulse-muted" }, r.name || r.company || "-")),
            h("div", { className: "font-mono text-sm" }, fmtUsd(r.price || r.current_price))
          ),
          h("div", { className: "grid grid-cols-3 gap-2" },
            h(Metric, { label: "P/E", value: r.pe_ratio || r.pe || "-" }),
            h(Metric, { label: "Sector", value: r.sector || "-" }),
            h(Metric, { label: "Cap", value: r.market_cap_b ? `$${Number(r.market_cap_b).toFixed(1)}B` : "-" })
          )
        )
      })
    );
  }

  function Watchlist() {
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
      await api(`/api/watchlist/${encodeURIComponent(ticker.trim().toUpperCase())}`, { method: "POST" });
      setTicker(""); load();
    }
    async function remove(sym) {
      await api(`/api/watchlist/${encodeURIComponent(sym)}`, { method: "DELETE" });
      load();
    }
    useEffect(() => { load(); }, []);
    return h("div", null,
      h(SectionHead, { title: "Watchlist", subtitle: "A compact mobile card list with the same data available as a table on wider screens.", actions: [h(Button, { key: "refresh", onClick: load, disabled: busy }, "Refresh")] }),
      h(Card, { className: "mb-4 p-4" },
        h("div", { className: "flex gap-2" },
          h(TextInput, { placeholder: "Ticker", value: ticker, onChange: e => setTicker(e.target.value.toUpperCase()), onKeyDown: e => { if (e.key === "Enter") add(); } }),
          h(Button, { kind: "primary", onClick: add }, "Add")
        ),
        error ? h("p", { className: "mt-3 text-sm text-pulse-red" }, error) : null
      ),
      h(ResponsiveTable, {
        rows,
        columns: [
          { key: "ticker", label: "Ticker", render: r => h("strong", { className: "text-pulse-cyan" }, r.ticker) },
          { key: "name", label: "Name", render: r => r.name || "-" },
          { key: "price", label: "Price", className: "font-mono", render: r => fmtUsd(r.price || r.current_price) },
          { key: "change", label: "Change", className: "font-mono", render: r => fmtPct(r.change_pct || r.changePercent || 0, 2) },
          { key: "actions", label: "", render: r => h(Button, { onClick: () => remove(r.ticker), className: "min-h-8 px-2 text-xs" }, "Remove") },
        ],
        mobileRender: r => h("div", { className: "flex items-center justify-between gap-3" },
          h("div", null, h("div", { className: "font-mono text-lg text-pulse-cyan" }, r.ticker), h("div", { className: "text-sm text-pulse-muted" }, r.name || "-")),
          h("div", { className: "text-right" }, h("div", { className: "font-mono" }, fmtUsd(r.price || r.current_price)), h("div", { className: "text-xs text-pulse-muted" }, fmtPct(r.change_pct || r.changePercent || 0, 2))),
          h(Button, { onClick: () => remove(r.ticker), className: "min-h-9 px-2 text-xs" }, "Remove")
        )
      })
    );
  }

  function Predictions() {
    const [rows, setRows] = useState([]);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState("");
    const [selected, setSelected] = useState(null);
    async function load() {
      setBusy(true); setError("");
      try {
        const data = await api("/api/predictions");
        setRows(Array.isArray(data) ? data : (data.predictions || data.rows || []));
      } catch (err) { setError(err.message); } finally { setBusy(false); }
    }
    async function generate() {
      setBusy(true); setError("");
      try {
        await api("/api/predictions/generate", { method: "POST" });
        setTimeout(load, 1200);
      } catch (err) { setError(err.message); setBusy(false); }
    }
    useEffect(() => { load(); }, []);

    return h("div", null,
      h(SectionHead, { title: "Predictions", kicker: "Pulse ranking", subtitle: "Mobile cards first, dense table when there is room.", actions: [
        h(Button, { key: "refresh", onClick: load, disabled: busy }, "Refresh"),
        h(Button, { key: "gen", kind: "primary", onClick: generate, disabled: busy }, busy ? "Working..." : "Generate")
      ] }),
      error ? h(Empty, null, error) : null,
      h("div", { className: "grid gap-3 md:hidden" },
        rows.length ? rows.map((p, i) => h(PredictionCard, { key: i, p, onOpen: () => setSelected(p) })) : h(Empty, null, busy ? "Loading..." : "No predictions yet.")
      ),
      h("div", { className: "hidden overflow-x-auto rounded-xl border border-pulse-line bg-pulse-card md:block" },
        h("table", { className: "min-w-[1100px] text-sm" },
          h("thead", { className: "bg-pulse-panel font-mono text-[10px] uppercase tracking-[0.16em] text-pulse-dim" },
            h("tr", null, ["Ticker", "Score", "Factors", "3M", "6M", "12M", "Confidence", "Thesis"].map(x => h("th", { className: "px-3 py-3 text-left", key: x }, x)))
          ),
          h("tbody", null, rows.map((p, i) => h("tr", { key: i, className: "border-t border-pulse-line/70" },
            h("td", { className: "px-3 py-3" }, h("strong", { className: "font-mono text-pulse-cyan" }, p.ticker), h("div", { className: "text-xs text-pulse-muted" }, p.name || "")),
            h("td", { className: cx("px-3 py-3 font-mono", scoreTone(p.score)) }, p.score == null ? "-" : `${p.score}/100`),
            h("td", { className: "px-3 py-3" }, h(FactorCluster, { scores: p.factor_scores || {} })),
            h("td", { className: "px-3 py-3 font-mono" }, fmtPct(p.predicted_3m_pct, 1)),
            h("td", { className: "px-3 py-3 font-mono" }, fmtPct(p.predicted_6m_pct, 1)),
            h("td", { className: "px-3 py-3 font-mono" }, fmtPct(p.predicted_12m_pct, 1)),
            h("td", { className: "px-3 py-3" }, h(Confidence, { value: p.confidence, score: p.score })),
            h("td", { className: "px-3 py-3" }, h(Button, { onClick: () => setSelected(p), className: "min-h-8 px-2 text-xs" }, "View"))
          )))
        )
      ),
      selected ? h(SlideOver, { title: `${selected.ticker} prediction thesis`, onClose: () => setSelected(null) },
        h("div", { className: "grid gap-4" },
          h("div", { className: "grid grid-cols-3 gap-2" },
            h(Metric, { label: "Score", value: selected.score == null ? "-" : `${selected.score}/100`, tone: scoreTone(selected.score) }),
            h(Metric, { label: "3M", value: fmtPct(selected.predicted_3m_pct, 1) }),
            h(Metric, { label: "12M", value: fmtPct(selected.predicted_12m_pct, 1) })
          ),
          h(Card, { className: "p-4 text-sm leading-relaxed text-pulse-muted" }, selected.reasoning || "No reasoning available.")
        )
      ) : null
    );
  }

  function FactorCluster({ scores }) {
    const config = [["V", "value"], ["M", "momentum"], ["Q", "quality"], ["G", "growth"], ["C", "composite"]];
    return h("div", { className: "flex flex-wrap gap-1" },
      config.map(([label, key]) => {
        const value = scores[key];
        const tone = value == null ? "border-pulse-line text-pulse-dim" : value >= 70 ? "border-pulse-green/30 bg-pulse-green/10 text-pulse-green" : value >= 45 ? "border-pulse-amber/30 bg-pulse-amber/10 text-pulse-amber" : "border-pulse-red/30 bg-pulse-red/10 text-pulse-red";
        return h("span", { key, className: cx("inline-flex h-8 min-w-8 flex-col items-center justify-center rounded-md border font-mono text-[10px] font-bold", tone) }, label, h("small", { className: "text-[8px] opacity-80" }, value == null ? "-" : Math.round(value)));
      })
    );
  }

  function Confidence({ value, score }) {
    const label = String(value || (Number(score) >= 70 ? "high" : Number(score) >= 50 ? "medium" : "low")).toUpperCase();
    const tone = label.includes("HIGH") ? "border-pulse-green/30 bg-pulse-green/10 text-pulse-green" : label.includes("LOW") ? "border-pulse-red/30 bg-pulse-red/10 text-pulse-red" : "border-pulse-amber/30 bg-pulse-amber/10 text-pulse-amber";
    return h(Pill, { className: tone }, label);
  }

  function PredictionCard({ p, onOpen }) {
    return h(Card, { className: "p-4" },
      h("div", { className: "flex items-start justify-between gap-3" },
        h("div", { className: "min-w-0" }, h("div", { className: "font-mono text-xl text-pulse-cyan" }, p.ticker), h("div", { className: "truncate text-sm text-pulse-muted" }, p.name || "-")),
        h("div", { className: cx("font-mono text-xl", scoreTone(p.score)) }, p.score == null ? "-" : Math.round(p.score))
      ),
      h("div", { className: "mt-4" }, h(FactorCluster, { scores: p.factor_scores || {} })),
      h("div", { className: "mt-4 grid grid-cols-3 gap-2" },
        h(Metric, { label: "3M", value: fmtPct(p.predicted_3m_pct, 1) }),
        h(Metric, { label: "6M", value: fmtPct(p.predicted_6m_pct, 1) }),
        h(Metric, { label: "12M", value: fmtPct(p.predicted_12m_pct, 1) })
      ),
      h(Button, { onClick: onOpen, className: "mt-4 w-full" }, "View thesis")
    );
  }

  function SlideOver({ title, onClose, children }) {
    return h("div", { className: "fixed inset-0 z-50" },
      h("div", { className: "absolute inset-0 bg-black/70", onClick: onClose }),
      h("section", { className: "absolute inset-x-0 bottom-0 max-h-[92dvh] overflow-y-auto rounded-t-2xl border-t border-pulse-line bg-pulse-panel p-4 shadow-2xl md:inset-y-0 md:left-auto md:right-0 md:w-[560px] md:max-h-none md:rounded-none md:border-l md:border-t-0 md:p-5" },
        h("div", { className: "sticky top-0 z-10 -mx-4 -mt-4 mb-4 flex items-start justify-between gap-3 border-b border-pulse-line bg-pulse-panel/95 p-4 backdrop-blur md:-mx-5 md:-mt-5 md:p-5" },
          h("div", null, h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, "Detail"), h("h3", { className: "mt-1 text-xl font-semibold" }, title)),
          h(Button, { onClick: onClose, className: "min-h-9 px-3" }, "Close")
        ),
        children
      )
    );
  }

  function Thesis() {
    const [ticker, setTicker] = useState("CEG");
    const [runFresh, setRunFresh] = useState(false);
    const [thesis, setThesis] = useState(null);
    const [history, setHistory] = useState([]);
    const [recon, setRecon] = useState(null);
    const [busy, setBusy] = useState(false);
    const [status, setStatus] = useState("");

    async function loadLatest(sym) {
      const target = (sym || ticker).trim().toUpperCase();
      if (!target) return;
      setBusy(true); setStatus(`Loading latest thesis for ${target}...`);
      try {
        const data = await api(`/v1/thesis/${encodeURIComponent(target)}/latest`);
        setThesis(data);
        setTicker(target);
        setStatus(`Loaded ${target}.`);
        loadHistory(target);
        reconcile(data);
      } catch (err) {
        setStatus(err.message || "No thesis found.");
      } finally {
        setBusy(false);
      }
    }

    async function run() {
      const target = ticker.trim().toUpperCase();
      if (!target) return;
      setBusy(true); setStatus(`Starting agent run for ${target}...`);
      try {
        const run = await api("/v1/runs", { method: "POST", body: JSON.stringify({ tickers: [target], run_fresh: runFresh }) });
        pollRun(run.run_id, target);
      } catch (err) {
        setStatus(err.message || "Could not start run.");
        setBusy(false);
      }
    }

    async function pollRun(runId, target) {
      let attempts = 0;
      const timer = setInterval(async () => {
        attempts += 1;
        try {
          const data = await api(`/v1/runs/${encodeURIComponent(runId)}`);
          setStatus(`Run ${data.status || "running"} · completed ${(data.completed || []).length}`);
          if (["complete", "completed", "partial", "failed"].includes(data.status) || attempts > 90) {
            clearInterval(timer);
            setBusy(false);
            loadLatest(target);
          }
        } catch (err) {
          clearInterval(timer);
          setBusy(false);
          setStatus(err.message || "Run failed.");
        }
      }, 2500);
    }

    async function loadHistory(sym) {
      try {
        const data = await api(`/v1/thesis/${encodeURIComponent(sym)}/history?limit=12`);
        setHistory(data.theses || []);
      } catch {
        setHistory([]);
      }
    }

    async function loadById(id) {
      setBusy(true);
      try {
        const data = await api(`/v1/thesis/id/${encodeURIComponent(id)}`);
        setThesis(data);
        reconcile(data);
      } catch (err) {
        setStatus(err.message || "Could not load thesis.");
      } finally { setBusy(false); }
    }

    async function reconcile(t) {
      setRecon(null);
      if (!t || !t.ticker) return;
      try {
        const res = await fetch(`${PICK_SHOVELS_API}/api/reconcile/${encodeURIComponent(t.ticker)}?theme_id=ai-infra`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ stock_analysis: t })
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        setRecon(await res.json());
      } catch {
        setRecon({ unavailable: true });
      }
    }

    return h("div", null,
      h(SectionHead, { title: "Agent Thesis", kicker: "8-agent deep dive", subtitle: "Run fresh agents, view dated cached results, and reconcile the thesis against Picks-shovels theme context." }),
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
        status ? h("p", { className: "mt-3 text-sm text-pulse-muted" }, status) : null
      ),
      thesis ? h(ThesisView, { thesis, history, recon, onHistory: loadById }) : h(Empty, null, "Enter a ticker, load latest, or run agents.")
    );
  }

  function ThesisView({ thesis, history, recon, onHistory }) {
    const score = Number(thesis.composite_score || 0);
    const direction = score >= 60 ? "BULLISH" : score <= 40 ? "BEARISH" : "NEUTRAL";
    const forecast = thesis.forecast || {};
    return h("div", { className: "grid gap-4" },
      h(Card, { className: "p-4" },
        h("div", { className: "flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between" },
          h("div", null,
            h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-cyan" }, "Deep dive"),
            h("h3", { className: "mt-1 text-2xl font-semibold" }, thesis.ticker, " Analysis"),
            h("p", { className: "mt-1 text-sm text-pulse-muted" }, fmtDate(thesis.generated_at), thesis.thesis_id ? ` · thesis ${String(thesis.thesis_id).slice(0, 8)}` : "")
          ),
          h("a", { href: `/v1/thesis/${encodeURIComponent(thesis.ticker)}/export.pdf`, target: "_blank", className: "inline-flex min-h-10 items-center justify-center rounded-lg border border-pulse-line px-3 text-sm text-pulse-ink" }, "Export PDF")
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
            h(Metric, { label: "Risk", value: thesis.risk_rating || "-" }),
            h(Metric, { label: "Evidence", value: thesis.evidence_quality || "-" }),
            h(Metric, { label: "12M base", value: forecast["12m"] ? fmtPct(forecast["12m"].base_return_pct, 1) : "-" })
          )
        )
      ),
      history && history.length ? h(Card, { className: "p-4" },
        h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, "Dated results · last 12 months"),
        h("div", { className: "mt-3 flex gap-2 overflow-x-auto pb-1 scrollbar-none" },
          history.map(row => h("button", { key: row.thesis_id, onClick: () => onHistory(row.thesis_id), className: "shrink-0 rounded-lg border border-pulse-line bg-pulse-panel px-3 py-2 text-left" },
            h("div", { className: cx("font-mono text-sm", scoreTone(row.composite_score)) }, Number(row.composite_score || 0).toFixed(1)),
            h("div", { className: "text-[11px] text-pulse-muted" }, row.generated_at ? new Date(row.generated_at).toLocaleDateString() : "-")
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
        h("div", { className: "pt-2 text-xs text-pulse-dim" }, "Confidence ", forecast.confidence == null ? "-" : `${Math.round(Number(forecast.confidence) * 100)}%`, " · score ", weighted == null ? "-" : Number(weighted).toFixed(1))
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
      h("p", { className: "mt-1 text-sm text-pulse-muted" }, data.theme_data ? `Tier ${data.theme_data.tier ?? "-"} · ${data.theme_data.exposure_pct ?? "-"}% exposure · using displayed Stock Picker thesis` : ""),
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

  function Recommendations() {
    const [data, setData] = useState(null);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState("");
    async function run() {
      setBusy(true); setError("");
      try {
        const start = await api("/api/recommendations/start", { method: "POST" });
        const id = start.job_id || start.id;
        if (id) {
          setTimeout(async () => {
            try { setData(await api(`/api/recommendations/progress/${id}`)); } finally { setBusy(false); }
          }, 1500);
        } else {
          setData(start); setBusy(false);
        }
      } catch (err) { setError(err.message); setBusy(false); }
    }
    const buys = data?.buy || data?.buys || data?.recommendations || [];
    const sells = data?.sell || data?.sells || [];
    return h("div", null,
      h(SectionHead, { title: "Signals", subtitle: "Recommendation engine output, converted to mobile cards first.", actions: [h(Button, { key: "run", kind: "primary", onClick: run, disabled: busy }, busy ? "Running..." : "Generate signals")] }),
      error ? h(Empty, null, error) : null,
      h("div", { className: "grid gap-3 md:grid-cols-2" },
        h(SignalList, { title: "Buy candidates", rows: buys }),
        h(SignalList, { title: "Sell signals", rows: sells })
      )
    );
  }

  function SignalList({ title, rows }) {
    const list = Array.isArray(rows) ? rows : [];
    return h(Card, { className: "p-4" },
      h("div", { className: "font-mono text-[10px] uppercase tracking-[0.24em] text-pulse-dim" }, title),
      h("div", { className: "mt-3 grid gap-3" }, list.length ? list.map((r, i) => h("div", { key: i, className: "rounded-lg border border-pulse-line bg-pulse-panel p-3" },
        h("div", { className: "flex items-start justify-between gap-3" }, h("strong", { className: "font-mono text-pulse-cyan" }, r.ticker || r.symbol || "-"), h("span", { className: "font-mono text-sm" }, r.signal || r.recommendation || "")),
        h("p", { className: "mt-2 text-sm text-pulse-muted" }, r.reasoning || r.reason || r.narrative || "")
      )) : h("p", { className: "text-sm text-pulse-muted" }, "No rows yet."))
    );
  }

  function SimpleEndpointTab({ title, subtitle, endpoint, render }) {
    const [data, setData] = useState(null);
    const [error, setError] = useState("");
    const [busy, setBusy] = useState(false);
    async function load() {
      setBusy(true); setError("");
      try { setData(await api(endpoint)); } catch (err) { setError(err.message); } finally { setBusy(false); }
    }
    useEffect(() => { load(); }, [endpoint]);
    return h("div", null,
      h(SectionHead, { title, subtitle, actions: [h(Button, { key: "refresh", onClick: load, disabled: busy }, "Refresh")] }),
      error ? h(Empty, null, error) : data ? render(data) : h(Empty, null, busy ? "Loading..." : "No data.")
    );
  }

  function JsonCards({ data }) {
    if (Array.isArray(data)) {
      return h("div", { className: "grid gap-3 md:grid-cols-2 xl:grid-cols-3" }, data.map((item, i) => h(Card, { key: i, className: "p-4" }, h("pre", { className: "whitespace-pre-wrap break-words text-xs text-pulse-muted" }, JSON.stringify(item, null, 2)))));
    }
    return h(Card, { className: "p-4" }, h("pre", { className: "max-h-[70dvh] overflow-auto whitespace-pre-wrap break-words text-xs text-pulse-muted" }, JSON.stringify(data, null, 2)));
  }

  function App() {
    const [ready, setReady] = useState(false);
    const [user, setUser] = useState("");
    const [active, setActive] = useState("thesis");

    async function check() {
      if (!token()) { setReady(true); return; }
      try {
        const data = await api("/api/auth/me");
        setUser(data.username || "user");
      } catch {
        setToken("");
      } finally {
        setReady(true);
      }
    }

    useEffect(() => { check(); }, []);
    if (!ready) return h("div", { className: "flex min-h-screen items-center justify-center text-pulse-muted" }, "Loading StockLens...");
    if (!token()) return h(Login, { onLogin: check });

    const tabs = {
      screener: h(Screener),
      watchlist: h(Watchlist),
      predictions: h(Predictions),
      thesis: h(Thesis),
      recommendations: h(Recommendations),
      portfolio: h(SimpleEndpointTab, { title: "Portfolio", subtitle: "Holdings rendered as mobile-first JSON cards while the v2 portfolio editor is completed.", endpoint: "/api/portfolio", render: data => h(JsonCards, { data }) }),
      paper: h(SimpleEndpointTab, { title: "Paper P&L", subtitle: "Paper trading state and trade history.", endpoint: "/api/paper-portfolio", render: data => h(JsonCards, { data }) }),
      alerts: h(SimpleEndpointTab, { title: "Alerts", subtitle: "Alert history and notification status.", endpoint: "/api/alerts", render: data => h(JsonCards, { data }) }),
      sentiment: h(SimpleEndpointTab, { title: "Sentiment", subtitle: "Current watchlist sentiment snapshot.", endpoint: "/api/sentiment?watchlist=true", render: data => h(JsonCards, { data }) }),
      ops: h(SimpleEndpointTab, { title: "Operations", subtitle: "Agent pipeline health and scheduler status.", endpoint: "/v1/operations/status", render: data => h(JsonCards, { data }) }),
    };

    return h(Shell, { user, active, setActive, logout: () => { setToken(""); setUser(""); } }, tabs[active] || tabs.thesis);
  }

  ReactDOM.createRoot(document.getElementById("root")).render(h(App));
})();
