from fastapi.responses import HTMLResponse


def get_ui_html() -> str:
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>dataIDE — mini UI</title>
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <style>
    :root { --bg:#0f1117; --card:#151924; --text:#e6e6e6; --muted:#a0a4b8; --accent:#6ee7b7; --danger:#ff6b6b; }
    html,body { margin:0; padding:0; background:var(--bg); color:var(--text); font:14px/1.45 system-ui, -apple-system, Segoe UI, Roboto, Arial; }
    .wrap { max-width:1100px; margin:32px auto; padding:0 16px; }
    h1 { font-size:22px; margin:0 0 16px; }
    .card { background:var(--card); border-radius:14px; padding:16px; box-shadow:0 4px 18px rgba(0,0,0,.25); margin-bottom:16px; }
    textarea { width:100%; min-height:120px; resize:vertical; border-radius:10px; border:1px solid #2a3142; padding:12px; color:var(--text); background:#0f1320; }
    button, a.button { display:inline-flex; align-items:center; gap:8px; border:0; border-radius:12px; padding:10px 14px; background:var(--accent); color:#08130e; font-weight:600; cursor:pointer; }
    button:disabled, a.button.disabled { opacity:.6; cursor:not-allowed; }
    .row { display:flex; gap:16px; flex-wrap:wrap; }
    .col { flex:1 1 320px; }
    pre, code { white-space:pre-wrap; word-break:break-word; }
    .muted { color:var(--muted); }
    details { background:#0e1220; border-radius:10px; padding:8px 12px; margin:8px 0; }
    .err { color:var(--danger); font-weight:600; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    #erd svg { width:100%; height:auto; background:white; border-radius:10px; }
  </style>

  <!-- Robust Mermaid loader with fallback -->
  <script>
    (function loadMermaid(){
      function init(){ try { if (window.mermaid) window.mermaid.initialize({ startOnLoad:false, securityLevel:"loose" }); } catch(e){ console.warn("Mermaid init failed:", e); } }
      const s=document.createElement("script");
      s.src="https://unpkg.com/mermaid@10/dist/mermaid.min.js";
      s.onload=init;
      s.onerror=function(){
        const s2=document.createElement("script");
        s2.src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js";
        s2.onload=init;
        document.head.appendChild(s2);
      };
      document.head.appendChild(s);
    })();
  </script>
</head>
<body>
  <div class="wrap">
    <h1>dataIDE — mini UI</h1>

    <div class="card">
      <label for="prompt" class="muted">Prompt</label>
      <textarea id="prompt" placeholder="Describe the dataset you want (entities, relationships, units, geography…). Example: Create a customer–orders dataset for a UK D2C coffee brand including customers, orders, products, payments, shipments."></textarea>
      <div style="display:flex; gap:10px; margin-top:10px; flex-wrap:wrap;">
        <button id="genBtn">Generate</button>
        <button id="dlAll" disabled>Download All</button>
        <button id="dlBtn" disabled>Download samples.zip</button>
        <button id="dlJson" disabled>Profiling JSON</button>
        <button id="dlCsv" disabled>Profiling CSV</button>
        <span id="status" class="muted" style="align-self:center;"></span>
      </div>
    </div>

    <div class="row">
      <div class="col">
        <div class="card">
          <h3 style="margin-top:0;">Dataset</h3>
          <div id="desc" class="muted">No result yet.</div>
          <div id="tables"></div>
        </div>
        <div class="card">
          <h3 style="margin-top:0;">JSON (full payload)</h3>
          <pre id="jsonOut" class="mono muted" style="max-height:360px; overflow:auto;">—</pre>
        </div>
      </div>
      <div class="col">
        <div class="card">
          <h3 style="margin-top:0;">ERD</h3>
          <div id="erd"><div class="muted">Will render after Generate.</div></div>
        </div>
        <div class="card">
          <h3 style="margin-top:0;">Profiling</h3>
          <pre id="profile" class="mono muted">—</pre>
        </div>
      </div>
    </div>

    <div class="card">
      <h3 style="margin-top:0;">Charts</h3>
      <div id="charts" class="row"></div>
    </div>

    <div class="card">
      <h3 style="margin-top:0;">Correlation Heatmaps</h3>
      <div id="corrs" class="row"></div>
    </div>
  </div>

<script>
  const $ = (id) => document.getElementById(id);
  const genBtn   = $("genBtn");
  const dlAll   = $("dlAll");
  const dlBtn    = $("dlBtn");
  const dlJson   = $("dlJson");
  const dlCsv    = $("dlCsv");
  const promptEl = $("prompt");
  const status   = $("status");
  const desc     = $("desc");
  const tablesEl = $("tables");
  const jsonOut  = $("jsonOut");
  const profile  = $("profile");
  const erdEl    = $("erd");
  const chartsEl = $("charts");
  const corrsEl  = $("corrs");

  function renderTables(tables) {
    tablesEl.innerHTML = "";
    if (!tables || !tables.length) { tablesEl.innerHTML = "<div class='muted'>No tables.</div>"; return; }
    for (const t of tables) {
      const d = document.createElement("details");
      d.open = false;
      d.innerHTML = `<summary><b>${t.name}</b>${t.description ? " — " + t.description : ""}</summary>`;
      const pk = t.primary_key ? `<div><b>Primary key:</b> ${t.primary_key.join(", ")}</div>` : "";
      const fk = (t.foreign_keys && t.foreign_keys.length)
        ? `<div><b>Foreign keys:</b> ${t.foreign_keys.map(f => f.column + " → " + f.ref).join(", ")}</div>` : "";
      const head = "<thead><tr><th align='left'>Column</th><th align='left'>Type</th><th align='left'>Nullable</th><th align='left'>PII</th><th align='left'>Semantics</th><th align='left'>Description</th></tr></thead>";
      const body = `<tbody>${
        t.columns.map(c =>
          `<tr><td>${c.name}</td><td>${c.dtype}</td><td>${c.nullable?"yes":"no"}</td><td>${c.pii||"none"}</td><td>${c.semantics||""}</td><td>${c.description||""}</td></tr>`
        ).join("")
      }</tbody>`;
      const tbl = `<table class="mono" style="width:100%;border-spacing:0 6px;">${head}${body}</table>`;
      d.insertAdjacentHTML("beforeend", pk + fk + tbl);
      tablesEl.appendChild(d);
    }
  }

  async function renderMermaid(code) {
    if (!window.mermaid || !window.mermaid.render) {
      erdEl.innerHTML = "<div class='err'>Mermaid not available. Showing ERD source instead.</div><pre class='mono'>" + code.replace(/</g,"&lt;") + "</pre>";
      return;
    }
    try {
      const { svg } = await window.mermaid.render("erdGraph", code);
      erdEl.innerHTML = svg;
    } catch (e) {
      erdEl.innerHTML = "<div class='err'>Mermaid failed. Showing ERD source.</div><pre class='mono'>" + code.replace(/</g,"&lt;") + "</pre>";
    }
  }

  function imgPlotURL(p, c) {
    const q = new URLSearchParams({ prompt:p, table:c.table, kind:c.kind, x:c.x || "", ts:String(Date.now()) });
    if (c.y) q.append("y", c.y);
    return "/api/plot.png?" + q.toString();
  }

  function renderCharts(prompt, charts) {
    chartsEl.innerHTML = "";
    if (!charts || charts.length === 0) { chartsEl.innerHTML = "<div class='muted'>No chart suggestions.</div>"; return; }
    charts.slice(0,6).forEach(c => {
      const wrap = document.createElement("div"); wrap.className = "col";
      const card = document.createElement("div"); card.className = "card";
      const title = document.createElement("div"); title.innerHTML = `<b>${c.title}</b> <span class='muted'>(${c.kind})</span>`;
      const img = document.createElement("img");
      img.src = imgPlotURL(prompt, c); img.alt = c.title; img.style.width = "100%"; img.style.borderRadius = "10px";
      card.appendChild(title); card.appendChild(img); wrap.appendChild(card); chartsEl.appendChild(wrap);
    });
  }

  function renderCorrs(prompt, profile) {
    corrsEl.innerHTML = "";
    const tables = Object.keys(profile || {});
    let any = false;
    tables.forEach(t => {
      const tinfo = profile[t] || {};
      if (tinfo.correlation_pearson && Object.keys(tinfo.correlation_pearson).length >= 2) {
        any = true;
        const wrap = document.createElement("div"); wrap.className = "col";
        const card = document.createElement("div"); card.className = "card";
        const h = document.createElement("div"); h.innerHTML = "<b>" + t + "</b>";
        const img = document.createElement("img");
        img.src = "/api/corr.png?" + new URLSearchParams({ prompt: prompt, table: t, ts: String(Date.now()) }).toString();
        img.alt = t + " correlation"; img.style.width = "100%"; img.style.borderRadius = "10px";
        card.appendChild(h); card.appendChild(img); wrap.appendChild(card); corrsEl.appendChild(wrap);
      }
    });
    if (!any) corrsEl.innerHTML = "<div class='muted'>No tables with enough numeric columns for correlation.</div>";
  }

  async function generate() {
    const p = promptEl.value.trim();
    if (!p) { alert("Enter a prompt first."); return; }

    genBtn.disabled = true;
    dlAll.disabled = true;
    dlBtn.disabled = dlJson.disabled = dlCsv.disabled = true;
    status.textContent = "Generating…";
    desc.textContent = "—"; tablesEl.innerHTML = ""; jsonOut.textContent = "—"; profile.textContent = "—";
    erdEl.innerHTML = "<div class='muted'>Rendering…</div>"; chartsEl.innerHTML = ""; corrsEl.innerHTML = "";

    const body = new URLSearchParams(); body.append("prompt", p);

    try {
      const r = await fetch("/api/generate", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body
      });
      if (!r.ok) throw new Error("HTTP " + r.status);
      const payload = await r.json();

      desc.textContent = payload.dataset_description || "(no description)";
      renderTables(payload.tables || []);
      jsonOut.textContent = JSON.stringify(payload, null, 2);
      if (Array.isArray(payload.caveats) && payload.caveats.length) {
        const warn = document.createElement("div");
        warn.className = "err";
        warn.textContent = "Note: " + payload.caveats.join(" | ");
        desc.prepend(warn);
      }

      profile.textContent = JSON.stringify(payload.profiling_summary || {}, null, 2);
      try { await renderMermaid(payload.mermaid_erd || "erDiagram\n"); } catch(e){ erdEl.innerHTML = "<div class='err'>ERD render failed.</div>"; }

      dlAll.onclick = async () => {
        const resp = await fetch("/api/export.zip", { method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body });
        if (!resp.ok) { alert("Download failed: HTTP " + resp.status); return; }
        const blob = await resp.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url; a.download = "dataIDE_export.zip";
        document.body.appendChild(a); a.click(); a.remove();
        URL.revokeObjectURL(url);
      };
      dlAll.disabled = false;

      dlBtn.onclick  = async () => {
        const resp = await fetch("/api/samples.zip", { method:"POST", headers:{ "Content-Type":"application/x-www-form-urlencoded" }, body });
        if (!resp.ok) return alert("Download failed: HTTP " + resp.status);
        const blob = await resp.blob(), url = URL.createObjectURL(blob);
        const a = document.createElement("a"); a.href = url; a.download = "samples.zip"; document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
      };
      dlJson.onclick = () => window.location.href = "/api/profile.json?" + new URLSearchParams({prompt:p});
      dlCsv.onclick  = () => window.location.href = "/api/profile.csv?"  + new URLSearchParams({prompt:p});
      dlBtn.disabled = dlJson.disabled = dlCsv.disabled = false;

      try { renderCharts(p, payload.suggested_charts || []); } catch(e){ chartsEl.innerHTML = "<div class='err'>Charts failed.</div>"; }
      try { renderCorrs(p, payload.profiling_summary || {}); } catch(e){ corrsEl.innerHTML  = "<div class='err'>Heatmaps failed.</div>"; }

      status.textContent = "Done.";
    } catch (err) {
      status.textContent = "";
      alert("Generate failed: " + err.message);
    } finally {
      genBtn.disabled = false;
    }
  }

  genBtn.addEventListener("click", generate);
</script>
</body>
</html>
    """


def ui_response() -> HTMLResponse:
    return HTMLResponse(get_ui_html())


