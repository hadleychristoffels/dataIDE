import io, os, csv, zipfile, json, random, string
from typing import List, Dict, Optional
from datetime import datetime

from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse, Response
from pydantic import BaseModel

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")  # headless rendering
import matplotlib.pyplot as plt


# =========================
# Pydantic schemas (typed)
# =========================
class ColumnMeta(BaseModel):
    name: str
    dtype: str
    description: Optional[str] = None
    nullable: bool = True
    pii: Optional[str] = "none"          # none | low | moderate | high
    semantics: Optional[str] = None      # e.g., "ISO-8601 date", "GBP currency"


class TableSchema(BaseModel):
    name: str
    description: Optional[str] = None
    primary_key: Optional[List[str]] = None
    foreign_keys: Optional[List[Dict[str, str]]] = None  # {"column":"customer_id","ref":"customers.id"}
    columns: List[ColumnMeta]


class SuggestedChart(BaseModel):
    title: str
    kind: str                  # hist | bar | line | scatter | box
    table: str
    x: str
    y: Optional[str] = None
    note: Optional[str] = None


class DataIDEPayload(BaseModel):
    dataset_description: str
    tables: List[TableSchema]
    mermaid_erd: str           # Mermaid erDiagram block
    sample_rows: Dict[str, List[Dict[str, object]]]
    profiling_summary: Dict[str, Dict[str, object]]   # per-table stats
    suggested_charts: List[SuggestedChart]
    caveats: Optional[List[str]] = None


# =========================
# App
# =========================
app = FastAPI(title="dataIDE")


# =========================
# Inline UI (Mermaid + mini app)
# =========================
@app.get("/", response_class=HTMLResponse)
def ui():
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
  // --- element refs ---
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

  // --- render helpers ---
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
      // Show caveats (e.g., backend fallback reasons) at the top of Dataset box
        if (Array.isArray(payload.caveats) && payload.caveats.length) {
          const warn = document.createElement("div");
          warn.className = "err";
          warn.textContent = "Note: " + payload.caveats.join(" | ");
          desc.prepend(warn);
        }

      profile.textContent = JSON.stringify(payload.profiling_summary || {}, null, 2);
      try { await renderMermaid(payload.mermaid_erd || "erDiagram\\n"); } catch(e){ erdEl.innerHTML = "<div class='err'>ERD render failed.</div>"; }

      // downloads
      // one-click export zip
        dlAll.onclick = async () => {
          const resp = await fetch("/api/export.zip", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body
          });
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

      // visuals
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


# =========================
# Helpers
# =========================
def _zip_tables(sample_rows: Dict[str, List[dict]]) -> bytes:
    """Return a ZIP (bytes) of CSV files, one per table."""
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        for table, rows in sample_rows.items():
            if not rows:
                continue
            cols = list(rows[0].keys())
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow(r)
            zf.writestr(f"{table}.csv", buf.getvalue())
    mem.seek(0)
    return mem.read()


def _to_df(rows: List[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _coerce_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _coerce_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=False, infer_datetime_format=True)


def compute_profile(sample_rows: Dict[str, List[dict]], tables: List[TableSchema]) -> Dict[str, Dict]:
    profile: Dict[str, Dict] = {}
    schema_by_table = {t.name: t for t in tables}

    for tname, rows in sample_rows.items():
        df = _to_df(rows)
        info: Dict[str, Dict] = {"row_count": int(len(df)), "columns": {}}
        t_schema = schema_by_table.get(tname)

        if len(df) == 0:
            profile[tname] = info
            continue

        for col in df.columns:
            series = df[col]
            nn = int(series.notna().sum())
            stats = {
                "non_null": nn,
                "nulls": int(len(df) - nn),
                "null_pct": float(0 if len(df) == 0 else (len(df) - nn) / max(1, len(df))),
                "distinct": int(series.nunique(dropna=True)),
            }

            hint = None
            if t_schema:
                for c in t_schema.columns:
                    if c.name == col:
                        hint = (c.dtype or "").lower()
                        break

            # numeric summary
            if hint in {"int", "integer", "bigint", "float", "double", "decimal", "number"} or pd.api.types.is_numeric_dtype(series):
                num = _coerce_numeric(series)
                if num.notna().any():
                    stats.update({
                        "min": float(num.min()),
                        "p25": float(num.quantile(0.25)),
                        "p50": float(num.quantile(0.50)),
                        "p75": float(num.quantile(0.75)),
                        "max": float(num.max()),
                        "mean": float(num.mean()),
                        "std": float(num.std(ddof=0)),
                    })

            # date range
            if (hint and "date" in hint) or ("date" in col.lower()):
                dt = _coerce_datetime(series)
                if dt.notna().any():
                    stats.update({
                        "min_date": str(dt.min().date()),
                        "max_date": str(dt.max().date()),
                    })

            # top categories (low cardinality)
            if series.dtype == "object":
                vc = series.astype(str).value_counts(dropna=True).head(5)
                if len(vc) > 0:
                    stats["top_values"] = {k: int(v) for k, v in vc.to_dict().items()}

            info["columns"][col] = stats

        # quick numeric correlation (if any)
        df_num = pd.DataFrame()
        for c in df.columns:
            num = _coerce_numeric(df[c])
            if num.notna().any():
                df_num[c] = num
        if df_num.shape[1] >= 2:
            info["correlation_pearson"] = df_num.corr(numeric_only=True).round(3).to_dict()

        profile[tname] = info
    return profile


def suggest_charts(sample_rows: Dict[str, List[dict]]) -> List[SuggestedChart]:
    charts: List[SuggestedChart] = []
    for tname, rows in sample_rows.items():
        if not rows:
            continue
        df = _to_df(rows)

        # first numeric for hist
        for col in df.columns:
            if _coerce_numeric(df[col]).notna().any():
                charts.append(SuggestedChart(
                    title=f"{tname}.{col} — distribution",
                    kind="hist", table=tname, x=col))
                break

        # first low-card categorical for bar
        for col in df.columns:
            if df[col].dtype == "object" and df[col].astype(str).nunique() <= 30:
                charts.append(SuggestedChart(
                    title=f"{tname}.{col} — top categories",
                    kind="bar", table=tname, x=col))
                break

        # date + numeric -> line
        date_col = next((c for c in df.columns if "date" in c.lower() and _coerce_datetime(df[c]).notna().any()), None)
        num_col = next((c for c in df.columns if _coerce_numeric(df[c]).notna().any()), None)
        if date_col and num_col:
            charts.append(SuggestedChart(
                title=f"{tname}: {num_col} over {date_col}",
                kind="line", table=tname, x=date_col, y=num_col))

    return charts[:6]


def profile_to_rows(profile: Dict[str, Dict]) -> List[Dict]:
    rows: List[Dict] = []
    for tname, tinfo in profile.items():
        rows.append({"level": "table", "table": tname, "column": "", "metric": "row_count", "value": tinfo.get("row_count", 0)})
        for col, cstats in tinfo.get("columns", {}).items():
            for k, v in cstats.items():
                rows.append({"level": "column", "table": tname, "column": col, "metric": k, "value": v})
        corr = tinfo.get("correlation_pearson")
        if corr:
            cols = list(corr.keys())
            for i, a in enumerate(cols):
                for j, b in enumerate(cols):
                    if j <= i:
                        continue
                    val = corr.get(a, {}).get(b)
                    if val is not None:
                        rows.append({"level": "correlation", "table": tname, "column": "", "metric": f"{a}~{b}", "value": val})
    return rows

def build_export_zip(prompt: str, payload: DataIDEPayload) -> bytes:
    """Create a single zip with samples, payload.json, erd.mmd, profiling.json/csv, README."""
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        # samples/*.csv
        for table, rows in payload.sample_rows.items():
            if not rows:
                continue
            cols = list(rows[0].keys())
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow(r)
            zf.writestr(f"samples/{table}.csv", buf.getvalue())

        # payload.json
        zf.writestr("payload.json", json.dumps(payload.model_dump(), indent=2))

        # ERD
        zf.writestr("erd.mmd", payload.mermaid_erd)

        # profiling (json + csv)
        prof = payload.profiling_summary or compute_profile(payload.sample_rows, payload.tables)
        zf.writestr("profiling.json", json.dumps(prof, indent=2))
        rows = profile_to_rows(prof)
        df = pd.DataFrame(rows, columns=["level", "table", "column", "metric", "value"])
        zf.writestr("profiling.csv", df.to_csv(index=False))

        # README
        readme = (
            "dataIDE export\\n"
            f"Generated: {datetime.utcnow().isoformat()}Z\\n"
            f"Prompt: {prompt}\\n\\n"
            "Files:\\n"
            "- samples/*.csv — sample rows per table\\n"
            "- payload.json — full structured payload\\n"
            "- erd.mmd — Mermaid ERD\\n"
            "- profiling.json — computed profiling summary\\n"
            "- profiling.csv — flattened profiling table\\n"
        )
        zf.writestr("README.txt", readme)

    mem.seek(0)
    return mem.read()

# =========================
# Synthetic payload (MVP)
# =========================
def _synthetic_payload(user_prompt: str) -> DataIDEPayload:
    """Deterministic tiny dataset so you can test end-to-end without an API key."""
    random.seed(42)

    def rid(prefix="C", n=8):
        return prefix + "".join(random.choices(string.digits, k=n))

    customers = [
        {
            "customer_id": rid("C"),
            "email": f"user{i}@example.com",
            "country": "GB",
            "signup_date": f"2024-0{(i%9)+1}-0{(i%27)+1}"
        }
        for i in range(20)
    ]
    orders = [
        {
            "order_id": rid("O"),
            "customer_id": customers[i % 20]["customer_id"],
            "order_date": f"2025-0{(i%9)+1}-1{(i%9)}",
            "quantity": (i % 5) + 1,
            "amount_gbp": round(5 + random.random() * 95, 2)
        }
        for i in range(40)
    ]

    tables = [
        TableSchema(
            name="customers",
            description="Registered customers",
            primary_key=["customer_id"],
            columns=[
                ColumnMeta(name="customer_id", dtype="string", description="Unique customer identifier", nullable=False),
                ColumnMeta(name="email", dtype="string", description="Contact email", pii="moderate"),
                ColumnMeta(name="country", dtype="string", description="ISO country code"),
                ColumnMeta(name="signup_date", dtype="date", semantics="ISO-8601 date"),
            ],
        ),
        TableSchema(
            name="orders",
            description="Customer orders in GBP",
            primary_key=["order_id"],
            foreign_keys=[{"column": "customer_id", "ref": "customers.customer_id"}],
            columns=[
                ColumnMeta(name="order_id", dtype="string", nullable=False),
                ColumnMeta(name="customer_id", dtype="string", nullable=False),
                ColumnMeta(name="order_date", dtype="date"),
                ColumnMeta(name="quantity", dtype="int"),
                ColumnMeta(name="amount_gbp", dtype="decimal", semantics="GBP currency"),
            ],
        ),
    ]

    mermaid = """erDiagram
    customers ||--o{ orders : places
    customers {
      string customer_id PK
      string email
      string country
      date signup_date
    }
    orders {
      string order_id PK
      string customer_id FK
      date order_date
      int quantity
      decimal amount_gbp
    }"""

    profiling = compute_profile({"customers": customers, "orders": orders}, tables)
    charts = suggest_charts({"customers": customers, "orders": orders})

    return DataIDEPayload(
        dataset_description=f"Synthetic dataset for: {user_prompt}. UK D2C example with customers and orders.",
        tables=tables,
        mermaid_erd=mermaid,
        sample_rows={"customers": customers, "orders": orders},
        profiling_summary=profiling,
        suggested_charts=charts,
        caveats=["Synthetic sample for plumbing. Set OPENAI_API_KEY to use GPT-5 for real generation."],
    )


# =========================
# GPT-5 (optional) strict JSON
# =========================
def _maybe_gpt5_payload(user_prompt: str) -> DataIDEPayload:
    """
    If OPENAI_API_KEY is set, call GPT-5 to emit strict JSON matching DataIDEPayload.
    Otherwise fall back to synthetic.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return _synthetic_payload(user_prompt)

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        schema = DataIDEPayload.model_json_schema()
        system = (
            "You are dataIDE. Return ONLY JSON that validates against the provided JSON schema. "
            "Use Mermaid ERD syntax in 'mermaid_erd'. Provide realistic small samples (<= 50 rows/table). "
            "Prefer ISO-8601 dates; use GBP if UK context is implied."
        )

        resp = client.chat.completions.create(
            model="gpt-5",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_schema", "json_schema": schema},
        )
        data = json.loads(resp.choices[0].message.content)
        payload = DataIDEPayload.model_validate(data)

        # Enrich with deterministic profiling & fallback charts if missing
        payload.profiling_summary = compute_profile(payload.sample_rows, payload.tables)
        if not payload.suggested_charts:
            payload.suggested_charts = suggest_charts(payload.sample_rows)
        return payload

    except Exception as e:
        print("GPT-5 call failed, falling back to synthetic:", e)
        return _synthetic_payload(user_prompt)


# =========================
# Endpoints
# =========================
@app.get("/api/ping")
def ping():
    return {"ok": True}


@app.post("/api/generate")
def generate(prompt: str = Form(...)):
    try:
        print(f"[dataIDE] /api/generate prompt_len={len(prompt)}")
        payload = _maybe_gpt5_payload(prompt)
        # Ensure profiling & charts exist
        if not payload.profiling_summary:
            payload.profiling_summary = compute_profile(payload.sample_rows, payload.tables)
        if not payload.suggested_charts:
            payload.suggested_charts = suggest_charts(payload.sample_rows)
        return JSONResponse(payload.model_dump())
    except Exception as e:
        # Log and serve a safe fallback so the UI never 500s
        print("ERROR in /api/generate:", repr(e))
        fb = _synthetic_payload(prompt)
        fb.caveats = (fb.caveats or []) + [f"Fallback used due to error in /api/generate: {e}"]
        return JSONResponse(fb.model_dump())


@app.post("/api/samples.zip")
def samples_zip(prompt: str = Form(...)):
    payload = _maybe_gpt5_payload(prompt)
    zip_bytes = _zip_tables(payload.sample_rows)
    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=samples.zip"},
    )


@app.get("/api/profile.json")
def profile_json(prompt: str):
    payload = _maybe_gpt5_payload(prompt)
    prof = compute_profile(payload.sample_rows, payload.tables)
    return Response(
        json.dumps(prof, indent=2),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=profiling.json"}
    )


@app.get("/api/profile.csv")
def profile_csv(prompt: str):
    payload = _maybe_gpt5_payload(prompt)
    prof = compute_profile(payload.sample_rows, payload.tables)
    rows = profile_to_rows(prof)
    df = pd.DataFrame(rows, columns=["level", "table", "column", "metric", "value"])
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return Response(
        csv_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=profiling.csv"}
    )


@app.get("/api/plot.png")
def plot_png(prompt: str, table: str, kind: str = "hist", x: str = "", y: Optional[str] = None):
    """Return a PNG plot; if anything fails, render a text image explaining why."""
    payload = _maybe_gpt5_payload(prompt)
    rows = payload.sample_rows.get(table, [])
    fig, ax = plt.subplots(figsize=(9, 4.5))  # wider so bars are visible
    try:
        if not rows:
            raise ValueError(f"No rows for table '{table}'")

        df = pd.DataFrame(rows)
        if not x or x not in df.columns:
            # choose a sensible default
            x = next((c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])), df.columns[0])

        if kind == "hist":
            s = pd.to_numeric(df[x], errors="coerce").dropna()
            if s.empty: raise ValueError(f"Column '{x}' has no numeric data")
            s.plot(kind="hist", ax=ax)
            ax.set_ylabel("Frequency")

        elif kind == "bar":
            vc = df[x].astype(str).value_counts().sort_values(ascending=False).head(20)
            if vc.empty: raise ValueError(f"No categorical values to plot for '{x}'")
            vc.plot(kind="bar", ax=ax)
            ax.set_ylabel("Count")
            for label in ax.get_xticklabels():
                label.set_rotation(70)
                label.set_horizontalalignment("right")

        elif kind == "line":
            dx = pd.to_datetime(df[x], errors="coerce")
            if y:
                sy = pd.to_numeric(df[y], errors="coerce")
                line_df = pd.DataFrame({x: dx, y: sy}).dropna().sort_values(x)
                if line_df.empty: raise ValueError("No valid date/number pairs")
                line_df.set_index(x)[y].plot(kind="line", ax=ax)
                ax.set_ylabel(y)
            else:
                dx.value_counts().sort_index().plot(kind="line", ax=ax)
                ax.set_ylabel("Count")
            for label in ax.get_xticklabels():
                label.set_rotation(30)
                label.set_horizontalalignment("right")

        elif kind == "scatter" and y:
            sx = pd.to_numeric(df[x], errors="coerce")
            sy = pd.to_numeric(df[y], errors="coerce")
            sc = pd.DataFrame({x: sx, y: sy}).dropna()
            if sc.empty: raise ValueError("No numeric pairs for scatter")
            sc.plot(kind="scatter", x=x, y=y, ax=ax)

        elif kind == "box":
            s = pd.to_numeric(df[x], errors="coerce").dropna()
            if s.empty: raise ValueError(f"No numeric data for boxplot '{x}'")
            s.plot(kind="box", ax=ax)

        else:
            vc = df[x].astype(str).value_counts().head(20)
            vc.plot(kind="bar", ax=ax)
            ax.set_ylabel("Count")

        ax.set_title(f"{table}.{x} ({kind})")
        ax.grid(True, axis="y", alpha=0.2)
        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png", bbox_inches="tight")
        return Response(buf.getvalue(), media_type="image/png")
    except Exception as e:
        fig.clf()
        fig, ax = plt.subplots(figsize=(8, 2.5))
        ax.axis("off")
        ax.text(0.01, 0.5, f"Plot error: {e}", fontsize=12, va="center", ha="left", color="red")
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        return Response(buf.getvalue(), media_type="image/png")
    finally:
        plt.close(fig)


@app.get("/api/corr.png")
def corr_png(prompt: str, table: str):
    payload = _maybe_gpt5_payload(prompt)
    rows = payload.sample_rows.get(table, [])
    fig, ax = plt.subplots(figsize=(6.5, 5))
    try:
        if not rows:
            raise ValueError(f"No rows for table '{table}'")
        df = pd.DataFrame(rows)

        # numeric-only
        df_num = pd.DataFrame()
        for c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            if s.notna().any():
                df_num[c] = s

        if df_num.shape[1] < 2:
            raise ValueError("Not enough numeric columns for correlation")

        corr = df_num.corr(numeric_only=True)
        im = ax.imshow(corr, interpolation="nearest")   # default cmap
        ax.set_xticks(range(len(corr.columns)))
        ax.set_yticks(range(len(corr.columns)))
        ax.set_xticklabels(corr.columns, rotation=45, ha="right")
        ax.set_yticklabels(corr.columns)
        ax.set_title(f"{table} — Pearson correlation")
        for (i, j), val in np.ndenumerate(corr.values):
            ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=8)
        fig.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        return Response(buf.getvalue(), media_type="image/png")
    except Exception as e:
        fig.clf(); fig, ax = plt.subplots(figsize=(8, 2.5)); ax.axis("off")
        ax.text(0.02, 0.5, f"Heatmap error: {e}", fontsize=12, va="center", ha="left", color="red")
        buf = io.BytesIO(); fig.savefig(buf, format="png", bbox_inches="tight")
        return Response(buf.getvalue(), media_type="image/png")
    finally:
        plt.close(fig)

@app.post("/api/export.zip")
def export_all(prompt: str = Form(...)):
    payload = _maybe_gpt5_payload(prompt)
    # ensure profiling exists
    if not payload.profiling_summary:
        payload.profiling_summary = compute_profile(payload.sample_rows, payload.tables)
    zip_bytes = build_export_zip(prompt, payload)
    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=dataIDE_export.zip"}
    )