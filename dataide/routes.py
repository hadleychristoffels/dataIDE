import io
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from fastapi import APIRouter, Form
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

from dataide.schemas import DataIDEPayload
from dataide.services.charts import suggest_charts
from dataide.services.export import build_export_zip, zip_tables
from dataide.services.generator import maybe_gpt5_payload, synthetic_payload
from dataide.services.profile import compute_profile, profile_to_rows
from dataide.ui import ui_response


router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def ui() -> HTMLResponse:
    return ui_response()


@router.get("/api/ping")
def ping():
    return {"ok": True}


@router.post("/api/generate")
def generate(prompt: str = Form(...)):
    try:
        payload = maybe_gpt5_payload(prompt)
        if not payload.profiling_summary:
            payload.profiling_summary = compute_profile(payload.sample_rows, payload.tables)
        if not payload.suggested_charts:
            payload.suggested_charts = suggest_charts(payload.sample_rows)
        return JSONResponse(payload.model_dump())
    except Exception as e:
        fallback = synthetic_payload(prompt)
        fallback.caveats = (fallback.caveats or []) + [f"Fallback used due to error in /api/generate: {e}"]
        return JSONResponse(fallback.model_dump())


@router.post("/api/samples.zip")
def samples_zip(prompt: str = Form(...)):
    payload = maybe_gpt5_payload(prompt)
    zip_bytes = zip_tables(payload.sample_rows)
    return StreamingResponse(io.BytesIO(zip_bytes), media_type="application/zip", headers={"Content-Disposition": "attachment; filename=samples.zip"})


@router.get("/api/profile.json")
def profile_json(prompt: str):
    payload = maybe_gpt5_payload(prompt)
    prof = compute_profile(payload.sample_rows, payload.tables)
    return Response(
        pd.Series(prof).to_json(indent=2),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=profiling.json"},
    )


@router.get("/api/profile.csv")
def profile_csv(prompt: str):
    payload = maybe_gpt5_payload(prompt)
    prof = compute_profile(payload.sample_rows, payload.tables)
    rows = profile_to_rows(prof)
    df = pd.DataFrame(rows, columns=["level", "table", "column", "metric", "value"])
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return Response(csv_bytes, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=profiling.csv"})


@router.get("/api/plot.png")
def plot_png(prompt: str, table: str, kind: str = "hist", x: str = "", y: Optional[str] = None):
    payload = maybe_gpt5_payload(prompt)
    rows = payload.sample_rows.get(table, [])
    fig, ax = plt.subplots(figsize=(9, 4.5))
    try:
        if not rows:
            raise ValueError(f"No rows for table '{table}'")
        df = pd.DataFrame(rows)
        if not x or x not in df.columns:
            x = next((c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])), df.columns[0])

        if kind == "hist":
            s = pd.to_numeric(df[x], errors="coerce").dropna()
            if s.empty:
                raise ValueError(f"Column '{x}' has no numeric data")
            s.plot(kind="hist", ax=ax)
            ax.set_ylabel("Frequency")
        elif kind == "bar":
            vc = df[x].astype(str).value_counts().sort_values(ascending=False).head(20)
            if vc.empty:
                raise ValueError(f"No categorical values to plot for '{x}'")
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
                if line_df.empty:
                    raise ValueError("No valid date/number pairs")
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
            if sc.empty:
                raise ValueError("No numeric pairs for scatter")
            sc.plot(kind="scatter", x=x, y=y, ax=ax)
        elif kind == "box":
            s = pd.to_numeric(df[x], errors="coerce").dropna()
            if s.empty:
                raise ValueError(f"No numeric data for boxplot '{x}'")
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


@router.get("/api/corr.png")
def corr_png(prompt: str, table: str):
    payload = maybe_gpt5_payload(prompt)
    rows = payload.sample_rows.get(table, [])
    fig, ax = plt.subplots(figsize=(6.5, 5))
    try:
        if not rows:
            raise ValueError(f"No rows for table '{table}'")
        df = pd.DataFrame(rows)

        df_num = pd.DataFrame()
        for c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            if s.notna().any():
                df_num[c] = s

        if df_num.shape[1] < 2:
            raise ValueError("Not enough numeric columns for correlation")

        corr = df_num.corr(numeric_only=True)
        im = ax.imshow(corr, interpolation="nearest")
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


@router.post("/api/export.zip")
def export_all(prompt: str = Form(...)):
    payload = maybe_gpt5_payload(prompt)
    if not payload.profiling_summary:
        payload.profiling_summary = compute_profile(payload.sample_rows, payload.tables)
    zip_bytes = build_export_zip(prompt, payload)
    return StreamingResponse(io.BytesIO(zip_bytes), media_type="application/zip", headers={"Content-Disposition": "attachment; filename=dataIDE_export.zip"})


