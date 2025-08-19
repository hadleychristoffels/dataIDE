from typing import Dict, List

import pandas as pd

from dataide.schemas import TableSchema
from dataide.utils import to_dataframe, coerce_numeric, coerce_datetime


def compute_profile(sample_rows: Dict[str, List[dict]], tables: List[TableSchema]) -> Dict[str, Dict]:
    profile: Dict[str, Dict] = {}
    schema_by_table = {t.name: t for t in tables}

    for table_name, rows in sample_rows.items():
        df = to_dataframe(rows)
        table_info: Dict[str, Dict] = {"row_count": int(len(df)), "columns": {}}
        t_schema = schema_by_table.get(table_name)

        if len(df) == 0:
            profile[table_name] = table_info
            continue

        for column_name in df.columns:
            series = df[column_name]
            non_null_count = int(series.notna().sum())
            stats = {
                "non_null": non_null_count,
                "nulls": int(len(df) - non_null_count),
                "null_pct": float(0 if len(df) == 0 else (len(df) - non_null_count) / max(1, len(df))),
                "distinct": int(series.nunique(dropna=True)),
            }

            hinted_dtype = None
            if t_schema:
                for column_meta in t_schema.columns:
                    if column_meta.name == column_name:
                        hinted_dtype = (column_meta.dtype or "").lower()
                        break

            if hinted_dtype in {"int", "integer", "bigint", "float", "double", "decimal", "number"} or pd.api.types.is_numeric_dtype(series):
                numeric = coerce_numeric(series)
                if numeric.notna().any():
                    stats.update({
                        "min": float(numeric.min()),
                        "p25": float(numeric.quantile(0.25)),
                        "p50": float(numeric.quantile(0.50)),
                        "p75": float(numeric.quantile(0.75)),
                        "max": float(numeric.max()),
                        "mean": float(numeric.mean()),
                        "std": float(numeric.std(ddof=0)),
                    })

            if (hinted_dtype and "date" in hinted_dtype) or ("date" in column_name.lower()):
                dt = coerce_datetime(series)
                if dt.notna().any():
                    stats.update({
                        "min_date": str(dt.min().date()),
                        "max_date": str(dt.max().date()),
                    })

            if series.dtype == "object":
                value_counts = series.astype(str).value_counts(dropna=True).head(5)
                if len(value_counts) > 0:
                    stats["top_values"] = {k: int(v) for k, v in value_counts.to_dict().items()}

            table_info["columns"][column_name] = stats

        numeric_df = pd.DataFrame()
        for col in df.columns:
            numeric_series = coerce_numeric(df[col])
            if numeric_series.notna().any():
                numeric_df[col] = numeric_series
        if numeric_df.shape[1] >= 2:
            table_info["correlation_pearson"] = numeric_df.corr(numeric_only=True).round(3).to_dict()

        profile[table_name] = table_info
    return profile


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


