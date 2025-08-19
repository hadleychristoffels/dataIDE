from typing import Dict, List, Optional

import pandas as pd

from dataide.schemas import SuggestedChart
from dataide.utils import to_dataframe, coerce_numeric, coerce_datetime


def suggest_charts(sample_rows: Dict[str, List[dict]]) -> List[SuggestedChart]:
    charts: List[SuggestedChart] = []
    for table_name, rows in sample_rows.items():
        if not rows:
            continue
        df = to_dataframe(rows)

        for column in df.columns:
            if coerce_numeric(df[column]).notna().any():
                charts.append(SuggestedChart(title=f"{table_name}.{column} — distribution", kind="hist", table=table_name, x=column))
                break

        for column in df.columns:
            if df[column].dtype == "object" and df[column].astype(str).nunique() <= 30:
                charts.append(SuggestedChart(title=f"{table_name}.{column} — top categories", kind="bar", table=table_name, x=column))
                break

        date_col: Optional[str] = next((c for c in df.columns if "date" in c.lower() and coerce_datetime(df[c]).notna().any()), None)
        num_col: Optional[str] = next((c for c in df.columns if coerce_numeric(df[c]).notna().any()), None)
        if date_col and num_col:
            charts.append(SuggestedChart(title=f"{table_name}: {num_col} over {date_col}", kind="line", table=table_name, x=date_col, y=num_col))

    return charts[:6]


