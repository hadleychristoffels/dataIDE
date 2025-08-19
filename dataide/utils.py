from typing import List

import pandas as pd


def to_dataframe(rows: List[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def coerce_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=False, infer_datetime_format=True)


