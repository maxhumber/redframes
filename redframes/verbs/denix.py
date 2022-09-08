from __future__ import annotations

import pandas as pd


def denix(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    if columns and not isinstance(columns, list):
        raise TypeError("columns type is invalid, must be list[str] | None")
    df = df.dropna(subset=columns)
    df = df.reset_index(drop=True)
    return df
