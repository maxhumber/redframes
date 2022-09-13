from __future__ import annotations

import pandas as pd


def dedupe(df: pd.DataFrame, columns: list[str] | str | None = None) -> pd.DataFrame:
    if all([not isinstance(columns, list), not isinstance(columns, str), not columns == None]):
        raise TypeError("columns type is invalid, must be list[str] | str | None")
    if isinstance(columns, str):
        columns = [columns]
    if isinstance(columns, list):
        bad_keys = list(set(columns).difference(set(df.columns)))
        if bad_keys:
            message = f"columns argument is invalid, contains extra keys: {bad_keys}"
            raise ValueError(message)
    df = df.drop_duplicates(subset=columns, keep="first")
    df = df.reset_index(drop=True)
    return df
