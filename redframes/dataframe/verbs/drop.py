from __future__ import annotations

import pandas as pd


def drop(df: pd.DataFrame, columns: list[str] | str) -> pd.DataFrame:
    if not (isinstance(columns, list) or isinstance(columns, str)):
        raise TypeError("invalid columns type, must be list[str] | str")
    df = df.drop(columns, axis=1)
    return df
