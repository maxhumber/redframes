from __future__ import annotations

from typing import Literal

import pandas as pd
import pandas.core.groupby.generic as pg


def accumulate(
    df: pd.DataFrame | pg.DataFrameGroupBy, column: str, method: Literal["min", "max", "sum"], into: str
) -> pd.DataFrame:
    df = df.copy()
    if method == "min":
        df[into] = df[column].cummin()
    elif method == "max":
        df[into] = df[column].cummax()
    elif method == "sum":
        df[into] = df[column].cumsum()
    else:
        raise ValueError("method is invalid, must be one of {'min', 'max', 'sum'}")
    return df
