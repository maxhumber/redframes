from __future__ import annotations

from typing import Any, Callable
import pandas as pd
import pandas.core.groupby.generic as pg

def aggregate(df: pd.DataFrame | pg.DataFrameGroupBy, aggregations: dict[str, tuple[str, Callable[..., Any]]]) -> pd.DataFrame:
    if not isinstance(aggregations, dict):
        raise TypeError("aggregations type is invalid, must be dict[str, tuple[str, Callable[..., Any]]]")
    if isinstance(df, pg.DataFrameGroupBy):
        df = df.agg(**aggregations)
        df = df.reset_index()
    else:
        df = df.agg(**aggregations)
        df = df.T
        df = df.reset_index(drop=True)
        df = df.fillna(method="ffill")
        df = df.fillna(method="bfill")
        df = df.head(1)
    return df
