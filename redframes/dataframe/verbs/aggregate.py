from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import pandas.core.groupby.generic as pg

# name of aggregate? Not summrize?
# name of aggregations argument?


def aggregate(
    df: pd.DataFrame | pg.DataFrameGroupBy,
    aggregations: dict[str, tuple[str, Callable[..., Any]]],
) -> pd.DataFrame:
    if not isinstance(aggregations, dict):
        raise TypeError(
            "aggregations type is invalid, must be dict[str, tuple[str, Callable[..., Any]]]"
        )
    if isinstance(df, pg.DataFrameGroupBy):
        df = df.agg(**aggregations)
        df = df.reset_index()
    else:
        df = df.agg(**aggregations)  # type: ignore
        df = df.T  # type: ignore
        df = df.reset_index(drop=True)  # type: ignore
        df = df.fillna(method="ffill")  # type: ignore
        df = df.fillna(method="bfill")  # type: ignore
        df = df.head(1)  # type: ignore
    return df
