from __future__ import annotations

import pandas as pd
import pandas.core.groupby.generic as pg


def accumulate(
    df: pd.DataFrame | pg.DataFrameGroupBy, column: str, into: str
) -> pd.DataFrame:
    if not isinstance(column, str):
        raise TypeError("invalid column type, must be str")
    if not isinstance(into, str):
        raise TypeError("invalid into type, must be str")
    if isinstance(df, pd.DataFrame):
        df = df.copy()
    result = df[column].cumsum()
    if isinstance(df, pg.DataFrameGroupBy):
        df = df.obj.copy()
    df[into] = result
    return df

