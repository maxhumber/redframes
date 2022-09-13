from __future__ import annotations

import pandas as pd
import pandas.core.groupby.generic as pg

from ._validate import _validate_column_type_str


def accumulate(
    df: pd.DataFrame | pg.DataFrameGroupBy, column: str, into: str
) -> pd.DataFrame:
    _validate_column_type_str(column)
    if not isinstance(into, str):
        raise TypeError("must be str")
    if isinstance(df, pd.DataFrame):
        df = df.copy()
    result = df[column].cumsum()
    if isinstance(df, pg.DataFrameGroupBy):
        df = df.obj.copy()  # type: ignore
    df[into] = result
    return df
