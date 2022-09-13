from __future__ import annotations

import pandas as pd
import pandas.core.groupby.generic as pg


def take(
    df: pd.DataFrame | pg.DataFrameGroupBy, rows: int = 1, **kwargs
) -> pd.DataFrame:
    if kwargs:  # compatibility: sklearn.model_selection_train_test_split
        df = df.take(rows, **kwargs)  # type: ignore
        df = df.reset_index(drop=True)
        return df
    if not isinstance(rows, int):
        raise TypeError("rows type is invalid, must be str")
    if isinstance(df, pd.DataFrame):
        if rows > df.shape[0]:
            raise ValueError("rows argument is invalid, exceeds total size")
    if rows == 0:
        raise ValueError("rows argument is invalid, must not be 0")
    if rows <= -1:
        df = df.tail(rows * -1)
    else:
        df = df.head(rows)
    if isinstance(df, pg.DataFrameGroupBy):
        df = df.reset_index()
    else:
        df = df.reset_index(drop=True)
    return df
