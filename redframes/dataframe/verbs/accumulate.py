from __future__ import annotations

import pandas as pd
import pandas.core.groupby.generic as pg


def accumulate(
    df: pd.DataFrame | pg.DataFrameGroupBy, column: str, into: str
) -> pd.DataFrame:
    df = df.copy()
    df[into] = df[column].cumsum()
    return df
