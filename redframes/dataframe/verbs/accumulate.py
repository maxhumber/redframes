from __future__ import annotations

from typing import Literal

import pandas as pd
import pandas.core.groupby.generic as pg


# mean, max, min, product?


def accumulate(
    df: pd.DataFrame | pg.DataFrameGroupBy, column: str, into: str
) -> pd.DataFrame:
    df = df.copy()
    df[into] = df[column].cumsum()
    return df
