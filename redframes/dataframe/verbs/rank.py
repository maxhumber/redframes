from __future__ import annotations

from typing import Literal

import pandas as pd
import pandas.core.groupby.generic as pg

# need to rename the name methods?
# ascending? descending? reverse?


def rank(
    df: pd.DataFrame | pg.DataFrameGroupBy,
    column: str,
    method: Literal["average", "min", "max", "first", "dense"],
    into: str,
    reverse: bool = False,
) -> pd.DataFrame:
    df = df.copy()
    df[into] = df[column].rank(method=method, ascending=not reverse)
    return df
