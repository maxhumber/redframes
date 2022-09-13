from __future__ import annotations

from typing import Literal

import pandas as pd
import pandas.core.groupby.generic as pg

# need to rename the name methods?


def rank(
    df: pd.DataFrame | pg.DataFrameGroupBy,
    column: str,
    into: str,
    method: Literal["min", "first", "dense"] = "dense",
    descending: bool = False,
) -> pd.DataFrame:
    df = df.copy()
    df[into] = df[column].rank(method=method, ascending=not descending)
    return df
