from __future__ import annotations

import pandas as pd
import pandas.core.groupby.generic as pg


def group(df: pd.DataFrame, columns: list[str]) -> pg.DataFrameGroupBy:
    if not isinstance(columns, list):
        raise TypeError("columns type is invalid, must be list[str]")
    gdf = df.groupby(columns)
    return gdf
