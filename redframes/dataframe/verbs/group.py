from __future__ import annotations

import pandas as pd
import pandas.core.groupby.generic as pg

from ._validate import _validate_columns_type_list_str


def group(df: pd.DataFrame, by: list[str] | str) -> pg.DataFrameGroupBy:
    _validate_columns_type_list_str(by)
    gdf = df.groupby(by)
    return gdf
