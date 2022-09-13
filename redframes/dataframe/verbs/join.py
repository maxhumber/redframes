from __future__ import annotations

import pandas as pd
from ...types import LazyColumns, PandasDataFrame, Join


def join(
    lhs: PandasDataFrame,
    rhs: PandasDataFrame,
    on: LazyColumns,
    how: Join = "left",
) -> PandasDataFrame:
    if not isinstance(on, dict):
        raise TypeError("on type is invalid, must be type dict[str, str]")
    if not how in ["left", "right", "inner", "full"]:
        raise ValueError(
            "method argument is invalid, must be one of {'left', 'right', 'inner', 'full'}"
        )
    how = "outer" if how == "full" else how  # type: ignore
    df = pd.merge(
        lhs, rhs, on=on, how=how, suffixes=("_lhs", "_rhs")
    )
    df = df.reset_index(drop=True)
    return df
