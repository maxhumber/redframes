from __future__ import annotations

import pandas as pd

from ..checks import _check_type
from ..types import Join, LazyColumns, PandasDataFrame


def join(
    lhs: PandasDataFrame,
    rhs: PandasDataFrame,
    on: LazyColumns,
    how: Join = "left",
    postfix: tuple[str, str] = ("_lhs", "_rhs"),
) -> PandasDataFrame:
    _check_type(on, {list, str})
    _check_type(how, str)
    _check_type(postfix, tuple)
    if not how in ["left", "right", "inner", "full"]:
        message = (
            "on argument is invalid, must be one of {'left', 'right', 'inner', 'full'}"
        )
        raise ValueError(message)
    how = "outer" if (how == "full") else how  # type: ignore
    df = pd.merge(lhs, rhs, on=on, how=how, suffixes=postfix)
    df = df.reset_index(drop=True)
    return df
