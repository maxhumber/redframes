from __future__ import annotations

from typing import Literal

import pandas as pd


def join(
    lhs: pd.DataFrame,
    rhs: pd.DataFrame,
    on: dict[str, str],
    method: Literal["left", "right", "inner", "full"] = "left",
    suffixes=("_lhs", "_rhs"),
) -> pd.DataFrame:
    if not isinstance(on, dict):
        raise TypeError("on type is invalid")
    if not method in ["left", "right", "inner", "full"]:
        raise ValueError("method argument is invalid")
    how = "outer" if method == "full" else method
    left_on, right_on = list(on.keys()), list(on.values())
    df = pd.merge(
        lhs, rhs, left_on=left_on, right_on=right_on, how=how, suffixes=suffixes
    )
    df = df.reset_index(drop=True)
    return df
