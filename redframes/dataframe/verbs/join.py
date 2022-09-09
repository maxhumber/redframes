from __future__ import annotations

from typing import Literal

import pandas as pd


def join(
    lhs: pd.DataFrame,
    rhs: pd.DataFrame,
    on: dict[str, str],
    how: Literal["left", "right", "inner", "full"] = "left",
) -> pd.DataFrame:
    if not isinstance(on, dict):
        raise TypeError("on type is invalid, must be type dict[str, str]")
    if not how in ["left", "right", "inner", "full"]:
        raise ValueError(
            "method argument is invalid, must be one of {'left', 'right', 'inner', 'full'}"
        )
    how = "outer" if how == "full" else how  # type: ignore
    left_on = list(on.keys())
    right_on = list(on.values())
    df = pd.merge(
        lhs, rhs, left_on=left_on, right_on=right_on, how=how, suffixes=("_lhs", "_rhs")
    )
    df = df.reset_index(drop=True)
    return df
