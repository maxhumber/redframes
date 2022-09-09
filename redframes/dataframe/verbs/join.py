from __future__ import annotations

from typing import Literal

import pandas as pd


def join(
    lhs: pd.DataFrame,
    rhs: pd.DataFrame,
    on: dict[str, str],
    method: Literal["left", "right", "inner", "full"] = "left",
    suffixes: tuple[str, str] = ("_lhs", "_rhs"),
) -> pd.DataFrame:
    if not isinstance(on, dict):
        raise TypeError("on type is invalid, must be type dict[str, str]")
    if not method in ["left", "right", "inner", "full"]:
        raise ValueError(
            "method argument is invalid, must be one of {'left', 'right', 'inner', 'full'}"
        )
    if not (isinstance(suffixes, tuple) and len(suffixes) == 2):
        raise TypeError("suffixes type is invalid, must be tuple[str, str]")
    how = "outer" if method == "full" else method
    left_on, right_on = list(on.keys()), list(on.values())
    df = pd.merge(
        lhs, rhs, left_on=left_on, right_on=right_on, how=how, suffixes=suffixes
    )
    df = df.reset_index(drop=True)
    return df
