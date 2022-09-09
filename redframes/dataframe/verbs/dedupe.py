from __future__ import annotations

from typing import Literal

import pandas as pd

# name of this?
# keep argument?
# keep argument name?


def dedupe(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    keep: Literal["first", "last"] = "first",
) -> pd.DataFrame:
    if columns and not isinstance(columns, list):
        raise TypeError("columns type is invalid, must be list[str]")
    if keep not in ["first", "last"]:
        raise ValueError("keep argument is invalid, must be one of {'first', 'last'}")
    df = df.drop_duplicates(subset=columns, keep=keep)
    df = df.reset_index(drop=True)
    return df
