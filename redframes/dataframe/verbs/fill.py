from __future__ import annotations

from typing import Literal

import pandas as pd


def fill(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    strategy: Literal["down", "up", "constant"] = "down",
    constant: str | int | float | None = None,
) -> pd.DataFrame:
    if columns and not isinstance(columns, list):
        raise TypeError("columns type is invalid")
    if not (strategy in ["down", "up", "constant"]):
        raise ValueError("strategy argument is invalid")
    if strategy == "constant" and not constant:
        raise ValueError("constant argument required")
    method = {"down": "ffill", "up": "bfill", "constant": None}[strategy]
    value = None if strategy in ["down", "up"] else constant
    if columns:
        df[columns] = df[columns].fillna(value=constant, method=method)  # type: ignore
    else:
        df = df.fillna(value=value, method=method)  # type: ignore
    return df
