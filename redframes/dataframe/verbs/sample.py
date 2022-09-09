from __future__ import annotations

import pandas as pd


def sample(
    df: pd.DataFrame, rows: int | float = 1, seed: int | None = None
) -> pd.DataFrame:
    if type(rows) not in [int, float]:
        raise TypeError("rows type is invalid, must be int | float")
    if rows >= 1:
        if isinstance(rows, float):
            raise ValueError("rows (int) must be >= 1")
        df = df.sample(rows, random_state=seed)
    elif 0 < rows < 1:
        df = df.sample(frac=rows, random_state=seed)
    else:
        raise TypeError("rows (float) must be (0, 1)")
    df = df.reset_index(drop=True)
    return df
