from __future__ import annotations

import pandas as pd


def shuffle(df: pd.DataFrame, seed: int | None = None) -> pd.DataFrame:
    if not (isinstance(seed, int) or isinstance(seed, None)):
        raise TypeError("must be int | None")
    df = df.sample(frac=1, random_state=seed)
    df = df.reset_index(drop=True)
    return df
