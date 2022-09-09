from __future__ import annotations

import pandas as pd


def shuffle(df: pd.DataFrame, seed: int | None = None) -> pd.DataFrame:
    df = df.sample(frac=1, random_state=seed)
    df = df.reset_index(drop=True)
    return df
