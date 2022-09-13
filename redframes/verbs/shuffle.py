from __future__ import annotations

from ..types import PandasDataFrame


def shuffle(df: PandasDataFrame, seed: int | None = None) -> PandasDataFrame:
    if not (isinstance(seed, int) or seed == None):
        raise TypeError("must be int | None")
    df = df.sample(frac=1, random_state=seed)
    df = df.reset_index(drop=True)
    return df
