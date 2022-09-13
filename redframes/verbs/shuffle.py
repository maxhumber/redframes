from __future__ import annotations

from ..types import PandasDataFrame
from ..checks import enforce


def shuffle(df: PandasDataFrame, seed: int | None = None) -> PandasDataFrame:
    enforce(seed, {int, None})
    df = df.sample(frac=1, random_state=seed)
    df = df.reset_index(drop=True)
    return df
