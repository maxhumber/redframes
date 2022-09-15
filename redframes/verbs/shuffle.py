from __future__ import annotations

from ..checks import _check_type
from ..types import PandasDataFrame


def shuffle(df: PandasDataFrame, seed: int | None = None) -> PandasDataFrame:
    _check_type(seed, {int, None})
    df = df.sample(frac=1, random_state=seed)
    df = df.reset_index(drop=True)
    return df
