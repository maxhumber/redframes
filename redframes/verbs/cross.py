from __future__ import annotations

import pandas as pd

from ..checks import _check_type
from ..types import PandasDataFrame


def cross(
    lhs: PandasDataFrame,
    rhs: PandasDataFrame,
    postfix: tuple[str, str] = ("_lhs", "_rhs"),
) -> PandasDataFrame:
    _check_type(postfix, tuple)
    df = pd.merge(lhs, rhs, how="cross", suffixes=postfix)
    df = df.reset_index(drop=True)
    return df
