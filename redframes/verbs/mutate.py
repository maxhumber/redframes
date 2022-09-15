from __future__ import annotations

from ..checks import _check_type
from ..types import Column, Func, PandasDataFrame


def mutate(df: PandasDataFrame, over: dict[Column, Func]) -> PandasDataFrame:
    _check_type(over, dict)
    df = df.copy()
    for column, mutation in over.items():
        df[column] = df.apply(mutation, axis=1)
    return df  # type: ignore
