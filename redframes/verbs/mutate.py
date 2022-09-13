from __future__ import annotations

from ..types import Column, Func, PandasDataFrame, PandasGroupedFrame
from ..checks import enforce

def mutate(
    df: PandasDataFrame | PandasGroupedFrame, over: dict[Column, Func]
) -> PandasDataFrame:
    enforce(over, {dict})
    df = df.copy()
    for column, mutation in over.items():
        df[column] = df.apply(mutation, axis=1)
    return df
