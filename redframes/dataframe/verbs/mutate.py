from __future__ import annotations

from ...types import Column, PandasDataFrame, PandasGroupedFrame, Func


def mutate(
    df: PandasDataFrame | PandasGroupedFrame, over: dict[Column, Func]
) -> PandasDataFrame:
    if not isinstance(over, dict):
        raise TypeError("must be dict[str, Callable[..., Any]")
    df = df.copy()
    for column, mutation in over.items():
        df[column] = df.apply(mutation, axis=1)
    return df
