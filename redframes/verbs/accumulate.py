from __future__ import annotations

from ..types import Column, PandasDataFrame, PandasGroupedFrame
from ..checks import enforce

def accumulate(
    df: PandasDataFrame | PandasGroupedFrame, column: Column, into: Column
) -> PandasDataFrame:
    enforce(column, types={str})
    enforce(into, types={str})
    if isinstance(df, PandasDataFrame):
        df = df.copy()
    result = df[column].cumsum()
    if isinstance(df, PandasGroupedFrame):
        df = df.obj.copy()  # type: ignore
    df[into] = result
    return df
