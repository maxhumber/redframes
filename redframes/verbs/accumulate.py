from __future__ import annotations

from ..types import Column, PandasDataFrame, PandasGroupedFrame
from ._validate import _validate_column_type_str


def accumulate(
    df: PandasDataFrame | PandasGroupedFrame, column: Column, into: Column
) -> PandasDataFrame:
    _validate_column_type_str(column)
    if not isinstance(into, str):
        raise TypeError("must be str")
    if isinstance(df, PandasDataFrame):
        df = df.copy()
    result = df[column].cumsum()
    if isinstance(df, PandasGroupedFrame):
        df = df.obj.copy()  # type: ignore
    df[into] = result
    return df
