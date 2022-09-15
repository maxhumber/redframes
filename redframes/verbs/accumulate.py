from __future__ import annotations

import warnings

from ..checks import _check_type
from ..types import Column, PandasDataFrame, PandasGroupedFrame


def accumulate(
    df: PandasDataFrame | PandasGroupedFrame, column: Column, into: Column
) -> PandasDataFrame:
    _check_type(column, str)
    _check_type(into, str)
    if isinstance(df, PandasDataFrame):
        into_is_not_column = into != column
        into_is_in_df_columns = into in df.columns
        if into_is_not_column and into_is_in_df_columns:
            message = f"overwriting existing column '{into}'"
            warnings.warn(message)
        df = df.copy()
    result = df[column].cumsum()
    if isinstance(df, PandasGroupedFrame):
        df = df.obj.copy()  # type: ignore
    df[into] = result  # type: ignore
    return df  # type: ignore
