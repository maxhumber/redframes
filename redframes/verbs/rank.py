from __future__ import annotations

from ..checks import _check_type
from ..types import Column, PandasDataFrame, PandasGroupedFrame

import warnings


def rank(
    df: PandasDataFrame | PandasGroupedFrame,
    column: Column,
    into: Column,
    descending: bool = False,
) -> PandasDataFrame:
    _check_type(column, str)
    _check_type(into, str)
    _check_type(descending, bool)
    into_is_not_column = (into != column)
    into_is_in_df_columns = (into in df.columns)
    if into_is_not_column and into_is_in_df_columns: 
        message = f"overwriting existing column '{into}'"
        warnings.warn(message)
    df = df.copy()
    df[into] = df[column].rank(method="dense", ascending=not descending)
    return df
