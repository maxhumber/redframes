from __future__ import annotations

import warnings

from ..checks import _check_type
from ..types import Column, Columns, PandasDataFrame


def combine(
    df: PandasDataFrame, columns: Columns, into: Column, sep: str, drop: bool = True
) -> PandasDataFrame:
    _check_type(columns, list)
    _check_type(into, str)
    _check_type(sep, str)
    _check_type(drop, bool)
    into_is_in_columns = into in columns
    into_is_not_in_columns = not into_is_in_columns
    into_is_in_df_columns = into in df.columns
    if into_is_not_in_columns and into_is_in_df_columns:
        message = f"overwriting existing column '{into}'"
        warnings.warn(message)
    df = df.copy()
    new = df[columns].apply(lambda row: sep.join(row.values.astype(str)), axis=1)
    if drop:
        df = df.drop(columns, axis=1)
    df[into] = new
    return df
