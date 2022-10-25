from __future__ import annotations

from ..checks import _check_type
from ..types import Column, PandasDataFrame, PandasGroupedFrame


def pack(
    df: PandasDataFrame | PandasGroupedFrame, column: Column, sep: str
) -> PandasDataFrame:
    _check_type(column, str)
    _check_type(sep, str)
    order = df.obj.columns if isinstance(df, PandasGroupedFrame) else df.columns  # type: ignore
    df = df.agg(**{column: (column, lambda x: x.astype(str).str.cat(sep=sep))})  # type: ignore
    df = df[[col for col in df.columns if col in order]]
    df = df.reset_index(drop=True)
    return df
