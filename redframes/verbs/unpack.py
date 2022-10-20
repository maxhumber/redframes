from __future__ import annotations

from ..checks import _check_type
from ..types import Column, PandasDataFrame


def unpack(df: PandasDataFrame, column: Column, sep: str) -> PandasDataFrame:
    _check_type(column, str)
    _check_type(sep, str)
    df = df.assign(**{column: df[column].str.split(sep)})
    df = df.explode(column)
    df = df.reset_index(drop=True)
    return df
