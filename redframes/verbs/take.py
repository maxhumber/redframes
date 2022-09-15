from __future__ import annotations

from ..checks import _check_type
from ..types import PandasDataFrame, PandasGroupedFrame


def take(
    df: PandasDataFrame | PandasGroupedFrame, rows: int = 1, **kwargs
) -> PandasDataFrame:
    if kwargs:  # compatibility: sklearn / train_test_split
        df = df.take(rows, **kwargs)  # type: ignore
        df = df.reset_index(drop=True)
        return df
    _check_type(rows, int)
    if isinstance(df, PandasDataFrame):
        if rows > df.shape[0]:
            raise ValueError("rows argument is invalid, exceeds total size")
    if rows == 0:
        raise ValueError("rows argument is invalid, must not be 0")
    if rows <= -1:
        df = df.tail(rows * -1)
    else:
        df = df.head(rows)
    if isinstance(df, PandasGroupedFrame):
        df = df.reset_index()
    else:
        df = df.reset_index(drop=True)
    return df
