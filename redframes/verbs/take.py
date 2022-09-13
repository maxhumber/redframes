from __future__ import annotations

from ..types import PandasDataFrame, PandasGroupedFrame


# compatibility: sklearn / train_test_split
def take(
    df: PandasDataFrame | PandasGroupedFrame, rows: int = 1, **kwargs
) -> PandasDataFrame:
    if kwargs:
        df = df.take(rows, **kwargs)  # type: ignore
        df = df.reset_index(drop=True)
        return df
    if not isinstance(rows, int):
        raise TypeError("rows type is invalid, must be str")
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
