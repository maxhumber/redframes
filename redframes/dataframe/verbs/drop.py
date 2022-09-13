from __future__ import annotations

from ...types import LazyColumns, PandasDataFrame


def drop(df: PandasDataFrame, columns: LazyColumns) -> PandasDataFrame:
    if not (isinstance(columns, list) or isinstance(columns, str)):
        raise TypeError("invalid columns type, must be list[str] | str")
    df = df.drop(columns, axis=1)
    return df
