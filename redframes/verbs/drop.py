from __future__ import annotations

from ..checks import _check_type
from ..types import LazyColumns, PandasDataFrame


def drop(df: PandasDataFrame, columns: LazyColumns) -> PandasDataFrame:
    _check_type(columns, {list, str})
    df = df.drop(columns, axis=1)
    return df
