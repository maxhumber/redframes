from __future__ import annotations

from ..types import LazyColumns, PandasDataFrame
from ..checks import enforce

def drop(df: PandasDataFrame, columns: LazyColumns) -> PandasDataFrame:
    enforce(columns, {list, str})
    df = df.drop(columns, axis=1)
    return df
