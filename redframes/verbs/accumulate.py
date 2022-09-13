from __future__ import annotations

from ..checks import enforce
from ..types import Column, PandasDataFrame, PandasGroupedFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def accumulate(
    df: PandasDataFrame | PandasGroupedFrame, column: Column, into: Column
) -> PandasDataFrame:
    enforce(column, str)
    enforce(into, str)
    if isinstance(df, PandasDataFrame):
        df = df.copy()
    result = df[column].cumsum()
    if isinstance(df, PandasGroupedFrame):
        df = df.obj.copy()  # type: ignore
    df[into] = result
    return df
