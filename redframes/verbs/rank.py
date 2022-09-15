from __future__ import annotations

from ..checks import _check_type
from ..types import Column, PandasDataFrame, PandasGroupedFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def rank(
    df: PandasDataFrame | PandasGroupedFrame,
    column: Column,
    into: Column,
    descending: bool = False,
) -> PandasDataFrame:
    _check_type(column, str)
    _check_type(into, str)
    _check_type(descending, bool)
    df = df.copy()
    df[into] = df[column].rank(method="dense", ascending=not descending)
    return df
