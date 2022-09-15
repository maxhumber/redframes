from __future__ import annotations

from ..checks import _check_type
from ..types import Column, Func, PandasDataFrame, PandasGroupedFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def mutate(
    df: PandasDataFrame | PandasGroupedFrame, over: dict[Column, Func]
) -> PandasDataFrame:
    _check_type(over, dict)
    df = df.copy()
    for column, mutation in over.items():
        df[column] = df.apply(mutation, axis=1)
    return df
