from __future__ import annotations

from ..checks import enforce, enforce_keys
from ..types import LazyColumns, PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def sort(
    df: PandasDataFrame, columns: LazyColumns, descending: bool = False
) -> PandasDataFrame:
    enforce(columns, {list, str})
    enforce(descending, bool)
    enforce_keys(columns, df.columns)
    df = df.sort_values(by=columns, ascending=not descending)
    df = df.reset_index(drop=True)
    return df
