from __future__ import annotations

from ..checks import _check_type, _check_keys
from ..types import LazyColumns, PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def sort(
    df: PandasDataFrame, columns: LazyColumns, descending: bool = False
) -> PandasDataFrame:
    _check_type(columns, {list, str})
    _check_type(descending, bool)
    _check_keys(columns, df.columns)
    df = df.sort_values(by=columns, ascending=not descending)
    df = df.reset_index(drop=True)
    return df
