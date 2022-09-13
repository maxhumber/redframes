from __future__ import annotations

from ..checks import enforce
from ..types import LazyColumns, PandasDataFrame
from ._validate import _validate_columns_keys

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
    _validate_columns_keys(columns, df.columns)
    df = df.sort_values(by=columns, ascending=not descending)
    df = df.reset_index(drop=True)
    return df
