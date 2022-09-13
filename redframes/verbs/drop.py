from __future__ import annotations

from ..checks import enforce
from ..types import LazyColumns, PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def drop(df: PandasDataFrame, columns: LazyColumns) -> PandasDataFrame:
    enforce(columns, {list, str})
    df = df.drop(columns, axis=1)
    return df
