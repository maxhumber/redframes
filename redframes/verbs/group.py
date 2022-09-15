from __future__ import annotations

from ..checks import _check_type
from ..types import LazyColumns, PandasDataFrame, PandasGroupedFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def group(df: PandasDataFrame, by: LazyColumns) -> PandasGroupedFrame:
    _check_type(by, {list, str})
    gdf = df.groupby(by)
    return gdf
