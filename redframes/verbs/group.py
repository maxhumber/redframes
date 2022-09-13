from __future__ import annotations

from ..checks import enforce
from ..types import LazyColumns, PandasDataFrame, PandasGroupedFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def group(df: PandasDataFrame, by: LazyColumns) -> PandasGroupedFrame:
    enforce(by, {list, str})
    gdf = df.groupby(by)
    return gdf
