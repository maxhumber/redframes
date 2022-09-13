from __future__ import annotations

from ..checks import enforce, enforce_keys
from ..types import LazyColumns, PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def dedupe(df: PandasDataFrame, columns: LazyColumns | None = None) -> PandasDataFrame:
    enforce(columns, {list, str, None})
    enforce_keys(columns, df.columns)
    df = df.drop_duplicates(subset=columns, keep="first")
    df = df.reset_index(drop=True)
    return df
