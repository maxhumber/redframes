from __future__ import annotations

from ..checks import enforce
from ..types import LazyColumns, PandasDataFrame
from ._validate import _validate_columns_keys

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns

def dedupe(df: PandasDataFrame, columns: LazyColumns | None = None) -> PandasDataFrame:
    enforce(columns, {list, str, None})
    _validate_columns_keys(columns, df.columns)
    df = df.drop_duplicates(subset=columns, keep="first")
    df = df.reset_index(drop=True)
    return df
