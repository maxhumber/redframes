from __future__ import annotations

import pandas as pd

from ..checks import enforce
from ..types import Join, LazyColumns, PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns

def join(
    lhs: PandasDataFrame,
    rhs: PandasDataFrame,
    on: LazyColumns,
    how: Join = "left",
) -> PandasDataFrame:
    enforce(on, {list, str})
    enforce(how, str)
    if not how in ["left", "right", "inner", "full"]:
        raise ValueError(
            "method argument is invalid, must be one of {'left', 'right', 'inner', 'full'}"
        )
    how = "outer" if how == "full" else how  # type: ignore
    df = pd.merge(lhs, rhs, on=on, how=how, suffixes=("_lhs", "_rhs"))
    df = df.reset_index(drop=True)
    return df
