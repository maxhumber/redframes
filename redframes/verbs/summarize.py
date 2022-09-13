from __future__ import annotations

from ..checks import enforce
from ..types import Column, Func, PandasDataFrame, PandasGroupedFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def summarize(
    df: PandasDataFrame | PandasGroupedFrame,
    over: dict[Column, tuple[Column, Func]],
) -> PandasDataFrame:
    enforce(over, dict)
    if isinstance(df, PandasGroupedFrame):
        df = df.agg(**over)
        df = df.reset_index()
    else:
        df = df.agg(**over)  # type: ignore
        df = df.T  # type: ignore
        df = df.reset_index(drop=True)  # type: ignore
        df = df.fillna(method="ffill")  # type: ignore
        df = df.fillna(method="bfill")  # type: ignore
        df = df.head(1)  # type: ignore
    return df
