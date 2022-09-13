from __future__ import annotations

from ..checks import enforce
from ..types import PandasDataFrame


# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns

def sample(
    df: PandasDataFrame, rows: int | float = 1, seed: int | None = None
) -> PandasDataFrame:
    enforce(rows, {int, float})
    if rows >= 1:
        if isinstance(rows, float):
            raise ValueError("rows (int) must be >= 1")
        df = df.sample(rows, random_state=seed)
    elif 0 < rows < 1:
        df = df.sample(frac=rows, random_state=seed)
    else:
        raise ValueError("rows (float) must be (0, 1)")
    df = df.reset_index(drop=True)
    return df
