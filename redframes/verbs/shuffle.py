from __future__ import annotations

from ..checks import enforce
from ..types import PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def shuffle(df: PandasDataFrame, seed: int | None = None) -> PandasDataFrame:
    enforce(seed, {int, None})
    df = df.sample(frac=1, random_state=seed)
    df = df.reset_index(drop=True)
    return df
