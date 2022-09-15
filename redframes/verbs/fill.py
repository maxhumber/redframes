from __future__ import annotations

from ..checks import _check_type
from ..types import Direction, LazyColumns, PandasDataFrame, Value

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def fill(
    df: PandasDataFrame,
    columns: LazyColumns = None,
    direction: Direction | None = "down",
    constant: Value | None = None,
) -> PandasDataFrame:
    _check_type(columns, {list, str, None})
    _check_type(direction, {str, None})
    columns = [columns] if isinstance(columns, str) else columns
    if direction and constant:
        raise ValueError("direction OR constant arugment must be None")
    if (not direction) and (not constant):
        raise ValueError("direction OR constant arugment must not be None")
    if direction:
        if not (direction in ["down", "up"]):
            raise ValueError(
                "direction argument is invalid, must be one of {'down', 'up'}"
            )
        method = {"down": "ffill", "up": "bfill"}.get(direction)
        value = None
    if constant:
        value = constant
        method = None
    df = df.copy()
    if columns:
        df[columns] = df[columns].fillna(value=value, method=method)  # type: ignore
    else:
        df = df.fillna(value=value, method=method)  # type: ignore
    return df
