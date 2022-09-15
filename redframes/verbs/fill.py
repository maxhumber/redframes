from __future__ import annotations

from ..checks import _check_type
from ..types import Direction, LazyColumns, PandasDataFrame, Value


def fill(
    df: PandasDataFrame,
    columns: LazyColumns | None = None,
    direction: Direction | None = None,
    constant: Value | None = None,
) -> PandasDataFrame:
    _check_type(columns, {list, str, None})
    _check_type(direction, {str, None})
    columns = [columns] if isinstance(columns, str) else columns
    if (direction != None) and (constant != None):
        raise ValueError("either direction OR constant must be None")
    if (direction == None) and (constant == None):
        raise ValueError("either direction OR constant must not be None")
    if direction != None:
        if not (direction in ["down", "up"]):
            raise ValueError("must be one of {'down', 'up'}")
        method = {"down": "ffill", "up": "bfill"}.get(direction)
        value = None
    if constant != None:
        value = constant
        method = None
    df = df.copy()
    if columns:
        df[columns] = df[columns].fillna(value=value, method=method)  # type: ignore
    else:
        df = df.fillna(value=value, method=method)  # type: ignore
    return df
