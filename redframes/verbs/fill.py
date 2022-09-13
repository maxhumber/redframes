from __future__ import annotations

from ..types import Direction, LazyColumns, PandasDataFrame, Value
from ._validate import _validate_columns_type_list_str_none


def fill(
    df: PandasDataFrame,
    columns: LazyColumns = None,
    direction: Direction | None = "down",
    constant: Value | None = None,
) -> PandasDataFrame:
    _validate_columns_type_list_str_none(columns)
    if isinstance(columns, str):
        columns = [columns]
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
