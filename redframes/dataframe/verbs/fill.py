from __future__ import annotations

from ...types import Direction, Value, LazyColumns, PandasDataFrame


def fill(
    df: PandasDataFrame,
    columns: LazyColumns = None,
    direction: Direction | None = "down",
    constant: Value | None = None,
) -> PandasDataFrame:
    if columns and not isinstance(columns, list):
        raise TypeError("columns type is invalid, must be list[str]")
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
    if columns:
        df[columns] = df[columns].fillna(value=value, method=method)  # type: ignore
    else:
        df = df.fillna(value=value, method=method)  # type: ignore
    return df
