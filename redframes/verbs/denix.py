from __future__ import annotations

from ..checks import _check_type
from ..types import LazyColumns, PandasDataFrame


def denix(df: PandasDataFrame, columns: LazyColumns | None = None) -> PandasDataFrame:
    _check_type(columns, {list, str, None})
    columns = [columns] if isinstance(columns, str) else columns
    if isinstance(columns, list):
        bad_keys = set(columns).difference(df.columns)
        if bad_keys:
            if len(bad_keys) == 1:
                message = f"columns argument contains invalid key {bad_keys}"
            else:
                message = f"columns argument contains invalid keys {bad_keys}"
            raise KeyError(message)
    df = df.dropna(subset=columns)
    df = df.reset_index(drop=True)
    return df
