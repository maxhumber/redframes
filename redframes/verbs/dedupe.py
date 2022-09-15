from __future__ import annotations

from ..checks import _check_keys, _check_type
from ..types import LazyColumns, PandasDataFrame


def dedupe(df: PandasDataFrame, columns: LazyColumns | None = None) -> PandasDataFrame:
    _check_type(columns, {list, str, None})
    _check_keys(columns, df.columns)
    df = df.drop_duplicates(subset=columns, keep="first")
    df = df.reset_index(drop=True)
    return df
