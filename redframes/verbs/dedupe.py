from __future__ import annotations

from ..types import LazyColumns, PandasDataFrame
from ._validate import _validate_columns_keys
from ..checks import enforce


def dedupe(df: PandasDataFrame, columns: LazyColumns | None = None) -> PandasDataFrame:
    enforce(columns, {list, str, None})
    _validate_columns_keys(columns, df.columns)
    df = df.drop_duplicates(subset=columns, keep="first")
    df = df.reset_index(drop=True)
    return df
