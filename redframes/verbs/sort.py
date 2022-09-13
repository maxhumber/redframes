from __future__ import annotations

from ..types import LazyColumns, PandasDataFrame
from ._validate import _validate_columns_keys, _validate_columns_type_list_str
from ..checks import enforce

def sort(
    df: PandasDataFrame, columns: LazyColumns, descending: bool = False
) -> PandasDataFrame:
    enforce(columns, {list, str})
    enforce(descending, {bool})
    _validate_columns_keys(columns, df.columns)
    df = df.sort_values(by=columns, ascending=not descending)
    df = df.reset_index(drop=True)
    return df
