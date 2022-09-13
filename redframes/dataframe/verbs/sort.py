from __future__ import annotations

from ...types import LazyColumns, PandasDataFrame

from ._validate import _validate_columns_keys, _validate_columns_type_list_str


def sort(
    df: PandasDataFrame, columns: LazyColumns, descending: bool = False
) -> PandasDataFrame:
    if not isinstance(descending, bool):
        raise TypeError("must be bool")
    _validate_columns_type_list_str(columns)
    _validate_columns_keys(columns, df.columns)
    df = df.sort_values(by=columns, ascending=not descending)
    df = df.reset_index(drop=True)
    return df
