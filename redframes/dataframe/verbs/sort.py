from __future__ import annotations

import pandas as pd

from ._validate import _validate_columns_keys, _validate_columns_type_list_str


def sort(
    df: pd.DataFrame, columns: list[str] | str, descending: bool = False
) -> pd.DataFrame:
    if not isinstance(descending, bool):
        raise TypeError("must be bool")
    _validate_columns_type_list_str(columns)
    _validate_columns_keys(columns, df.columns)
    df = df.sort_values(by=columns, ascending=not descending)
    df = df.reset_index(drop=True)
    return df
