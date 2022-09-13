from __future__ import annotations

import pandas as pd

from ._validate import _validate_columns_keys, _validate_columns_type_list_str_none


def dedupe(df: pd.DataFrame, columns: list[str] | str | None = None) -> pd.DataFrame:
    _validate_columns_type_list_str_none(columns)
    _validate_columns_keys(columns, df.columns)
    df = df.drop_duplicates(subset=columns, keep="first")
    df = df.reset_index(drop=True)
    return df
