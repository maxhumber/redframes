from __future__ import annotations

import pandas as pd


def _validate_column_type_str(column: str):
    if not isinstance(column, str):
        raise TypeError("must be str")


def _validate_columns_type_list_str(columns: list[str] | str):
    if not (isinstance(columns, list) or isinstance(columns, str)):
        raise TypeError("must be list[str] | str")


def _validate_columns_type_list_str_none(columns: list[str] | str | None):
    is_not_list = not isinstance(columns, list)
    is_not_str = not isinstance(columns, str)
    is_not_none = not columns == None
    if all([is_not_list, is_not_str, is_not_none]):
        raise TypeError("must be list[str] | str | None")


def _validate_columns_keys(
    columns: list[str] | str | None, actual: list[str] | pd.Index
):
    if isinstance(columns, str):
        columns = [columns]
    columns = [] if columns == None else columns
    bad_keys = set(columns).difference(actual)  # type: ignore
    if bad_keys:
        if len(bad_keys) == 1:
            raise KeyError(f"invalid key {bad_keys}")
        else:
            raise KeyError(f"invalid keys {bad_keys}")
