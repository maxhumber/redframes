from __future__ import annotations

from typing import Any

import pandas as pd

from .types import Columns, LazyColumns, PandasIndex


def enforce(argument: Any, against: type | set[type | None]):
    if isinstance(against, set):
        if len(against) == 0:
            against = {against}  # type: ignore
    if not isinstance(against, set):
        against = {against}
    optional = None in against
    just_types = against.difference({None})
    checks = [isinstance(argument, t) for t in just_types]  # type: ignore
    if optional:
        checks += [argument == None]
    if not any(checks):
        str_types = " | ".join([t.__name__ for t in just_types])  # type: ignore
        if optional:
            str_types += " | None"
        raise TypeError(f"must be {str_types}")


def enforce_keys(columns: LazyColumns | None, true: Columns | PandasIndex):
    if isinstance(columns, str):
        columns = [columns]
    columns = [] if columns == None else columns
    bad_keys = set(columns).difference(true)  # type: ignore
    if bad_keys:
        if len(bad_keys) == 1:
            raise KeyError(f"invalid key {bad_keys}")
        else:
            raise KeyError(f"invalid keys {bad_keys}")


def _validate(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.index, pd.RangeIndex):
        raise IndexError("df.index is invalid, must be pd.RangeIndex")
    if df.index.name:
        raise IndexError("df.index is invalid, must unnamed")
    if df.iloc[0].name != 0:
        raise IndexError("df.index is invalid, must start at 0")
    if not isinstance(df.columns, pd.Index):
        raise KeyError("df.columns is invalid, must be flat")
    if df.columns.has_duplicates:
        raise KeyError("df.columns is invalid, must not contain duplicate keys")
    return df
