from __future__ import annotations

import pandas as pd

from ..checks import _check_type
from ..types import Column, Columns, LazyColumns, PandasDataFrame


def gather(
    df: PandasDataFrame,
    columns: Columns | None = None,
    beside: LazyColumns | None = None,
    into: tuple[Column, Column] = ("variable", "value"),
) -> PandasDataFrame:
    _check_type(columns, {list, None})
    _check_type(beside, {str, list, None})
    _check_type(into, tuple)
    if not (isinstance(into, tuple) and (len(into) == 2)):
        raise TypeError("must be tuple[str, str]")
    if into[0] == into[1]:
        raise TypeError("must be unique")
    if (into[0] in df.columns) or (into[1] in df.columns):
        raise TypeError("must not be an existing column key")
    if (columns != None) and (beside != None):
        raise ValueError("columns OR beside must be None")
    if (columns == None) and (beside == None):
        id_vars = []
        value_vars = list(df.columns)
    if isinstance(beside, str):
        beside = [beside]
    if isinstance(beside, list):
        id_vars = beside
        value_vars = [col for col in df.columns if col not in id_vars]
    if isinstance(columns, list):
        id_vars = [col for col in df.columns if col not in columns]
        value_vars = columns
    df = pd.melt(
        df,
        id_vars=id_vars,
        value_vars=value_vars,
        var_name=into[0],
        value_name=into[1],
    )
    df = df.dropna(subset=into[1])  # type: ignore
    df = df.reset_index(drop=True)
    return df
