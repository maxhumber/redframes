from __future__ import annotations

import warnings

import pandas as pd  # pyright: ignore[reportMissingImports]

from ..checks import _check_type
from ..types import Column, Columns, LazyColumns, PandasDataFrame, PandasGroupedFrame


def _melt(
    df: PandasDataFrame,
    cols_to_keep: list[str],
    cols_to_gather: list[str],
    into: tuple[str, str],
) -> PandasDataFrame:
    df = pd.melt(
        df,
        id_vars=cols_to_keep,
        value_vars=cols_to_gather,
        var_name=into[0],
        value_name=into[1],
    )
    df = df.dropna(subset=into[1])  # type: ignore
    df = df.reset_index(drop=True)
    return df


def _grouped_melt(df: PandasGroupedFrame, into: tuple[str, str]) -> PandasDataFrame:
    cols_to_keep = df.grouper.names  # type: ignore
    cols_to_gather = [col for col in df.obj.columns if col not in cols_to_keep]  # type: ignore
    df = _melt(df.obj, cols_to_keep, cols_to_gather, into)  # type: ignore
    return df


def gather(
    df: PandasDataFrame | PandasGroupedFrame,
    columns: Columns | None = None,
    beside: LazyColumns | None = None,
    into: tuple[Column, Column] = ("variable", "value"),
) -> PandasDataFrame:
    _check_type(columns, {list, None})
    _check_type(beside, {str, list, None})
    _check_type(into, tuple)
    if (columns == None) and (beside != None) and isinstance(df, PandasDataFrame):
        warnings.warn(
            "Marked for removal, please use `df.group(...).gather(...)` instead",
            FutureWarning,
        )
    if not (isinstance(into, tuple) and (len(into) == 2)):
        raise TypeError("must be tuple[str, str]")
    if into[0] == into[1]:
        raise TypeError("must be unique")
    if isinstance(df, PandasGroupedFrame):
        if (into[0] in df.obj.columns) or (into[1] in df.obj.columns):  # type: ignore
            raise ValueError("must not be an existing column key")
        if columns != None:
            raise ValueError("columns is incompatible with group+gather")
        if beside != None:
            raise ValueError("beside is incompatible with group+gather")
        df = _grouped_melt(df, into)
        return df
    if (into[0] in df.columns) or (into[1] in df.columns):
        raise ValueError("must not be an existing column key")
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
    df = _melt(df, id_vars, value_vars, into)  # pyright: ignore[reportUnboundVariable]
    return df
