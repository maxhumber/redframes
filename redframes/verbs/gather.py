from __future__ import annotations

import pandas as pd

from ..checks import _check_type
from ..types import Column, Columns, PandasDataFrame


def gather(
    df: PandasDataFrame,
    columns: Columns | None = None,
    into: tuple[Column, Column] = ("variable", "value"),
) -> PandasDataFrame:
    _check_type(columns, {list, None})
    _check_type(into, tuple)
    if not (isinstance(into, tuple) and len(into) == 2):
        raise TypeError("must be tuple[str, str]")
    if into[0] == into[1]:
        raise TypeError("must be unique")
    if into[0] in df.columns:
        raise TypeError("must not be an existing column key")
    if into[1] in df.columns:
        raise TypeError("must not be an existing column key")
    if columns == None:
        columns = list(df.columns)
    index = [col for col in df.columns if col not in columns]  # type: ignore
    df = pd.melt(
        df,
        id_vars=index,
        value_vars=columns,
        var_name=into[0],
        value_name=into[1],
    )
    df = df.dropna(subset=into[1])  # type: ignore
    df = df.reset_index(drop=True)
    return df
