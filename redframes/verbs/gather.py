from __future__ import annotations

import pandas as pd

from ..checks import enforce
from ..types import Column, Columns, PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns

def gather(
    df: PandasDataFrame,
    columns: Columns | None = None,
    into: tuple[Column, Column] = ("variable", "value"),
) -> PandasDataFrame:
    enforce(columns, {list, None})
    enforce(into, tuple)
    if not (isinstance(into, tuple) and len(into) == 2):
        raise TypeError("into type is invalid, must be tuple[str, str]")
    if not columns:
        columns = list(df.columns)
    index = [col for col in df.columns if col not in columns]
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
