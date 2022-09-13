import pandas as pd

from ..types import LazyColumns, PandasDataFrame
from ..checks import enforce


def select(df: PandasDataFrame, columns: LazyColumns) -> PandasDataFrame:
    enforce(columns, {list, str})
    if isinstance(columns, str):
        columns = [columns]
    bad_columns = list(set(columns) - set(df.columns))
    if bad_columns and len(bad_columns) == 1:
        raise KeyError(f"column key: {bad_columns} is invalid")
    if bad_columns and len(bad_columns) > 1:
        raise KeyError(f"column keys: {bad_columns} are invalid")
    df = df[columns]
    return df
