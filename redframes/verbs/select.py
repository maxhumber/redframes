import pandas as pd

from ..checks import _check_type
from ..types import LazyColumns, PandasDataFrame


def select(df: PandasDataFrame, columns: LazyColumns) -> PandasDataFrame:
    _check_type(columns, {list, str})
    columns = [columns] if isinstance(columns, str) else columns
    if len(set(columns)) != len(columns):
        raise KeyError(f"column keys must be unique")
    bad_columns = list(set(columns) - set(df.columns))
    if bad_columns and len(bad_columns) == 1:
        raise KeyError(f"column key: {bad_columns} is invalid")
    if bad_columns and len(bad_columns) > 1:
        raise KeyError(f"column keys: {bad_columns} are invalid")
    df = df[columns]
    return df
