import uuid

import pandas as pd

from ..checks import _check_type
from ..types import Column, PandasDataFrame


def spread(df: PandasDataFrame, column: Column, using: Column) -> PandasDataFrame:
    _check_type(column, str)
    _check_type(using, str)
    if column == using:
        raise KeyError("column and using must be unique")
    original_shape = df.shape[1]
    if original_shape == 2:
        temp = uuid.uuid4().hex
        df[temp] = df.groupby(column).cumcount()
    index = [col for col in df.columns if col not in [column, using]]
    df = pd.pivot_table(df, index=index, columns=[column], values=[using], aggfunc="first")  # type: ignore
    df.columns = [col for col in df.columns.get_level_values(1)]  # type: ignore
    df = df.reset_index().rename_axis(None, axis=0)
    if original_shape == 2:
        df = df.drop(temp, axis=1)  # type: ignore
    return df
