import pandas as pd

from ..dataframe import DataFrame


def unwrap(df: DataFrame) -> pd.DataFrame:
    if not isinstance(df, DataFrame):
        raise TypeError("df type is invalid, must be rf.DataFrame")
    return df._data.copy()
