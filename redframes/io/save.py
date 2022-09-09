import pandas as pd

from ..dataframe import DataFrame


def save(df: DataFrame, path: str, **kwargs) -> None:
    if not isinstance(df, DataFrame):
        raise TypeError("df type is invalid, must be rf.DataFrame")
    df._data.to_csv(path, **kwargs)
