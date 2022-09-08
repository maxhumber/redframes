import pandas as pd

from ..dataframe import DataFrame


def unwrap(df: DataFrame) -> pd.DataFrame:
    return df._data.copy()
