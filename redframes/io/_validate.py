import pandas as pd


def _validate(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.index, pd.RangeIndex):
        raise IndexError("df.index is invalid, must be pd.RangeIndex")
    if df.index.name:
        raise IndexError("df.index is invalid, must unnamed")
    if df.iloc[0].name != 0:
        raise IndexError("df.index is invalid, must start at 0")
    if not isinstance(df.columns, pd.Index):
        raise KeyError("df.columns is invalid, must be flat")
    if df.columns.has_duplicates:
        raise KeyError("df.columns is invalid, must not contain duplicate keys")
    return df
