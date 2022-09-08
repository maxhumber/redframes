import pandas as pd


def slice(df: pd.DataFrame, start: int, end: int) -> pd.DataFrame:
    if not isinstance(start, int):
        raise TypeError("start type is invalid, must be int")
    if not isinstance(end, int):
        raise TypeError("end type is invalid, must be int")
    df = df.iloc[start:end]
    df = df.reset_index(drop=True)
    return df
