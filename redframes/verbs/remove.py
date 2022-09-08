import pandas as pd


def remove(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if not isinstance(columns, list):
        raise TypeError("invalid columns type, must be list[str]")
    df = df.drop(columns, axis=1)
    return df
