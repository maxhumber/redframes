import pandas as pd


def split(df: pd.DataFrame, column: str, sep: str, into: list[str]) -> pd.DataFrame:
    if not isinstance(column, str):
        raise TypeError("column type is invalid")
    if not isinstance(sep, str):
        raise TypeError("sep type is invalid")
    if not isinstance(into, list):
        raise TypeError("into type is invalid")
    df = df.copy()
    df[into] = df[column].str.split(sep, expand=True)
    return df
