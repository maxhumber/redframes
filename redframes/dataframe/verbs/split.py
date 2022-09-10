import pandas as pd

# sep argument at the end?


def split(df: pd.DataFrame, column: str, sep: str, into: list[str]) -> pd.DataFrame:
    if not isinstance(column, str):
        raise TypeError("column type is invalid, must be str")
    if not isinstance(sep, str):
        raise TypeError("sep type is invalid, must be str")
    if not isinstance(into, list):
        raise TypeError("into type is invalid, must be list[str]")
    df = df.copy()
    df[into] = df[column].str.split(sep, expand=True)
    df = df.drop(column, axis=1)
    return df
