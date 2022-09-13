import pandas as pd


def sort(df: pd.DataFrame, columns: list[str], descending: bool = False) -> pd.DataFrame:
    if not isinstance(columns, list):
        raise TypeError("columns type is invalid, must be list[str]")
    if not isinstance(descending, bool):
        raise TypeError("descending type is invalid, must be bool")
    df = df.sort_values(by=columns, ascending=not descending)
    df = df.reset_index(drop=True)
    return df
