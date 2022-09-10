import pandas as pd

# reverse, ascending or descending?


def sort(df: pd.DataFrame, columns: list[str], reverse: bool = False) -> pd.DataFrame:
    if not isinstance(columns, list):
        raise TypeError("columns type is invalid, must be list[str]")
    if not isinstance(reverse, bool):
        raise TypeError("reverse type is invalid, must be bool")
    df = df.sort_values(by=columns, ascending=not reverse)
    df = df.reset_index(drop=True)
    return df
