import pandas as pd


def sort(df: pd.DataFrame, columns: list[str], reverse: bool = False) -> pd.DataFrame:
    if not isinstance(columns, list):
        raise TypeError(f"columns type is invalid")
    df = df.sort_values(by=columns, ascending=not reverse)
    df = df.reset_index(drop=True)
    return df
