import pandas as pd


def rename(df: pd.DataFrame, columns: dict[str, str]) -> pd.DataFrame:
    if not isinstance(columns, dict):
        raise TypeError(f"columns type is invalid, must be dict[str, str]")
    bad_columns = list(set(columns.keys()) - set(df.columns))
    if bad_columns and len(bad_columns) == 1:
        raise KeyError(f"column key: {bad_columns} is invalid")
    if bad_columns and len(bad_columns) > 1:
        raise KeyError(f"column keys: {bad_columns} are invalid")
    df = df.rename(columns=columns)
    return df
