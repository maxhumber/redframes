import pandas as pd


def spread(df: pd.DataFrame, column: str, using: str) -> pd.DataFrame:
    if not isinstance(column, str):
        raise TypeError("column type is invalid, must be str")
    if not isinstance(using, str):
        raise TypeError("using type is invalid, must be str")
    index = [col for col in df.columns if col not in [column, using]]
    df = pd.pivot_table(df, index=index, columns=[column], values=[using])  # type: ignore
    df.columns = [col for col in df.columns.get_level_values(1)]  # type: ignore
    df = df.reset_index().rename_axis(None, axis=0)
    return df
