import uuid

import pandas as pd


def spread(df: pd.DataFrame, column: str, using: str) -> pd.DataFrame:
    if not isinstance(column, str):
        raise TypeError("column type is invalid, must be str")
    if not isinstance(using, str):
        raise TypeError("using type is invalid, must be str")
    original_shape = df.shape[1]
    if original_shape == 2:
        temp = uuid.uuid4().hex
        df[temp] = df.groupby(column).cumcount()
    index = [col for col in df.columns if col not in [column, using]]
    df = pd.pivot_table(df, index=index, columns=[column], values=[using])  # type: ignore
    df.columns = [col for col in df.columns.get_level_values(1)]  # type: ignore
    df = df.reset_index().rename_axis(None, axis=0)
    if original_shape == 2:
        df = df.drop(temp, axis=1)
    return df
