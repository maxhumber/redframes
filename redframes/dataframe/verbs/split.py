import pandas as pd

import uuid


def split(
    df: pd.DataFrame, column: str, into: list[str], sep: str, drop: bool = True
) -> pd.DataFrame:
    if not isinstance(column, str):
        raise TypeError("column type is invalid, must be str")
    if not isinstance(into, list):
        raise TypeError("into type is invalid, must be list[str]")
    if not isinstance(sep, str):
        raise TypeError("sep type is invalid, must be str")
    if not isinstance(drop, bool):
        raise TypeError("drop type is invalid, must be str")
    if (column in into) and (not drop):
        raise ValueError("into columns argument is invalid, keys must be unique")
    bad_keys = set(df.columns).difference(set([column])).intersection(set(into))
    if bad_keys:
        raise ValueError("into columns argument is invalid, keys must be unique")
    columns = {uuid.uuid4().hex: col for col in into}
    temp = list(columns.keys())
    df = df.copy()
    df[temp] = df[column].str.split(sep, expand=True)
    if drop:
        df = df.drop(column, axis=1)
    df = df.rename(columns=columns)
    return df
