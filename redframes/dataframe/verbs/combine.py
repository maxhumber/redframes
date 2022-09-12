from __future__ import annotations

import uuid

import pandas as pd


def combine(
    df: pd.DataFrame, columns: list[str], into: str, sep: str, drop: bool = True
) -> pd.DataFrame:
    if not isinstance(columns, list):
        raise TypeError("columns type is invalid, must be list[str]")
    if not isinstance(into, str):
        raise TypeError("into type is invalid, must be str")
    if not isinstance(sep, str):
        raise TypeError("sep type is invalid, must be str")
    if not isinstance(drop, bool):
        raise TypeError("drop type is invalid, must be bool")
    if (into in df.columns) and (into not in columns):
        raise ValueError("into column argument is invalid, must be unique")
    if (into in df.columns) and (into in columns) and (not drop):
        raise ValueError("into column argument is invalid, must be unique")
    df = df.copy()
    temp = uuid.uuid4().hex
    df[temp] = df[columns].apply(lambda row: sep.join(row.values.astype(str)), axis=1)
    if drop:
        df = df.drop(columns, axis=1)
    df = df.rename(columns={temp: into})
    return df
