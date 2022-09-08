from __future__ import annotations

import uuid

import pandas as pd


def combine(df: pd.DataFrame, columns: list[str], sep: str, into: str) -> pd.DataFrame:
    if not isinstance(columns, list):
        raise TypeError("columns type is invalid")
    if not isinstance(sep, str):
        raise TypeError("sep type is invalid")
    if not isinstance(into, str):
        raise TypeError("into type is invalid")
    df = df.copy()
    new = uuid.uuid4().hex
    df[new] = df[columns].apply(lambda row: sep.join(row.values.astype(str)), axis=1)
    df = df.drop(columns, axis=1)
    df = df.rename(columns={new: into})
    return df
