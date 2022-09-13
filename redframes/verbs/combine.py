from __future__ import annotations

import uuid

from ..types import Column, Columns, PandasDataFrame


def combine(
    df: PandasDataFrame, columns: Columns, into: Column, sep: str, drop: bool = True
) -> PandasDataFrame:
    if not isinstance(columns, list):
        raise TypeError("must be Columns")
    if not isinstance(into, str):
        raise TypeError("must be str")
    if not isinstance(sep, str):
        raise TypeError("must be str")
    if not isinstance(drop, bool):
        raise TypeError("must be bool")
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
