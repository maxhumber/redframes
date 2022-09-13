from __future__ import annotations

import uuid

from ..checks import enforce
from ..types import Column, Columns, PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def combine(
    df: PandasDataFrame, columns: Columns, into: Column, sep: str, drop: bool = True
) -> PandasDataFrame:
    enforce(columns, list)
    enforce(into, str)
    enforce(sep, str)
    enforce(drop, bool)
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
