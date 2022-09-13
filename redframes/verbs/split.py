import uuid

from ..checks import enforce
from ..types import Column, Columns, PandasDataFrame

# ✅ No "Bad" Types
# ✅ No Side Effects
# ✅ No "Weird" Indexes
# ⚠️ checks.unique
# ❓ No Duplicate Columns


def split(
    df: PandasDataFrame, column: Column, into: Columns, sep: str, drop: bool = True
) -> PandasDataFrame:
    enforce(column, str)
    enforce(into, list)
    enforce(sep, str)
    enforce(drop, bool)
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
