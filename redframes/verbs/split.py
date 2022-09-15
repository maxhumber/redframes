import uuid

from ..checks import _check_type
from ..types import Column, Columns, PandasDataFrame


def split(
    df: PandasDataFrame, column: Column, into: Columns, sep: str, drop: bool = True
) -> PandasDataFrame:
    _check_type(column, str)
    _check_type(into, list)
    _check_type(sep, str)
    _check_type(drop, bool)
    if len(into) != len(set(into)):
        raise KeyError("into keys must be unique")
    if (column in into) and (not drop):
        raise KeyError("into keys must be unique")
    bad_keys = set(df.columns).difference(set([column])).intersection(set(into))
    if bad_keys:
        raise KeyError("into keys must be unique")
    columns = {uuid.uuid4().hex: col for col in into}
    temp = list(columns.keys())
    df = df.copy()
    df[temp] = df[column].str.split(sep, expand=True)
    if drop:
        df = df.drop(column, axis=1)
    df = df.rename(columns=columns)
    return df
