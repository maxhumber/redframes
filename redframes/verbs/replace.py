from ..checks import _check_type
from ..types import Column, NewValue, OldValue, PandasDataFrame


def replace(
    df: PandasDataFrame, over: dict[Column, dict[OldValue, NewValue]]
) -> PandasDataFrame:
    _check_type(over, dict)
    bad_columns = list(set(over.keys()) - set(df.columns))
    if bad_columns and len(bad_columns) == 1:
        raise KeyError(f"column key: {bad_columns} is invalid")
    if bad_columns and len(bad_columns) > 1:
        raise KeyError(f"column keys: {bad_columns} are invalid")
    df = df.replace(over)
    return df
