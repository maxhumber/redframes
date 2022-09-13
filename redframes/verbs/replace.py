from ..types import Column, PandasDataFrame, Value
from ..checks import enforce

def replace(
    df: PandasDataFrame, over: dict[Column, dict[Value, Value]]
) -> PandasDataFrame:
    enforce(over, {dict})
    bad_columns = list(set(over.keys()) - set(df.columns))
    if bad_columns and len(bad_columns) == 1:
        raise KeyError(f"column key: {bad_columns} is invalid")
    if bad_columns and len(bad_columns) > 1:
        raise KeyError(f"column keys: {bad_columns} are invalid")
    df = df.replace(over)
    return df
