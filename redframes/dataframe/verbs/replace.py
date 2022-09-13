from ...types import PandasDataFrame, Column, Value


def replace(df: PandasDataFrame, rules: dict[Column, dict[Value, Value]]) -> PandasDataFrame:
    if not isinstance(rules, dict):
        raise TypeError("rules type is invalid, must be dict[str, dict[Any, Any]]")
    bad_columns = list(set(rules.keys()) - set(df.columns))
    if bad_columns and len(bad_columns) == 1:
        raise KeyError(f"column key: {bad_columns} is invalid")
    if bad_columns and len(bad_columns) > 1:
        raise KeyError(f"column keys: {bad_columns} are invalid")
    df = df.replace(rules)
    return df
