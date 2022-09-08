from typing import Any

import pandas as pd


def replace(df: pd.DataFrame, rules: dict[str, dict[Any, Any]]) -> pd.DataFrame:
    if not isinstance(rules, dict):
        raise TypeError(f"invalid rules type")
    bad_columns = list(set(rules.keys()) - set(df.columns))
    if bad_columns and len(bad_columns) == 1:
        raise KeyError(f"column key: {bad_columns} is invalid")
    if bad_columns and len(bad_columns) > 1:
        raise KeyError(f"column keys: {bad_columns} are invalid")
    df = df.replace(rules)
    return df
