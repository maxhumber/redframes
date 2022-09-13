from __future__ import annotations

import pandas as pd

def _validate_columns_type(columns: list[str] | str | None):
    is_not_list = not isinstance(columns, list)
    is_not_str = not isinstance(columns, str)
    is_not_none = not columns == None
    if all([is_not_list, is_not_str, is_not_none]):
        raise TypeError("must be list[str] | str | None")
    
def _validate_columns_keys(requested: list[str], actual: list[str]):  
    bad_keys = set(requested).difference(actual) or []
    if bad_keys:
        if len(bad_keys) == 1:
            message = f"invalid key {bad_keys}"
        else: 
            message = f"invalid keys {bad_keys}"
        raise KeyError(message)