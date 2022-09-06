from __future__ import annotations
from typing import Any
import pandas as pd

def init(data: str | dict[str, list[Any]] | pd.DataFrame = pd.DataFrame()) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data
    elif isinstance(data, dict):
        return pd.DataFrame(data)
    elif isinstance(data, str):
        if not data.endswith(".csv"):
            raise TypeError
        return pd.read_csv(data)
    else:
        raise TypeError