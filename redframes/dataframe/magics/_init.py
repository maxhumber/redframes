from __future__ import annotations

from typing import Any

import pandas as pd


def _init(data: dict[str, list[Any]] | None = None) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()
    elif isinstance(data, dict):
        return pd.DataFrame(data)
    else:
        raise TypeError("data type is invalid")
