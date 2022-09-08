from typing import Any

import pandas as pd


def _getitem(df: pd.DataFrame, key: str) -> list[Any]:
    return list(df[key])
