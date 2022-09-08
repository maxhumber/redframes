from typing import Any

import pandas as pd


def rows(df: pd.DataFrame) -> list[list[Any]]:
    return df.values.tolist()
