from typing import Any

import pandas as pd


def shape(df: pd.DataFrame) -> dict[str, int]:
    return dict(zip(["rows", "columns"], df.shape))
