from typing import Callable

import pandas as pd

# call this keep?

def filter(df: pd.DataFrame, func: Callable[..., bool]) -> pd.DataFrame:
    if not callable(func):
        raise TypeError("func type is invalid, must be Callable[..., bool]")
    df = df.loc[func]  # type: ignore
    df = df.reset_index(drop=True)
    return df
