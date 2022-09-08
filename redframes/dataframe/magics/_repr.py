import pandas as pd


def _repr(df: pd.DataFrame) -> str:
    return df.__repr__()
