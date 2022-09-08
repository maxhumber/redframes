import pandas as pd


def slice(df: pd.DataFrame, start: int, end: int) -> pd.DataFrame:
    df = df.iloc[start:end]
    df = df.reset_index(drop=True)
    return df
