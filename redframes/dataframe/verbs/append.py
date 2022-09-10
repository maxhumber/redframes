import pandas as pd


def append(top: pd.DataFrame, bottom: pd.DataFrame) -> pd.DataFrame:
    df = pd.concat([top, bottom])
    df = df.reset_index(drop=True)
    return df
