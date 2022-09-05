import pandas as pd

def take(df: pd.DataFrame, rows: int = 1) -> pd.DataFrame:
    if not isinstance(rows, int):
        raise TypeError
    if rows == 0:
        raise ValueError
    if rows <= -1:
        df = df.tail(rows * -1)
    else:
        df = df.head(rows)
    df = df.reset_index(drop=True)
    return df