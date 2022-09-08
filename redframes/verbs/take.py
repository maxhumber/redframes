import pandas as pd

def take(df, rows: int = 1) -> pd.DataFrame:
    if not isinstance(rows, int): 
        raise TypeError("rows must be int")
    if rows > df.shape[0]:
        raise ValueError("rows exceeds total size")
    if rows == 0:
        raise ValueError("rows must not be 0")
    if rows <= -1:
        df = df.tail(rows * -1)
    else:
        df = df.head(rows)
    df = df.reset_index(drop=True)
    return df