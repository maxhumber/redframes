import pandas as pd

# allow to drop just one column?

def drop(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if not isinstance(columns, list):
        raise TypeError("invalid columns type, must be list[str]")
    df = df.drop(columns, axis=1)
    return df
