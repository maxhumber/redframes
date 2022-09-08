import pandas as pd


def types(df: pd.DataFrame) -> dict[str, type]:
    df = df.reset_index(drop=True)
    df = df.astype("object")
    types = {str(col): type(df.loc[0, col]) for col in df}  # type: ignore
    return types
