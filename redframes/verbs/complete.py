import itertools

import pandas as pd


def complete(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if not isinstance(columns, list):
        raise TypeError("columns type is invalid, must be list[str]")
    series = [df[column] for column in columns]
    index = pd.MultiIndex.from_tuples(itertools.product(*series), names=columns)
    df = df.set_index(columns)
    df = df.reindex(index)
    df = df.reset_index()
    return df