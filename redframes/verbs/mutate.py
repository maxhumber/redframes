from typing import Any, Callable

import pandas as pd


def mutate(df: pd.DataFrame, mutations: dict[str, Callable[..., Any]]) -> pd.DataFrame:
    if not isinstance(mutations, dict):
        raise TypeError(
            "mutations type is invalid, must be dict[str, Callable[..., Any]"
        )
    df = df.copy()
    for column, mutation in mutations.items():
        df[column] = df.apply(mutation, axis=1)
    return df
