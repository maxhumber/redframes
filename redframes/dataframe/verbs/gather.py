from __future__ import annotations
import pandas as pd


def gather(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    into: tuple[str, str] = ("variable", "value"),
) -> pd.DataFrame:
    if not (isinstance(columns, list) or not columns):
        raise TypeError("columns type is invalid, must be list[str] | None")
    if not (isinstance(into, tuple) and len(into) == 2):
        raise TypeError("into type is invalid, must be tuple[str, str]")
    if not columns:
        columns = list(df.columns)
    index = [col for col in df.columns if col not in columns]
    df = pd.melt(
        df,
        id_vars=index,
        value_vars=columns,
        var_name=into[0],
        value_name=into[1],
    )
    df = df.dropna(subset=into[1])  # type: ignore
    return df
