import pandas as pd


def _repr_html(df: pd.DataFrame) -> str:
    return df._repr_html_()
