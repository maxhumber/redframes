import pandas as pd


def _eq(lhs: pd.DataFrame, rhs: pd.DataFrame) -> bool:
    return lhs.equals(rhs)
