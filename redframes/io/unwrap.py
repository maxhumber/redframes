import pandas as pd

from ..checks import enforce
from ..core import DataFrame


def unwrap(df: DataFrame) -> pd.DataFrame:
    enforce(df, DataFrame)
    return df._data.copy()
