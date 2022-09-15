import pandas as pd

from ..checks import _check_type
from ..core import DataFrame


def unwrap(df: DataFrame) -> pd.DataFrame:
    _check_type(df, DataFrame)
    return df._data.copy()
