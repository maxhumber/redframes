import pandas as pd

from ..checks import _check_type, _check_index, _check_columns
from ..core import DataFrame


def wrap(pdf: pd.DataFrame) -> DataFrame:
    _check_type(pdf, pd.DataFrame)
    _check_index(pdf)
    _check_columns(pdf)
    df = DataFrame()
    df._data = pdf.copy()
    return df
