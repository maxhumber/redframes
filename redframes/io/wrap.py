import pandas as pd

from ..checks import _validate, enforce
from ..core import DataFrame


def wrap(pdf: pd.DataFrame) -> DataFrame:
    enforce(pdf, pd.DataFrame)
    pdf = _validate(pdf)
    df = DataFrame()
    df._data = pdf.copy()
    return df
