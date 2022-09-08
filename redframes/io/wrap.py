import pandas as pd

from ..dataframe import DataFrame
from ._validate import _validate


def wrap(pdf: pd.DataFrame) -> DataFrame:
    if not isinstance(pdf, pd.DataFrame):
        raise TypeError("pdf type is invalid, must be pd.DataFrame")
    pdf = _validate(pdf)
    df = DataFrame()
    df._data = pdf.copy()
    return df
