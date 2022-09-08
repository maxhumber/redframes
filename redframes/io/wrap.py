import pandas as pd

from ..dataframe import DataFrame
from ._validate import _validate


def wrap(pdf: pd.DataFrame) -> DataFrame:
    pdf = _validate(pdf)
    df = DataFrame()
    df._data = pdf.copy()
    return df
