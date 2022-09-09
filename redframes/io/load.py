import pandas as pd

from ..dataframe import DataFrame
from ..dataframe import _wrap
from ._validate import _validate


def load(path: str, **kwargs) -> DataFrame:
    if not (isinstance(path, str) and path.endswith(".csv")):
        raise TypeError("file at path is invalid, must be a csv")
    data = pd.read_csv(path, **kwargs)
    data = _validate(data)
    return _wrap(data)
