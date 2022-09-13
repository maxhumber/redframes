import pandas as pd

from ..checks import _validate, enforce
from ..core import DataFrame, _wrap


def load(path: str, **kwargs) -> DataFrame:
    enforce(path, str)
    data = pd.read_csv(path, **kwargs)
    data = _validate(data)
    return _wrap(data)
