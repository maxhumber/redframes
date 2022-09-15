from ..checks import _check_type
from ..core import DataFrame


def save(df: DataFrame, path: str, **kwargs) -> None:
    _check_type(df, DataFrame)
    _check_type(path, str)
    df._data.to_csv(path, index=False, **kwargs)
