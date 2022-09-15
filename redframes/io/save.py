from ..checks import _check_type, _check_file
from ..core import DataFrame


def save(df: DataFrame, path: str, **kwargs) -> None:
    _check_type(df, DataFrame)
    _check_type(path, str)
    _check_file(path)
    df._data.to_csv(path, index=False, **kwargs)
