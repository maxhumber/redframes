from ..checks import enforce
from ..core import DataFrame


def save(df: DataFrame, path: str, **kwargs) -> None:
    enforce(df, DataFrame)
    enforce(path, str)
    df._data.to_csv(path, index=False, **kwargs)
