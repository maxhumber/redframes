from ..checks import _check_file, _check_type
from ..core import DataFrame


def save(df: DataFrame, path: str, **kwargs) -> None:
    """Save a rf.DataFrame to a csv file (opposite of `load`)

    Example:

    ```python
    rf.save(df, "example.csv")
    ```
    """
    _check_type(df, DataFrame)
    _check_type(path, str)
    _check_file(path)
    df._data.to_csv(path, index=False, **kwargs)
