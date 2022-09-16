import pandas as pd

from redframes.types import PandasDataFrame

from ..checks import _check_columns, _check_file, _check_index, _check_type
from ..core import DataFrame, _wrap


def load(path: str, **kwargs) -> DataFrame:
    """Load a csv file into a rf.DataFrame (opposite of `save`)

    Example:

    ```python
    df = rf.load("example.csv")
    ```
    """
    _check_type(path, str)
    _check_file(path)
    data: PandasDataFrame = pd.read_csv(path, **kwargs)  # type: ignore
    _check_index(data)
    _check_columns(data)
    return _wrap(data)
