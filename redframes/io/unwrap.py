import pandas as pd

from ..checks import _check_type
from ..core import DataFrame


def unwrap(df: DataFrame) -> pd.DataFrame:
    """Unwrap a rf.DataFrame into a pd.DataFrame (opposite of `wrap`)

    Example:

    ```python
    df = rf.DataFrame({"foo": range(10)})
    pdf = rf.unwrap(df)
    # pdf is now a pandas DataFrame!
    ```
    """
    _check_type(df, DataFrame)
    return df._data.copy()
