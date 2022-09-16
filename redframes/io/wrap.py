import pandas as pd

from ..checks import _check_columns, _check_index, _check_type
from ..core import DataFrame


def wrap(pdf: pd.DataFrame) -> DataFrame:
    """Wrap a pd.DataFrame into a rf.DataFrame

    Example:

    ```python
    pdf = pd.DataFrame({"foo": range(10)})
    df = rf.wrap(pdf)
    # df is now a redframes DataFrame!
    ```
    """
    _check_type(pdf, pd.DataFrame)
    _check_index(pdf)
    _check_columns(pdf)
    df = DataFrame()
    df._data = pdf.copy()
    return df
