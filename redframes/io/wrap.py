from __future__ import annotations

import warnings

from ..checks import _check_columns, _check_index, _check_type
from ..core import DataFrame
from ..types import PandasDataFrame


def wrap(pdf: PandasDataFrame) -> DataFrame:
    """Convert a pd.DataFrame into a rf.DataFrame (opposite of `unwrap`)

    Example:

    ```python
    pdf = pd.DataFrame({"foo": range(10)})
    rdf = rf.wrap(pdf)
    ```
    """
    warnings.warn(
        "Marked for removal, please use `pd.DataFrame().to_redframes()` instead",
        FutureWarning,
    )
    _check_type(pdf, PandasDataFrame)
    _check_index(pdf)
    _check_columns(pdf)
    rdf = DataFrame()
    rdf._data = pdf.copy()
    return rdf
