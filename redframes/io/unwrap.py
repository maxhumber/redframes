from __future__ import annotations

import warnings

from ..checks import _check_type
from ..core import DataFrame
from ..types import PandasDataFrame


def unwrap(rdf: DataFrame) -> PandasDataFrame:
    """Convert a rf.DataFrame into a pd.DataFrame (opposite of `wrap`)

    Example:

    ```python
    rdf = rf.DataFrame({"foo": range(10)})
    pdf = rf.unwrap(rdf)
    ```
    """
    warnings.warn(
        "Marked for removal, please use `df.to_pandas()` instead", FutureWarning
    )
    _check_type(rdf, DataFrame)
    return rdf._data.copy()
