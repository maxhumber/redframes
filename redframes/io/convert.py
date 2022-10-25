from __future__ import annotations

from ..checks import _check_columns, _check_index, _check_type
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
    _check_type(rdf, DataFrame)
    return rdf._data.copy()


def wrap(pdf: PandasDataFrame) -> DataFrame:
    """Convert a pd.DataFrame into a rf.DataFrame (opposite of `unwrap`)

    Example:

    ```python
    pdf = pd.DataFrame({"foo": range(10)})
    rdf = rf.wrap(pdf)
    ```
    """
    _check_type(pdf, PandasDataFrame)
    _check_index(pdf)
    _check_columns(pdf)
    rdf = DataFrame()
    rdf._data = pdf.copy()
    return rdf


def convert(df: DataFrame | PandasDataFrame) -> PandasDataFrame | DataFrame:
    """Convert a rf.DataFrame into a pd.DataFrame (and/or vice versa)

    Example:

    ```python
    redf = rf.DataFrame({"foo": range(10)})
    padf = rf.convert(redf) # now a pd.DataFrame
    redf = rf.convert(padf) # now a rf.DataFrame
    ```
    """
    if isinstance(df, DataFrame):
        return unwrap(df)
    if isinstance(df, PandasDataFrame):
        return wrap(df)
    raise TypeError("must be rf.DataFrame | pd.DataFrame")
