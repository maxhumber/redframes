from __future__ import annotations

import datetime
from typing import Any, Callable, Literal, Union

import numpy as np  # pyright: ignore[reportMissingImports]
import pandas as pd  # pyright: ignore[reportMissingImports]
import pandas.core.groupby.generic as pg  # pyright: ignore[reportMissingImports]

Value = Any
Values = list[Value]
OldValue = Value
NewValue = Value
Column = str
Columns = list[Column]
LazyColumns = Union[Column, Columns]
OldColumn = Column
NewColumn = Column
Direction = Literal["up", "down"]
Func = Callable[..., Any]
Join = Literal["left", "right", "inner", "full"]
NumpyArray = np.ndarray
NumpyType = np.dtype
PandasDataFrame = pd.DataFrame
PandasGroupedFrame = pg.DataFrameGroupBy
PandasIndex = pd.Index
PandasRangeIndex = pd.RangeIndex
DateTime = datetime.datetime
